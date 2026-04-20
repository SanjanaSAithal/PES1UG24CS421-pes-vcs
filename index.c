// index.c — Staging area implementation
//
// Text format of .pes/index (one entry per line, sorted by path):
//
//   <mode-octal> <64-char-hex-hash> <mtime-seconds> <size> <path>
//
// Example:
//   100644 a1b2c3d4e5f6...  1699900000 42 README.md
//   100644 f7e8d9c0b1a2...  1699900100 128 src/main.c

#include "index.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>

// ─── PROVIDED ────────────────────────────────────────────────────────────────

IndexEntry* index_find(Index *index, const char *path) {
    for (int i = 0; i < index->count; i++) {
        if (strcmp(index->entries[i].path, path) == 0)
            return &index->entries[i];
    }
    return NULL;
}

int index_remove(Index *index, const char *path) {
    for (int i = 0; i < index->count; i++) {
        if (strcmp(index->entries[i].path, path) == 0) {
            int remaining = index->count - i - 1;
            if (remaining > 0)
                memmove(&index->entries[i], &index->entries[i + 1],
                        remaining * sizeof(IndexEntry));
            index->count--;
            return index_save(index);
        }
    }
    fprintf(stderr, "error: '%s' is not in the index\n", path);
    return -1;
}

int index_status(const Index *index) {
    printf("Staged changes:\n");
    int staged_count = 0;
    for (int i = 0; i < index->count; i++) {
        printf("  staged:     %s\n", index->entries[i].path);
        staged_count++;
    }
    if (staged_count == 0) printf("  (nothing to show)\n");
    printf("\n");

    printf("Unstaged changes:\n");
    int unstaged_count = 0;
    for (int i = 0; i < index->count; i++) {
        struct stat st;
        if (stat(index->entries[i].path, &st) != 0) {
            printf("  deleted:    %s\n", index->entries[i].path);
            unstaged_count++;
        } else {
            if (st.st_mtime != (time_t)index->entries[i].mtime_sec ||
                (uint32_t)st.st_size != index->entries[i].size) {
                printf("  modified:   %s\n", index->entries[i].path);
                unstaged_count++;
            }
        }
    }
    if (unstaged_count == 0) printf("  (nothing to show)\n");
    printf("\n");

    printf("Untracked files:\n");
    int untracked_count = 0;
    DIR *dir = opendir(".");
    if (dir) {
        struct dirent *ent;
        while ((ent = readdir(dir)) != NULL) {
            if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0) continue;
            if (strcmp(ent->d_name, ".pes") == 0) continue;
            if (strcmp(ent->d_name, "pes") == 0) continue;
            if (strstr(ent->d_name, ".o") != NULL) continue;

            int is_tracked = 0;
            for (int i = 0; i < index->count; i++) {
                if (strcmp(index->entries[i].path, ent->d_name) == 0) {
                    is_tracked = 1;
                    break;
                }
            }

            if (!is_tracked) {
                struct stat st;
                stat(ent->d_name, &st);
                if (S_ISREG(st.st_mode)) {
                    printf("  untracked:  %s\n", ent->d_name);
                    untracked_count++;
                }
            }
        }
        closedir(dir);
    }
    if (untracked_count == 0) printf("  (nothing to show)\n");
    printf("\n");

    return 0;
}

// ─── TODO ────────────────────────────────────────────────────────────────────

static int compare_entries(const void *a, const void *b) {
    return strcmp(((const IndexEntry *)a)->path, ((const IndexEntry *)b)->path);
}

int index_load(Index *index) {
    index->count = 0;

    FILE *f = fopen(INDEX_FILE, "r");
    if (!f) return 0;  // No index yet = empty, not an error

    char hex[HASH_HEX_SIZE + 1];
    unsigned long mode, mtime, size;
    char path[512];

    while (fscanf(f, "%lo %64s %lu %lu %511s",
                  &mode, hex, &mtime, &size, path) == 5) {
        if (index->count >= MAX_INDEX_ENTRIES) break;
        IndexEntry *e = &index->entries[index->count++];
        e->mode      = (uint32_t)mode;
        e->mtime_sec = (uint64_t)mtime;
        e->size      = (uint32_t)size;
        strncpy(e->path, path, sizeof(e->path) - 1);
        e->path[sizeof(e->path) - 1] = '\0';
        hex_to_hash(hex, &e->hash);
    }

    fclose(f);
    return 0;
}

int index_save(const Index *index) {
    // Heap-allocate to avoid stack overflow (Index struct is ~5MB)
    Index *sorted = malloc(sizeof(Index));
    if (!sorted) return -1;
    *sorted = *index;
    qsort(sorted->entries, sorted->count, sizeof(IndexEntry), compare_entries);

    char tmp_path[256];
    snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", INDEX_FILE);

    FILE *f = fopen(tmp_path, "w");
    if (!f) { free(sorted); return -1; }

    for (int i = 0; i < sorted->count; i++) {
        char hex[HASH_HEX_SIZE + 1];
        hash_to_hex(&sorted->entries[i].hash, hex);
        fprintf(f, "%lo %s %lu %u %s\n",
                (unsigned long)sorted->entries[i].mode,
                hex,
                (unsigned long)sorted->entries[i].mtime_sec,
                sorted->entries[i].size,
                sorted->entries[i].path);
    }

    fflush(f);
    fsync(fileno(f));
    fclose(f);
    free(sorted);

    if (rename(tmp_path, INDEX_FILE) != 0) return -1;
    return 0;
}

int index_add(Index *index, const char *path) {
    // Get file metadata first
    struct stat st;
    if (lstat(path, &st) != 0) {
        fprintf(stderr, "error: cannot stat '%s'\n", path);
        return -1;
    }

    // Read file contents
    FILE *f = fopen(path, "rb");
    if (!f) {
        fprintf(stderr, "error: cannot open '%s'\n", path);
        return -1;
    }

    size_t file_size = (size_t)st.st_size;
    unsigned char *data = NULL;

    if (file_size > 0) {
        data = malloc(file_size);
        if (!data) { fclose(f); return -1; }
        if (fread(data, 1, file_size, f) != file_size) {
            free(data); fclose(f); return -1;
        }
    }
    fclose(f);

    // Write blob to object store
    ObjectID hash;
    if (object_write(OBJ_BLOB, data, file_size, &hash) != 0) {
        free(data);
        return -1;
    }
    free(data);

    // Update or add index entry
    IndexEntry *existing = index_find(index, path);
    if (!existing) {
        if (index->count >= MAX_INDEX_ENTRIES) return -1;
        existing = &index->entries[index->count++];
    }

    existing->mode      = (st.st_mode & S_IXUSR) ? 0100755 : 0100644;
    existing->hash      = hash;
    existing->mtime_sec = (uint64_t)st.st_mtime;
    existing->size      = (uint32_t)st.st_size;
    strncpy(existing->path, path, sizeof(existing->path) - 1);
    existing->path[sizeof(existing->path) - 1] = '\0';

    return index_save(index);
}