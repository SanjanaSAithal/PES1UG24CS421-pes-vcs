// object.c — Content-addressable object store
#include "pes.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <openssl/evp.h>

// ─── PROVIDED ────────────────────────────────────────────────────────────────

void hash_to_hex(const ObjectID *id, char *hex_out) {
    for (int i = 0; i < HASH_SIZE; i++)
        sprintf(hex_out + i * 2, "%02x", id->hash[i]);
    hex_out[HASH_HEX_SIZE] = '\0';
}

int hex_to_hash(const char *hex, ObjectID *id_out) {
    if (strlen(hex) < HASH_HEX_SIZE) return -1;
    for (int i = 0; i < HASH_SIZE; i++) {
        unsigned int byte;
        if (sscanf(hex + i * 2, "%2x", &byte) != 1) return -1;
        id_out->hash[i] = (uint8_t)byte;
    }
    return 0;
}

void compute_hash(const void *data, size_t len, ObjectID *id_out) {
    unsigned int hash_len;
    EVP_MD_CTX *ctx = EVP_MD_CTX_new();
    EVP_DigestInit_ex(ctx, EVP_sha256(), NULL);
    EVP_DigestUpdate(ctx, data, len);
    EVP_DigestFinal_ex(ctx, id_out->hash, &hash_len);
    EVP_MD_CTX_free(ctx);
}

void object_path(const ObjectID *id, char *path_out, size_t path_size) {
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(id, hex);
    snprintf(path_out, path_size, "%s/%.2s/%s", OBJECTS_DIR, hex, hex + 2);
}

int object_exists(const ObjectID *id) {
    char path[512];
    object_path(id, path, sizeof(path));
    return access(path, F_OK) == 0;
}

// ─── TODO ────────────────────────────────────────────────────────────────────

int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out) {
    const char *type_str;
    if      (type == OBJ_BLOB)   type_str = "blob";
    else if (type == OBJ_TREE)   type_str = "tree";
    else if (type == OBJ_COMMIT) type_str = "commit";
    else return -1;

    // Build header: "blob 42\0"
    char header[64];
    int header_len = snprintf(header, sizeof(header), "%s %zu", type_str, len) + 1;

    // Combine header + data into full object
    size_t full_size = (size_t)header_len + len;
    unsigned char *full = malloc(full_size);
    if (!full) return -1;
    memcpy(full, header, header_len);
    memcpy(full + header_len, data, len);

    // Compute hash of full object
    compute_hash(full, full_size, id_out);

    // If already stored, skip writing (deduplication)
    if (object_exists(id_out)) {
        free(full);
        return 0;
    }

    // Build directory and file paths
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(id_out, hex);

    char dir_path[512], obj_path[512], tmp_path[512];
    snprintf(dir_path, sizeof(dir_path), "%s/%.2s", OBJECTS_DIR, hex);
    snprintf(obj_path, sizeof(obj_path), "%s/%.2s/%s", OBJECTS_DIR, hex, hex + 2);
    snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", obj_path);

    mkdir(dir_path, 0755);

    // Atomic write: write to temp file, then rename
    int fd = open(tmp_path, O_WRONLY | O_CREAT | O_TRUNC, 0444);
    if (fd < 0) { free(full); return -1; }
    ssize_t written = write(fd, full, full_size);
    fsync(fd);
    close(fd);
    free(full);

    if (written < 0 || (size_t)written != full_size) return -1;

    if (rename(tmp_path, obj_path) != 0) return -1;

    return 0;
}

int object_read(const ObjectID *id, ObjectType *type_out, void **data_out, size_t *len_out) {
    char obj_path[512];
    object_path(id, obj_path, sizeof(obj_path));

    int fd = open(obj_path, O_RDONLY);
    if (fd < 0) return -1;

    struct stat st;
    fstat(fd, &st);
    size_t full_size = (size_t)st.st_size;

    unsigned char *full = malloc(full_size);
    if (!full) { close(fd); return -1; }
    ssize_t bytes_read = read(fd, full, full_size);
    close(fd);
    if (bytes_read < 0 || (size_t)bytes_read != full_size) { free(full); return -1; }

    // Verify integrity: recompute hash and compare
    ObjectID computed;
    compute_hash(full, full_size, &computed);
    char computed_hex[HASH_HEX_SIZE + 1];
    hash_to_hex(&computed, computed_hex);
    char expected_hex[HASH_HEX_SIZE + 1];
    hash_to_hex(id, expected_hex);
    if (strcmp(computed_hex, expected_hex) != 0) {
        free(full);
        return -1;  // corruption detected
    }

    // Parse header: find the '\0' separator
    unsigned char *null_pos = memchr(full, '\0', full_size);
    if (!null_pos) { free(full); return -1; }

    // Determine type from header prefix
    if      (strncmp((char *)full, "blob",   4) == 0) *type_out = OBJ_BLOB;
    else if (strncmp((char *)full, "tree",   4) == 0) *type_out = OBJ_TREE;
    else if (strncmp((char *)full, "commit", 6) == 0) *type_out = OBJ_COMMIT;
    else { free(full); return -1; }

    // Parse size from header
    char *space = strchr((char *)full, ' ');
    *len_out = strtoul(space + 1, NULL, 10);

    // Copy out just the data portion
    size_t header_len = (size_t)(null_pos - full) + 1;
    *data_out = malloc(*len_out);
    if (!*data_out) { free(full); return -1; }
    memcpy(*data_out, full + header_len, *len_out);

    free(full);
    return 0;
}
