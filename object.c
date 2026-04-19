// object.c — Content-addressable object store
//
// Every piece of data (file contents, directory listings, commits) is stored
// as an "object" named by its SHA-256 hash. Objects are stored under
// .pes/objects/XX/YYYYYY... where XX is the first two hex characters of the
// hash (directory sharding).
//
// Uses SHA-256 hashing with directory sharding (.pes/objects/XX/...) 
// and atomic writes (temp file + rename) similar to Git's object storage
// PROVIDED functions: compute_hash, object_path, object_exists, hash_to_hex, hex_to_hash
// TODO functions:     object_write, object_read

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
    for (int i = 0; i < HASH_SIZE; i++) {
        sprintf(hex_out + i * 2, "%02x", id->hash[i]);
    }
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

// Get the filesystem path where an object should be stored.
// Format: .pes/objects/XX/YYYYYYYY...
// The first 2 hex chars form the shard directory; the rest is the filename.
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

int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out) {
    // Step 1: build the header string e.g. "blob 16\0"
    const char *type_str = (type == OBJ_BLOB)   ? "blob"   :
                           (type == OBJ_TREE)   ? "tree"   : "commit";
    char header[64];
    int header_len = snprintf(header, sizeof(header), "%s %zu", type_str, len) + 1;
    // +1 because snprintf doesn't count the null terminator, but it is part of the format

    // Step 2: build full object = header + data in one buffer
    size_t full_len = (size_t)header_len + len;
    uint8_t *full = malloc(full_len);
    if (!full) return -1;
    memcpy(full, header, header_len);
    memcpy(full + header_len, data, len);

    // Step 3: compute SHA-256 of the full object
    ObjectID id;
    compute_hash(full, full_len, &id);

    // Step 4: deduplication - if already exists just return
    if (object_exists(&id)) {
        *id_out = id;
        free(full);
        return 0;
    }

    // Step 5: create shard directory .pes/objects/XX/
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(&id, hex);
    char shard_dir[512];
    snprintf(shard_dir, sizeof(shard_dir), "%s/%.2s", OBJECTS_DIR, hex);
    mkdir(shard_dir, 0755);

    // Step 6: write to a temp file inside the shard dir
    char tmp_path[512];
    snprintf(tmp_path, sizeof(tmp_path), "%s/.tmp_XXXXXX", shard_dir);
    int fd = mkstemp(tmp_path);
    if (fd < 0) { free(full); return -1; }

    if (write(fd, full, full_len) != (ssize_t)full_len) {
        close(fd); free(full); return -1;
    }

    // Step 7: fsync the temp file to ensure data reaches disk
    fsync(fd);
    close(fd);
    free(full);

    // Step 8: atomic rename temp file -> final object path
    char final_path[512];
    object_path(&id, final_path, sizeof(final_path));
    if (rename(tmp_path, final_path) != 0) return -1;

    // Step 9: fsync the shard directory to persist the rename
    int dir_fd = open(shard_dir, O_RDONLY);
    if (dir_fd >= 0) { fsync(dir_fd); close(dir_fd); }

    *id_out = id;
    return 0;
}

int object_read(const ObjectID *id, ObjectType *type_out, void **data_out, size_t *len_out) {
    // Step 1: get the file path from the hash
    char path[512];
    object_path(id, path, sizeof(path));

    // Step 2: open and read entire file into memory
    FILE *f = fopen(path, "rb");
    if (!f) return -1;
    fseek(f, 0, SEEK_END);
    size_t file_len = (size_t)ftell(f);
    fseek(f, 0, SEEK_SET);

    uint8_t *raw = malloc(file_len);
    if (!raw) { fclose(f); return -1; }
    if (fread(raw, 1, file_len, f) != file_len) { free(raw); fclose(f); return -1; }
    fclose(f);

    // Step 3: verify integrity - recompute hash and compare to expected
    ObjectID computed;
    compute_hash(raw, file_len, &computed);
    if (memcmp(computed.hash, id->hash, HASH_SIZE) != 0) {
        free(raw);
        return -1;
    }

    // Step 4: find the '\0' separating header from data
    uint8_t *null_byte = memchr(raw, '\0', file_len);
    if (!null_byte) { free(raw); return -1; }

    // Step 5: parse the object type from the header
    if      (strncmp((char *)raw, "blob ",   5) == 0) *type_out = OBJ_BLOB;
    else if (strncmp((char *)raw, "tree ",   5) == 0) *type_out = OBJ_TREE;
    else if (strncmp((char *)raw, "commit ", 7) == 0) *type_out = OBJ_COMMIT;
    else { free(raw); return -1; }

    // Step 6: extract the data portion (everything after the '\0')
    uint8_t *data_start = null_byte + 1;
    size_t data_len = file_len - (size_t)(data_start - raw);

    uint8_t *out = malloc(data_len + 1);
    if (!out) { free(raw); return -1; }
    memcpy(out, data_start, data_len);
    out[data_len] = '\0';

    *data_out = out;
    *len_out  = data_len;
    free(raw);
    return 0;
}
