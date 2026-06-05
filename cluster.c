#include <stdint.h>
#include <string.h>

#define XXH_INLINE_ALL
#include "xxhash.h"


// Initial shingle size
int SHINGLE_SIZE = 3;

// Function to set shingle
void setShigle_size(int size)
{
    SHINGLE_SIZE = size;
}

// Find string length up to newline
static inline size_t get_str_len(const char *str, size_t max_len) {
    char *end = memchr(str, 0xA, max_len);
    if (end) {
        size_t len = end - str;
        if (len > 0 && str[len - 1] == 0xD) {
            return len - 1;
        }
        return len;
    }
    return max_len;
}

// Compute clustering hash using min-hash of shingles
// Similar strings share more shingles, producing closer hash values
uint64_t cluster_hash_compute(const char *str, size_t len) {
    if (len < SHINGLE_SIZE) {
        return XXH3_64bits(str, len);
    }

    uint64_t min_hash = UINT64_MAX;

    // Slide through string with shingle_size window
    for (size_t i = 0; i <= len - SHINGLE_SIZE; i++) {
        uint64_t h = XXH3_64bits(&str[i], SHINGLE_SIZE);
        if (h < min_hash) {
            min_hash = h;
        }
    }

    return min_hash;
}

// Comparison function for sort
// Groups similar strings by their minimum shingle hash
int cluster_hash_cmp(const char *a, const char *b) {
    size_t len_a = get_str_len(a, 0xFFFFFFFF);
    size_t len_b = get_str_len(b, 0xFFFFFFFF);

    uint64_t hash_a = cluster_hash_compute(a, len_a);
    uint64_t hash_b = cluster_hash_compute(b, len_b);

    if (hash_a < hash_b) return -1;
    if (hash_a > hash_b) return 1;
    return 0;
}

// Get just the cluster hash for a string
uint64_t get_cluster_hash(const char *str, size_t max_len) {
    size_t len = get_str_len(str, max_len);
    return cluster_hash_compute(str, len);
}
