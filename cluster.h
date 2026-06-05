#ifndef CLUSTER_HASH_H
#define CLUSTER_HASH_H

#include <stdint.h>

// Compute cluster hash using min-hash of shingles
uint64_t cluster_hash_compute(const char *str, size_t len);

// Compare two strings by cluster hash
int cluster_hash_cmp(const char *a, const char *b);

// Get cluster hash for a string
uint64_t get_cluster_hash(const char *str, size_t max_len);

// Setter for shingle
void setShigle_size(int size);
#endif
