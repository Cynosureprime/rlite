#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>

#define THREAD_MAX 16  // Adjust based on your CPU cores

typedef struct {
    size_t* arr;
    char** str_arr;
    size_t low;
    size_t high;
} SortParams;

void merge(size_t arr[], char** str_arr, size_t left, size_t mid, size_t right) {
    size_t i, j, k;
    size_t n1 = mid - left + 1;
    size_t n2 = right - mid;

    // Create temp arrays
    size_t* L = (size_t*)malloc(n1 * sizeof(size_t));
    size_t* R = (size_t*)malloc(n2 * sizeof(size_t));
    char** L_str = (char**)malloc(n1 * sizeof(char*));
    char** R_str = (char**)malloc(n2 * sizeof(char*));

    // Copy data to temp arrays
    for (i = 0; i < n1; i++) {
        L[i] = arr[left + i];
        L_str[i] = str_arr[left + i];
    }
    for (j = 0; j < n2; j++) {
        R[j] = arr[mid + 1 + j];
        R_str[j] = str_arr[mid + 1 + j];
    }

    // Merge temp arrays back
    i = 0;
    j = 0;
    k = left;
    while (i < n1 && j < n2) {
        if (L[i] >= R[j]) {  // >= for descending order
            arr[k] = L[i];
            str_arr[k] = L_str[i];
            i++;
        } else {
            arr[k] = R[j];
            str_arr[k] = R_str[j];
            j++;
        }
        k++;
    }

    // Copy remaining elements
    while (i < n1) {
        arr[k] = L[i];
        str_arr[k] = L_str[i];
        i++;
        k++;
    }
    while (j < n2) {
        arr[k] = R[j];
        str_arr[k] = R_str[j];
        j++;
        k++;
    }

    // Free temporary arrays
    free(L);
    free(R);
    free(L_str);
    free(R_str);
}

void* parallel_mergesort(void* arg) {
    SortParams* params = (SortParams*)arg;
    size_t low = params->low;
    size_t high = params->high;
    size_t* arr = params->arr;
    char** str_arr = params->str_arr;

    if (low < high) {
        size_t mid = low + (high - low) / 2;

        // Create thread parameters for left and right halves
        SortParams left_params = {arr, str_arr, low, mid};
        SortParams right_params = {arr, str_arr, mid + 1, high};

        if (high - low > 1000000) {  // Parallel threshold
            pthread_t tid1, tid2;
            pthread_create(&tid1, NULL, parallel_mergesort, &left_params);
            pthread_create(&tid2, NULL, parallel_mergesort, &right_params);

            pthread_join(tid1, NULL);
            pthread_join(tid2, NULL);
        } else {
            // Sequential for smaller chunks
            parallel_mergesort(&left_params);
            parallel_mergesort(&right_params);
        }

        merge(arr, str_arr, low, mid, high);
    }
    return NULL;
}

void sort_arrays(size_t arr[], char** str_arr, unsigned long n) {
    SortParams params = {arr, str_arr, 0, n - 1};
    parallel_mergesort(&params);
}
