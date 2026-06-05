#define CACHE_LINE_SIZE 64
#define COUNTER_STRIDE (CACHE_LINE_SIZE / sizeof(size_t))

#ifdef __MINGW32__
#define ffs __builtin_ffs
#endif

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>
#include <string.h>
#include <fcntl.h>
#include <libgen.h>
#include <inttypes.h>

#include <getopt.h>
#include <time.h>
#define _FILE_OFFSET_BITS 64
#include <sys/types.h>
#include <sys/stat.h>
#include <emmintrin.h>
#include "json_stats.h"
#include <stdatomic.h>

#ifdef _WIN
#include "advfile/dirent.h"
#else
#include <dirent.h>
#endif // _WIN

#include <stdbool.h>

#include "threading/yarn.h"
#include "threading/getinfo.c"
#include "advFile/advFile.h"
#include "advFile/fhandle.h"
#include "cluster.h"

#define XXH_INLINE_ALL
#include "xxhash.h"
#include "roaring.h"

#include "strings.h"

#define NUM_SEGMENTS 4096

#include <pthread.h>

typedef struct {
    pthread_mutex_t mutex;
    char padding[CACHE_LINE_SIZE - sizeof(pthread_mutex_t)];
} __attribute__((aligned(CACHE_LINE_SIZE))) CacheAlignedMutex;
CacheAlignedMutex pointerMapMutexes[NUM_SEGMENTS];


typedef struct {
    char *buffer;
    size_t buffer_size;
    char **pointerMap;
    size_t itemCount;
    size_t pointerMap_size;
    uintptr_t endAddress;
    int ready;
    int eof;
    size_t next_f_rem;
    pthread_mutex_t mutex;
    pthread_cond_t  cond;
} PrefetchBuffer;

typedef struct {
    file_struct *handler;
    PrefetchBuffer *pb;
} PrefetchRequest;


static PrefetchBuffer *pfbufs = NULL;
static PrefetchRequest *pf_queue = NULL;
static int pf_head = 0, pf_tail = 0, pf_stop = 0;
static size_t pf_queue_size = 0;
static pthread_mutex_t pf_queue_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  pf_queue_cond  = PTHREAD_COND_INITIALIZER;

#define version "0.42"
int mylstrcmp(const char *a, const char *b);

roaring64_bitmap_t *SearchMap[16];
roaring64_bitmap_t *FinalMap;

size_t binary_index_start[256] = {0};
size_t binary_index_end[256] = {0};
FILE *writePtr;
FILE *dupePtr;
FILE* idx;
size_t * hitCounters;
size_t lineCount = 0;
size_t lineMax = 0;
int LenMatch = 0;
char strings[BUFSIZ];
char* DelimitChar;
char pretty[64];
char pretty2[64];
size_t * occuranceCounter;
#define TAG_MASK 0x8000000000000000ULL
#define MAX_READ 0xFFFFFFFFFFFFFFFF
ErrorStats error;
int indxmode = 0;
uint64_t mask_bits = (1ULL << 40) - 1;
size_t Progress_Reads = 0;
size_t count_limit = 0;
//mode 0:build, 1:use, 2:add

// Double Buffering Globals
typedef struct {
    char *buffer;
    size_t buffer_size;
    char **pointerMap;
    size_t itemCount;
    uintptr_t endAddress;
    int eof;
} ReadBuffer;

// Interleaved buffer
ReadBuffer read_buffers[1];

// Synchronization
pthread_mutex_t read_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t  read_cond_producer = PTHREAD_COND_INITIALIZER;
pthread_cond_t  read_cond_consumer = PTHREAD_COND_INITIALIZER;

int read_buf_ready[1] = {0}; // Flags: 0=empty, 1=ready
int read_buf_idx = 0;           // Current buffer being filled/read
int read_eof_flag = 0;          // Global EOF flag

// Prefetched interleaved buffer, at most we have 2 open buffers
void *prefetch_reader_thread(void *arg) {
    file_struct *input = (file_struct *)arg;

    while (1) {
        pthread_mutex_lock(&read_mutex);
        while (read_buf_ready[0]) {
            pthread_cond_wait(&read_cond_producer, &read_mutex);
        }

        if (input->f_rem == 0) {
            read_eof_flag = 1;
            pthread_cond_signal(&read_cond_consumer);
            pthread_mutex_unlock(&read_mutex);
            break;
        }

        file_struct tmp = *input;
        tmp.buffer = NULL; tmp.buffer_size = 0;
        tmp.pointerMap = NULL; tmp.pointerMap_size = 0;

        pthread_mutex_unlock(&read_mutex);
        ReadFile02(&tmp);
        IndexFile(&tmp);

        pthread_mutex_lock(&read_mutex);
        read_buffers[0].buffer = tmp.buffer;
        read_buffers[0].buffer_size = tmp.buffer_size;
        read_buffers[0].pointerMap = tmp.pointerMap;
        read_buffers[0].itemCount = tmp.itemCount;
        read_buffers[0].endAddress = tmp.endAddress;

        input->bytesRead = tmp.bytesRead;
        input->f_rem = tmp.f_rem;
        input->extensions = tmp.extensions;

        read_buf_ready[0] = 1;
        pthread_cond_signal(&read_cond_consumer);
        pthread_mutex_unlock(&read_mutex);
    }
    return NULL;
}

void *prefetch_thread(void *arg) {
    while (1) {
        pthread_mutex_lock(&pf_queue_mutex);
        while (pf_head == pf_tail && !pf_stop)
            pthread_cond_wait(&pf_queue_cond, &pf_queue_mutex);
        if (pf_stop && pf_head == pf_tail) {
            pthread_mutex_unlock(&pf_queue_mutex);
            break;
        }
        PrefetchRequest req = pf_queue[pf_head % pf_queue_size];
        pf_head++;
        pthread_mutex_unlock(&pf_queue_mutex);

        file_struct tmp = *req.handler;
        tmp.buffer = NULL; tmp.buffer_size = 0;
        tmp.pointerMap = NULL; tmp.pointerMap_size = 0;
        ReadFile02(&tmp);
        IndexFile(&tmp);
        req.handler->bytesRead = tmp.bytesRead;
        //req.handler->f_rem     = tmp.f_rem;

        pthread_mutex_lock(&req.pb->mutex);
        req.pb->next_f_rem  = tmp.f_rem;
        req.pb->eof         = (tmp.f_rem == 0);
        req.pb->buffer      = tmp.buffer;
        req.pb->buffer_size = tmp.buffer_size;
        req.pb->pointerMap  = tmp.pointerMap;
        req.pb->itemCount   = tmp.itemCount;
        req.pb->endAddress  = tmp.endAddress;
        req.pb->ready       = 1;
        pthread_cond_signal(&req.pb->cond);
        pthread_mutex_unlock(&req.pb->mutex);
    }
    return NULL;
}

static inline void adjust_loser_tree(int *tree, int idx, int k, file_struct *handlers, size_t *read_indices) {
    int parent = (idx + k) / 2;
    while (parent > 0) {
        int opponent = tree[parent];

        if (opponent == -1) {
            tree[parent] = idx;
            return;
        }

        // Check if either handler is fully exhausted
        bool idx_exhausted = (handlers[idx].f_rem == 0 && read_indices[idx] >= handlers[idx].itemCount);
        bool opp_exhausted = (handlers[opponent].f_rem == 0 && read_indices[opponent] >= handlers[opponent].itemCount);

        int winner, loser;

        if (idx_exhausted && opp_exhausted) {
            winner = idx; loser = opponent;
        } else if (idx_exhausted) {
            winner = opponent; loser = idx;
        } else if (opp_exhausted) {
            winner = idx; loser = opponent;
        } else {
            // Both have data - safe to compare
            char *str_idx = handlers[idx].pointerMap[read_indices[idx]];
            char *str_opp = handlers[opponent].pointerMap[read_indices[opponent]];

            if (mystrcmp(str_idx, str_opp) <= 0) {
                winner = idx; loser = opponent;
            } else {
                winner = opponent; loser = idx;
            }
        }

        tree[parent] = loser;
        idx = winner;
        parent /= 2;
    }
    tree[0] = idx;
}

static inline void build_loser_tree(int *tree, int k, file_struct *handlers, size_t *read_indices) {
    // Initialize leaves with their own indices
for (int i = 0; i < k; i++) {
        tree[i] = -1;
    }
    // Build tree by adjusting each leaf
    for (int i = 0; i < k; i++) {
        adjust_loser_tree(tree, i, k, handlers, read_indices);
    }
}
roaring64_bitmap_t* merge_bitmaps_inplace(roaring64_bitmap_t** bitmaps, size_t count) {
    if (count == 0) return NULL;
    if (count == 1) return roaring64_bitmap_copy(bitmaps[0]);

    // Create copy of first bitmap
    roaring64_bitmap_t* result = roaring64_bitmap_copy(bitmaps[0]);

    // Merge others into it
    for (size_t i = 1; i < count; i++) {
        roaring64_bitmap_or_inplace(result, bitmaps[i]);
        roaring64_bitmap_free(bitmaps[i]);
    }

    return result;
}

uint64_t get_highest_hex_digit(uint64_t x) {
    // Find the position of highest bit
    int highest_bit = 63 - __builtin_clzll(x);

    // Calculate which hex digit position this corresponds to (divide by 4 rounding up)
    int hex_position = (highest_bit + 4) >> 2;

    // Shift right to isolate the highest hex digit
    return (x >> (4 * (hex_position - 1))) & 0xF;
}

int count_mask_bits(uint64_t mask) {
    int count = 0;
    while (mask) {
        count++;
        mask &= (mask - 1);  // Clear least significant set bit
    }
    return count;
}
int isTagged(void* ptr) {
    return ((uintptr_t)ptr & TAG_MASK) == TAG_MASK;
}

void* tagPointer(void* ptr) {
    return (void*)((uintptr_t)ptr | TAG_MASK);
}

void* untagPointer(void* ptr) {
    return (void*)((uintptr_t)ptr & ~TAG_MASK);
}

//Matches up-to newline
int mystrcmp(const char *a, const char *b) {
  const unsigned char *s1 = (const unsigned char *) a;
  const unsigned char *s2 = (const unsigned char *) b;
  unsigned char c1, c2;

      do
	{
	  c1 = (unsigned char) *s1++;

	  if (c1 == '\r')
      {
          c1 = (unsigned char) *s1++;
          if (c1 != '\n')
            c1 = (unsigned char) *s1--;
      }


	  c2 = (unsigned char) *s2++;

	  if (c2 == '\r')
      {
	      c2 = (unsigned char) *s2++;
          if (c2 != '\n')
            c2 = (unsigned char) *s2--;
      }

	  if (c1 == '\n')
	    return c1 - c2;
	}
      while (c1 == c2);
      return c1 - c2;
}

//Input B may contain flagged pointer
int mystrcmp2(const char *a, const char *b)
{
    unsigned char * ptr = b;

    if (isTagged(ptr))
        ptr = untagPointer(ptr);

    const unsigned char *s1 = (const unsigned char *)a;
    const unsigned char *s2 = (const unsigned char *)ptr;
    unsigned char c1, c2;
    do
    {
	  c1 = (unsigned char) *s1++;

	  if (c1 == '\r')
      {
          c1 = (unsigned char) *s1++;
          if (c1 != '\n')
            c1 = (unsigned char) *s1--;
      }


	  c2 = (unsigned char) *s2++;

	  if (c2 == '\r')
      {
	      c2 = (unsigned char) *s2++;
          if (c2 != '\n')
            c2 = (unsigned char) *s2--;
      }

	  if (c1 == '\n')
            return c1 - c2;
    } while (c1 == c2);
    return c1 - c2;
}

inline char *findeol(char *s, int64_t l) {
  unsigned int align, res, f;
  __m128i cur, seek;

  if (l <=0) return (NULL);

  seek = _mm_set1_epi8('\n');
  align = ((uint64_t) s) & 0xf;
  s = (char *) (((uint64_t) s) & 0xfffffffffffffff0L);
  cur = _mm_load_si128((__m128i const *) s);
  res = _mm_movemask_epi8(_mm_cmpeq_epi8(seek, cur)) >> align;

  f = ffs(res);
  res <<= align;
  if (f && (f <= l))
    return (s + ffs(res) - 1);
  s += 16;
  l -= (16 - align);

  while (l >= 16) {
    cur = _mm_load_si128((__m128i const *) s);
    res = _mm_movemask_epi8(_mm_cmpeq_epi8(seek, cur));
    f = ffs(res);
    if (f)
      return (s + f - 1);
    s += 16;
    l -= 16;
  }
  if (l > 0) {
    cur = _mm_load_si128((__m128i const *) s);
    res = _mm_movemask_epi8(_mm_cmpeq_epi8(seek, cur));
    f = ffs(res);
    if (f && (f <= l)) {
      return (s + f - 1);
    }
  }
  return (NULL);
}


//Indexed Binary Search
long long int BinarySearchOG(char *key, char *stringArray[], long long int lo, long long int hi)
{
    long long int mid;
    long long int readItems = hi;
    hi = hi - 1;

    while (lo <= hi)
    {
        mid = (lo + hi) / 2;

        if (mid == readItems)
        {
            return -1;
        }
        if (mystrcmp2(key, stringArray[mid]) == 0)
        {
            stringArray[mid] = tagPointer(stringArray[mid]);
            return mid;
        }

        if (mystrcmp2(key, stringArray[mid]) < 0)
            hi = mid - 1;
        else
            lo = mid + 1;
    }
    return -1;
}

int comp2(const void *a, const void *b) {
    char *a1 = (char *)a;

    char * ptr = b;

    if (isTagged(ptr))
        ptr = untagPointer(ptr);

    return(mylstrcmp(a1,ptr));
}

int comp_cluster(const void *s1, const void *s2)
{
    return cluster_hash_cmp(*(const char **)s1, *(const char **)s2);
}

long long int BinarySearchLen(char *key, char *stringArray[], long long int lo, long long int hi)
{
    long long int mid;
    long long int readItems = hi;
    hi = hi - 1;

    while (lo <= hi)
    {
        mid = (lo + hi) / 2;

        if (mid == readItems)
        {
            return -1;
        }
        if (comp2(key, stringArray[mid]) == 0)
        {
            stringArray[mid] = tagPointer(stringArray[mid]);
            return mid;
        }

        if (comp2(key, stringArray[mid]) < 0)
            hi = mid - 1;
        else
            lo = mid + 1;
    }

    return -1;
}


//Matches up-to specified length
int mylstrcmp(const char *a, const char *b) {
  const unsigned char *s1 = (const unsigned char *) a;
  const unsigned char *s2 = (const unsigned char *) b;
  unsigned char c1, c2;
  int len, ilen = LenMatch;
  if (ilen == 0) {
      do
	{
	  c1 = (unsigned char) *s1++;
	  if (c1 == '\r')
	      c1 = (unsigned char) *s1++;
	  c2 = (unsigned char) *s2++;
	  if (c2 == '\r')
	      c2 = (unsigned char) *s2++;
	  if (c1 == '\n')
	    return c1 - c2;
	}
      while (c1 == c2);
      return c1 - c2;
  } else {
      len = 0;
      do
	{
	  c1 = (unsigned char) *s1++;
	  if (c1 == '\r')
	      c1 = (unsigned char) *s1++;
	  c2 = (unsigned char) *s2++;
	  if (c2 == '\r')
	      c2 = (unsigned char) *s2++;
	  if (c1 == '\n')
	    return c1 - c2;
	}
      while (c1 == c2 && ++len < ilen);
      return c1 - c2;
  }
}

//Unused for now, matched up to character
int myldstrcmp(const char *a, const char *b) {
  const unsigned char *s1 = (const unsigned char *) a;
  const unsigned char *s2 = (const unsigned char *) b;
  unsigned char c1, c2;
  int len, ilen = LenMatch;
  if (ilen == 0) {
      do
	{
	  c1 = (unsigned char) *s1++;
	  if (c1 == '\r')
	      c1 = (unsigned char) *s1++;
	  c2 = (unsigned char) *s2++;
	  if (c2 == '\r')
	      c2 = (unsigned char) *s2++;
	  if (c1 == '\n')
	    return c1 - c2;
		if (c1 == DelimitChar[0])
			return c1 - c2;
		if (c2 == DelimitChar[0])
			return c2 - c1;
	}
      while (c1 == c2);
      return c1 - c2;
  } else {
      len = 0;
      do
	{
	  c1 = (unsigned char) *s1++;
	  if (c1 == '\r')
	      c1 = (unsigned char) *s1++;
	  c2 = (unsigned char) *s2++;
	  if (c2 == '\r')
	      c2 = (unsigned char) *s2++;
	  if (c1 == '\n')
	    return c1 - c2;
		if (c1 == DelimitChar[0])
			return c1 - c2;
		if (c2 == DelimitChar[0])
			return c2 - c1;
	}
      while (c1 == c2 && ++len < ilen);
      return c1 - c2;
  }
}



float time_elapsed(struct timespec * clock,bool restart_clock)
{
    struct timespec now;
    clock_gettime(CLOCK_REALTIME, &now);

    float elapsed_time = (now.tv_sec-clock->tv_sec ) +
                              ( now.tv_nsec-clock->tv_nsec) / 1e9;
    if (restart_clock)
        clock_gettime(CLOCK_REALTIME, clock);

    return elapsed_time;
}


char* size_t_to_pretty(size_t num, char* buffer, size_t buffer_size) {
    char temp[64];
    int len, i, j;

    // Convert number to string
    snprintf(temp, sizeof(temp), "%zu", num);
    len = strlen(temp);

    // Buffer overflow check
    size_t required = len + (len - 1) / 3;
    if (required + 1 > buffer_size) {
        return NULL;
    }

    // Start from the end of the string
    i = len - 1;
    j = buffer_size - 1;
    buffer[j] = '\0';
    j--;

    int count = 0;
    while (i >= 0 && j >= 0) {
        buffer[j] = temp[i];
        count++;
        if (count == 3 && i > 0) {
            j--;
            if (j >= 0) {
                buffer[j] = ',';
            }
            count = 0;
        }
        i--;
        j--;
    }

    // Shift the string to the beginning of the buffer
    if (j < 0) {
        return NULL; // Buffer overflow
    }
    memmove(buffer, buffer + j + 1, buffer_size - j - 1);

    return buffer;
}



//Used for locking pointermap during inner tagging, will re-aquire new lock when needed
long long int mutex_relock(long long int currIdx, long long int nextIdx)
{
    pthread_mutex_unlock(&pointerMapMutexes[currIdx].mutex);
    pthread_mutex_lock(&pointerMapMutexes[nextIdx].mutex);

    return nextIdx;
}

typedef struct worknode
{

    bool exit_status;
    int nodeid;
    struct worknode *next;
    char **pointerMap;
    char *buffer;
    size_t buffer_size;

    bool mFlag;
    bool cFlag;

    size_t *charLen;
    size_t bytesRead;
    int mode;
    uint32_t mask;
    file_struct node_filestruct;
    file_struct node_refstruct;
    size_t start;
    size_t end;
    size_t *counter;
    long file_id;
    long id;
    double time;
    bool qFlag;
    bool keep;
} __attribute__((aligned(64))) JOB; // 64-byte alignment to improve cache access

typedef struct indexnode
{
    int mode;
    bool exit_status;
    int nodeid;
    struct indexnode *next;

    size_t buffer_size;

    char ***pointerMap;

    char **DeDupeMap;
    bool write_dupe;
    size_t start;
    size_t end;

    size_t chunk_count;
    file_struct *node_filestruct;
    bool mFlag;
    bool qFlag;
    size_t * countedArr;

    size_t id;

} __attribute__((aligned(64)))JOBIndex;




JOBIndex *indexjobs;
JOB *jobs = NULL;

size_t read_id = 0;

JOBIndex *IndexWorkHead = NULL, **IndexWorkTail = &IndexWorkHead;
JOBIndex *IndexFreeHead = NULL, **IndexFreeTail = &IndexFreeHead;
JOBIndex *IndexProcessedHead = NULL, **IndexProcessedTail = &IndexProcessedHead;

JOB *WorkHead = NULL, **WorkTail = &WorkHead;
JOB *FreeHead = NULL, **FreeTail = &FreeHead;
JOB *ProcessedHead = NULL, **ProcessedTail = &ProcessedHead;

static lock *WorkWaiting, *ThreadLock, *FreeWaiting, *WriteOrder, *IndexWorkWaiting, *IndexFreeWaiting, *DupeWrite_Lock;
static lock * roaring64_lock[4097];

size_t i = 0;
void buildFree(int WUs)
{
    int i = 0;
    JOB *job;

    FreeHead = NULL;
    FreeTail = &FreeHead;
    if (jobs == NULL)
    {
        return;
    }
    for (i = 0; i < WUs; i++)
    {
        job = &jobs[i];
        if (FreeTail)
        {
            *FreeTail = job;
            FreeTail = &(job->next);
        }
        else
        {
            FreeHead = job;
            FreeTail = &(job->next);
        }
        job->nodeid = 0;
        job->next = NULL;
        job->exit_status = false;
    }
}

void buildIndexFree(int WUs)
{
    int i = 0;
    JOBIndex *job;
    IndexFreeHead = NULL;
    IndexFreeTail = &IndexFreeHead;
    if (indexjobs == NULL)
    {
        return;
    }
    for (i = 0; i < WUs; i++)
    {

        // printf("Building free %zu\n",i);
        job = &indexjobs[i];
        if (IndexFreeTail)
        {
            *IndexFreeTail = job;
            IndexFreeTail = &(job->next);
        }
        else
        {
            IndexFreeHead = job;
            IndexFreeTail = &(job->next);
        }
        job->nodeid = 0;
        job->next = NULL;
        job->exit_status = false;
    }
}

void addWork(JOB *job, bool status)
{
    possess(WorkWaiting);
    job->exit_status = status;
    job->next = NULL;
    *WorkTail = job;
    WorkTail = &(job->next);
    twist(WorkWaiting, BY, +1);
}

void addWorkIndex(JOBIndex *job, bool status)
{
    possess(IndexWorkWaiting);
    job->exit_status = status;
    job->next = NULL;
    *IndexWorkTail = job;
    IndexWorkTail = &(job->next);
    twist(IndexWorkWaiting, BY, +1);
}

char *fix_path(char *pathtodir)
{
    size_t i = 0;
    for (i = 0; i < strlen(pathtodir) - 1; i++)
    {
        if (pathtodir[i] == 47)
        {
            pathtodir[i] = 92;
        }
    }

    return pathtodir;
}

void ScanFile(char *filetoindex, char *pathtodir, char *filepath)
{
#ifdef _POSIX
    sprintf(filepath, "%s%s", fix_path(pathtodir), filetoindex);
#else
    sprintf(filepath, "%s\\%s", fix_path(pathtodir), filetoindex);
#endif
}

// Scans file and provides threads with work
void listfile(char *name)
{
    file_struct dfile;
    dfile.f_name = name;
    JOB *job;

    if (OpenFile02(&dfile) != 1)
    {
        fprintf(stderr, "File %s doesn't exist or is invalid\n", name);
        return;
    }

    // Package each block into a job struct for thread
    while (dfile.f_rem != 0)
    {
        ReadFile02(&dfile);
        possess(FreeWaiting);
        // Keep adding work until the number of free WUs is 0
        wait_for(FreeWaiting, NOT_TO_BE, 0);
        job = FreeHead;
        FreeHead = job->next;
        twist(FreeWaiting, BY, -1);
        job->next = NULL;
        // Pass the structure to the job node and let that deal with indexing
        job->node_filestruct = dfile;
        // Reset this otherwise read function will free the buffer, buffer is instead free within thread call
        dfile.buffer_size = 0;
        addWork(job, false);
    }
}

// Scans directory and provides threads with work
void listdir(char *name, int level, int maxdir)
{
    char procFile[BUFSIZ];
    JOB *job;

    if (maxdir != 0)
    {
        if (level > maxdir)
            return;
    }

    DIR *dir;
    struct dirent *entry;

    if (!(dir = opendir(name)))
        return;
    if (!(entry = readdir(dir)))
        return;

    do
    {
        if (entry->d_type == DT_DIR)
        {
            char path[1024];
            int len = snprintf(path, sizeof(path) - 1, "%s/%s", name, entry->d_name);
            path[len] = 0;
            if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
                continue;
            listdir(path, level + 1, maxdir);
        }
        else
        {
            ScanFile(entry->d_name, name, procFile);

            file_struct dfile;
            dfile.f_name = procFile;

            if (OpenFile02(&dfile) != 1)
            {
                fprintf(stderr, "File %s doesn't exist or is invalid\n", procFile);
            }

            // Package each block into a job struct for thread
            while (dfile.f_rem != 0)
            {
                ReadFile02(&dfile);
                possess(FreeWaiting);
                // Keep adding work until the number of free WUs is 0
                wait_for(FreeWaiting, NOT_TO_BE, 0);
                job = FreeHead;
                FreeHead = job->next;
                twist(FreeWaiting, BY, -1);
                job->next = NULL;

                // Pass the structure to the job node and let that deal with indexing
                job->node_filestruct = dfile;
                // Reset this otherwise read function will free the buffer, buffer is instead free within thread call
                dfile.buffer_size = 0;
                addWork(job, false);
            }
        }
    } while (entry = readdir(dir));
    closedir(dir);
}

void thread_index(void * dummy)
{
    (void)dummy;
    JOBIndex *job;

    while (1)
    {
        possess(IndexWorkWaiting);
        wait_for(IndexWorkWaiting, TO_BE_MORE_THAN, 0);
        if (IndexWorkHead == NULL)
        {
            release(IndexWorkWaiting);
            exit(1);
        }
        job = IndexWorkHead;
        IndexWorkHead = job->next;

        if (IndexWorkHead == NULL)
            IndexWorkTail = &IndexWorkHead;

        if (job->exit_status == true)
        {
            possess(IndexFreeWaiting);
            if (IndexFreeTail)
            {
                *IndexFreeTail = job;
                IndexFreeTail = &(job->next);
            }
            else
            {
                IndexFreeHead = job;
                IndexFreeTail = &(job->next);
            }
            twist(IndexFreeWaiting, BY, +1);
            twist(IndexWorkWaiting, BY, -1);
            possess(ThreadLock);
            twist(ThreadLock, BY, -1);

            return;
        }
        twist(IndexWorkWaiting, BY, -1);

        switch (job->mode)
        {

        case 0:; // Index

            size_t chunk_count = 0;

            chunk_count = IndexChunk(job->node_filestruct, job->pointerMap, job->start, job->end, job->id);
            size_t iLoop = 0;
            uint64_t Digest;

            size_t buf_max = 0;

            if (job->mFlag)
            {
                for (iLoop = 0; iLoop < chunk_count; iLoop++)
                {
                    //Need work here
                    buf_max = &job->node_filestruct->buffer[job->end]-job->pointerMap[job->id][iLoop];
                    buf_max++;

                    if (mystrlen2(job->pointerMap[job->id][iLoop],buf_max) > 0)
                    {
                        Digest = XXH3_64bits(job->pointerMap[job->id][iLoop], mystrlen2(job->pointerMap[job->id][iLoop],buf_max));

                        Digest = mask_bits & Digest;
                        possess(roaring64_lock[get_highest_hex_digit(Digest)]);
                            roaring64_bitmap_add(SearchMap[get_highest_hex_digit(Digest)], Digest);
                        release(roaring64_lock[get_highest_hex_digit(Digest)]);

                    }
                }

            }

            job->chunk_count = chunk_count;

            //fprintf(stderr,"counted %zu\n",chunk_count);

            break;
        case 1:; // De-dupe
            size_t actualIndex = job->start;
            WBuffer Write_Buffer;

            if (job->write_dupe)
            {
                initWriteBuffer(&Write_Buffer);

            }

            //Analysis
            if (job->qFlag)
            {

                for (size_t line = (job->start + 1); line < job->end; line++)
                {
                    job->countedArr[actualIndex]++;

                    if (mystrcmp(job->DeDupeMap[line], job->DeDupeMap[actualIndex]) == 0)
                    {

                        //job->countedArr[actualIndex]++;

                        if (job->write_dupe ==1)
                        {
                            buffer_string2(&Write_Buffer,job->DeDupeMap[line],MAX_READ);
                            flushBuffer(&Write_Buffer,dupePtr,0);
                        }

                        job->DeDupeMap[line][0] = 10;
                    }
                    else
                    {

                        //job->countedArr[actualIndex]++;

                        actualIndex++;

                        // Shuffle to that position;
                        job->DeDupeMap[actualIndex] = job->DeDupeMap[line];
                    }
                }

                if (actualIndex > 1)
                {
                    job->countedArr[actualIndex]++;
                }

            }
            else //no Analysis
            {
                for (size_t line = (job->start + 1); line < job->end; line++)
                {
                    if (mystrcmp(job->DeDupeMap[line], job->DeDupeMap[actualIndex]) == 0)
                    {
                        if (job->write_dupe ==1)
                        {

                            buffer_string2(&Write_Buffer,job->DeDupeMap[line],MAX_READ);
                            flushBuffer(&Write_Buffer,dupePtr,0);
                        }

                        job->DeDupeMap[line][0] = 10;
                    }
                    else
                    {
                        actualIndex++;
                        // Shuffle to that position;
                        job->DeDupeMap[actualIndex] = job->DeDupeMap[line];
                    }
                }
            }


            //Clean up dupe-buffer
            if (job->write_dupe ==1)
            {
                flushBuffer(&Write_Buffer,dupePtr,1);
                free(Write_Buffer.buffer);
            }

            //Check if de-dupe write and clear buffer
            actualIndex++;
            job->end = actualIndex;

            break;
        }
        possess(IndexFreeWaiting);
        if (IndexFreeTail)
        {
            *IndexFreeTail = job;
            IndexFreeTail = &(job->next);
        }
        else
        {
            IndexFreeHead = job;
            IndexFreeTail = &(job->next);
        }
        twist(IndexFreeWaiting, BY, +1);
    }
}

void thread_search(void * dummy)
{
    (void)dummy;
    JOB *job;
    long file_id = 0;
    size_t searchHits = 0;
    struct timespec thread_start;
    while (1)
    {
        possess(WorkWaiting);
        wait_for(WorkWaiting, TO_BE_MORE_THAN, 0);


        if (WorkHead == NULL)
        {
            fprintf(stderr, "Job null in procjob\n");
            release(WorkWaiting);
            exit(1);
        }

        job = WorkHead;
        /*
        if (job->op == JOB_DONE) {
                release(WorkWaiting);
                return;
        }
        */

        WorkHead = job->next;
        if (WorkHead == NULL)
            WorkTail = &WorkHead;

        if (job->exit_status == true)
        {
            possess(FreeWaiting);
            if (FreeTail)
            {
                *FreeTail = job;
                FreeTail = &(job->next);
            }
            else
            {
                FreeHead = job;
                FreeTail = &(job->next);
            }
            twist(FreeWaiting, BY, +1);
            twist(WorkWaiting, BY, -1);
            possess(ThreadLock);
            twist(ThreadLock, BY, -1);

            if (searchHits != 0)
            {
                __sync_fetch_and_add(&hitCounters[file_id * COUNTER_STRIDE], searchHits);
            }

            return;
        }


        twist(WorkWaiting, BY, -1);

        switch (job->mode)
        {

        case 0: // Search mode
            clock_gettime(CLOCK_REALTIME, &thread_start);
            IndexFile(&job->node_refstruct);

            size_t iLoop = 0;
            uint64_t Digest;

            if (file_id != job->file_id)
            {
                if (searchHits != 0)
                {
                    __sync_fetch_and_add(&hitCounters[file_id * COUNTER_STRIDE], searchHits);
                }
                searchHits = 0;
                file_id = job->file_id;
            }

            size_t start = 0;
            size_t end = 0;
            size_t buffer_left = 0;
            for (iLoop = 0; iLoop < job->node_refstruct.itemCount; iLoop++)
            {

                if (job->mFlag)
                {
                    buffer_left = &job->node_refstruct.buffer[job->node_refstruct.buffer_size-1]-job->node_refstruct.pointerMap[iLoop];
                    Digest = XXH3_64bits(job->node_refstruct.pointerMap[iLoop], mystrlen2(job->node_refstruct.pointerMap[iLoop],buffer_left));

                    Digest = mask_bits & Digest;
                    if (roaring64_bitmap_contains(FinalMap, Digest))
                    {
                        start = binary_index_start[(unsigned char)job->node_refstruct.pointerMap[iLoop][0]];
                        end = binary_index_end[(unsigned char)job->node_refstruct.pointerMap[iLoop][0]];
                        if(BinarySearchOG(job->node_refstruct.pointerMap[iLoop], job->node_filestruct.pointerMap, start, end)!=-1)
                            searchHits++;
                    }
                }
                else
                {
                    //Use a indexed binary search to reduce number of compares required
                    start = binary_index_start[(unsigned char)job->node_refstruct.pointerMap[iLoop][0]];
                    end = binary_index_end[(unsigned char)job->node_refstruct.pointerMap[iLoop][0]];
                    if (LenMatch == 0)
                    {
                        if(BinarySearchOG(job->node_refstruct.pointerMap[iLoop], job->node_filestruct.pointerMap, start, end)!=-1)
                            searchHits++;
                        //if(BinarySearchOG(job->node_refstruct.pointerMap[iLoop], job->node_filestruct.pointerMap, 0, job->node_filestruct.itemCount)!=-1)
                            //searchHits++;
                    }
                    else
                    {
                        long long int index = BinarySearchLen(job->node_refstruct.pointerMap[iLoop], job->node_filestruct.pointerMap, start, end);

                        if(index !=-1)
                        {
                            searchHits++;
                            char * ptr = NULL;

                            long long int segmentIndex = (index-1) / 4096;
                            if (segmentIndex < 0)
                                segmentIndex = 0;

                            pthread_mutex_lock(&pointerMapMutexes[segmentIndex].mutex);

                            for (long long int inner = index-1; inner > start; inner--)
                            {
                                if ((inner/4096) != segmentIndex)
                                    segmentIndex = mutex_relock(segmentIndex,inner/4096);

                                if (isTagged(job->node_filestruct.pointerMap[inner]))
                                    ptr = untagPointer(job->node_filestruct.pointerMap[inner]);
                                else
                                    ptr = job->node_filestruct.pointerMap[inner];

                                if (mylstrcmp(job->node_refstruct.pointerMap[iLoop],ptr) == 0)
                                {
                                    job->node_filestruct.pointerMap[inner] = tagPointer(job->node_filestruct.pointerMap[inner]);
                                    searchHits++;
                                }
                                else
                                {
                                    break;
                                }
                            }

                            for (size_t inner = index+1; inner < end; inner++)
                            {
                                if (isTagged(job->node_filestruct.pointerMap[inner]))
                                    ptr = untagPointer(job->node_filestruct.pointerMap[inner]);
                                else
                                    ptr = job->node_filestruct.pointerMap[inner];

                                if (mylstrcmp(job->node_refstruct.pointerMap[iLoop],ptr) == 0)
                                {
                                    job->node_filestruct.pointerMap[inner] =tagPointer(job->node_filestruct.pointerMap[inner]);
                                    searchHits++;
                                }
                                else
                                {
                                    break;
                                }
                            }
                            pthread_mutex_unlock(&pointerMapMutexes[segmentIndex].mutex);
                        }

                    }
                }
            }

            // Clear the buffers that were passed to us
            free(job->node_refstruct.pointerMap);
            free(job->node_refstruct.buffer);

            double timed = time_elapsed(&thread_start,1);
            job->time = timed;
            break;

        case 1:; // Write mode
            WBuffer Write_Buffer;
            initWriteBuffer(&Write_Buffer);

            char * ptr;

            size_t localWriteCount = 0;
            size_t BuffRem = 0;

            if (!job->qFlag)
            {
                for (iLoop = job->start; iLoop < job->end; iLoop++)
                {
                    if (isTagged(job->node_filestruct.pointerMap[iLoop]) == job->cFlag)
                    {
                        ptr = untagPointer(job->node_filestruct.pointerMap[iLoop]);
                        BuffRem = job->node_filestruct.endAddress - (uintptr_t)ptr;
                        buffer_string2(&Write_Buffer, ptr,BuffRem+1);
                        localWriteCount++;
                    }
                }
            }
            else
            {
                for (iLoop = job->start; iLoop < job->end; iLoop++)
                {
                    if (isTagged(job->node_filestruct.pointerMap[iLoop]) == job->cFlag)
                    {
                        BuffRem = job->node_filestruct.endAddress - (uintptr_t)job->node_filestruct.pointerMap[iLoop];
                        ptr = untagPointer(job->node_filestruct.pointerMap[iLoop]);
                        buffer_string3(occuranceCounter,iLoop,&Write_Buffer, ptr,BuffRem,count_limit);
                        localWriteCount++;
                    }
                }
            }

            possess(WriteOrder);
            wait_for(WriteOrder, TO_BE, job->id);
            fwrite(Write_Buffer.buffer, 1, Write_Buffer.bufferUsed, writePtr);
            free(Write_Buffer.buffer);

            if (peek_lock(WriteOrder) > 2000)
                twist(WriteOrder, TO, 0);
            else
                twist(WriteOrder, BY, +1);

            __sync_fetch_and_add(job->counter, localWriteCount);

            // Do not free free OG buffer since writes are still pending, only this part is done

            break;



        case 2: //Counting mode
            clock_gettime(CLOCK_REALTIME, &thread_start);

            CountChunkV2(&job->node_filestruct);

            //Reusing this lock
            possess(DupeWrite_Lock);
                lineCount += job->node_filestruct.itemCount;
                if (job->node_filestruct.itemMax > lineMax)
                    lineMax = job->node_filestruct.itemMax;
            release(DupeWrite_Lock);

            free(job->node_filestruct.buffer);
            free(job->node_filestruct.pointerMap);

            break;

        case 3: //Index mode (build by chunks)
            IndexFile(&job->node_filestruct);

            //size_t buf_max = &(job->node_filestruct.buffer) + job->node_filestruct.buffer_size;
            size_t buf_max =  job->node_filestruct.buffer_size;
            buf_max++;

            for (size_t iLoop = 0; iLoop < job->node_filestruct.itemCount; iLoop++)
            {

                if (mystrlen2(job->node_filestruct.pointerMap[iLoop],buf_max) > 0)
                {
                    Digest = XXH3_64bits(job->node_filestruct.pointerMap[iLoop], mystrlen2(job->node_filestruct.pointerMap[iLoop],buf_max));
                    Digest = mask_bits & Digest;
                    possess(roaring64_lock[get_highest_hex_digit(Digest)]);
                        roaring64_bitmap_add(SearchMap[get_highest_hex_digit(Digest)], Digest);
                    release(roaring64_lock[get_highest_hex_digit(Digest)]);
                }

            }
            __sync_fetch_and_add(&Progress_Reads,job->node_filestruct.itemCount);
            free(job->node_filestruct.buffer);
            free(job->node_filestruct.pointerMap);
            break;

        case 4: //Index mode (search by chunks)
            IndexFile(&job->node_filestruct);
            localWriteCount = 0;
            initWriteBuffer(&Write_Buffer);

            //buf_max = &(job->node_filestruct.buffer) + job->node_filestruct.buffer_size;
            buf_max = job->node_filestruct.buffer_size;
            BuffRem = 0;
            buf_max++;
            for (size_t iLoop = 0; iLoop < job->node_filestruct.itemCount; iLoop++)
            {
                if (mystrlen2(job->node_filestruct.pointerMap[iLoop],buf_max) > 0)
                {
                    Digest = XXH3_64bits(job->node_filestruct.pointerMap[iLoop], mystrlen2(job->node_filestruct.pointerMap[iLoop],buf_max));
                    Digest = mask_bits & Digest;
                    if (roaring64_bitmap_contains(FinalMap, Digest) != job->keep)
                    {
                        BuffRem = job->node_filestruct.endAddress - (uintptr_t)job->node_filestruct.pointerMap[iLoop];
                        buffer_string2(&Write_Buffer,job->node_filestruct.pointerMap[iLoop],BuffRem);
                        localWriteCount++;
                    }
                }

            }
            possess(WriteOrder);
                fwrite(Write_Buffer.buffer, 1, Write_Buffer.bufferUsed, writePtr);
            release(WriteOrder);
            free(Write_Buffer.buffer);
            __sync_fetch_and_add(&lineCount,job->node_filestruct.itemCount);
            __sync_fetch_and_add(&Progress_Reads,localWriteCount);
            free(job->node_filestruct.buffer);
            free(job->node_filestruct.pointerMap);
            break;
        case 5: // Similar mode, scan and write
            IndexFile(&job->node_filestruct);
            localWriteCount = 0;

            initWriteBuffer(&Write_Buffer);

            for (iLoop = 0; iLoop < job->node_filestruct.itemCount; iLoop++)
            {
                if (roaring64_bitmap_contains(FinalMap,get_cluster_hash(job->node_filestruct.pointerMap[iLoop],0xFFFFFFF)))
                {
                    buffer_string2(&Write_Buffer,job->node_filestruct.pointerMap[iLoop],MAX_READ);
                    flushBuffer(&Write_Buffer,writePtr,0);
                    localWriteCount++;
                }
            }

            flushBuffer(&Write_Buffer,writePtr,1);
            __sync_fetch_and_add(&lineCount,job->node_filestruct.itemCount);
            __sync_fetch_and_add(&Progress_Reads,localWriteCount);
            free(job->node_filestruct.pointerMap);
            free(job->node_filestruct.buffer);
            free(Write_Buffer.buffer);

        break;
        }

        possess(FreeWaiting);
        job->next = NULL;
        if (FreeTail)
        {
            *FreeTail = job;
            FreeTail = &(job->next);
        }
        else
        {
            FreeHead = job;
            FreeTail = &(job->next);
        }
        twist(FreeWaiting, BY, +1);
    }
}

void sizeUsage(size_t in) {
    int counter = 0;
    double size = in;
    while(size > 1024)
    {
        size = size/1024;
        counter++;
    }
    switch(counter){
    case 0:
        sprintf(strings,"%.2fBs",size);
        break;
    case 1:
        sprintf(strings,"%.2fKBs",size);
        break;
    case 2:
        sprintf(strings,"%.2fMBs",size);
        break;
    case 3:
        sprintf(strings,"%.2fGBs",size);
        break;
    case 4:
        sprintf(strings,"%.2fTBs",size);
        break;
    }

}

int compareStrings(char **s1, char **s2)
{
    return mystrcmp(*s1, *s2);
}


int main(int argc, char *argv[])
{

help:
if (argc == 1) {
    fprintf(stderr,
        "%s %s by CynosurePrime (CsP)\n"
        "A simplified version of the rling tool\n\n"
        "Usage: %s inputfile [ref1] [ref2] [ref3] [options]\n"
        "Options:\n"
        "\t-t [threads]  Number of threads where MT is used\n"
        "\t-z [num]      Sort by similar mode, specify shingle number\n"
        "\t\t\t High -z results is less collisions, more similar clustering\n"
        "\t\t\t Low -z results is more collisions, less similar clustering\n"
        "\t-Z [dir]      Output similar mode by clusterfile to dir by appending, default to stdout\n"
        "\t-m            Enable lookup map, useful for high number of searches\n"
        "\t-o [file]     Output to a file, defaults to stdout\n"
        "\t-n            Do not remove duplicates\n"
        "\t-c            Write items in common between lists\n"
        "\t-l [len]      Limit matching up to a specified length\n"
        "\t-e [char]     Limit matching up to a specified character (not implemented)\n"
        "\t-p            Input list is pre-sorted, do not perform sort\n"
        "\t-D [file]     Write duplicates to file\n"
        "\t-S mem|parts  Use positive values for RAM limit use negative for parts\n"
        "\t\t\t -S 400M/20G/1T RAM limits\n"
        "\t\t\t -S -2/-10/-20 2,10,20 parts\n"
        "\t-T [dir]      Temp dir to swap part files in\n"
        "\t-r [dir]      Read and recurse a directory (not implemented)\n"
        "\t-L, --count   Line Count with longest count\n"
        "\t-q            Occurance analysis outputs as TSV format\n"
        "\t-C [count]    Limit output to min count occurrance (outputs 1 instance)\n"
        "\t-j, --json    Output stats as json\n"
        "\t-V, --version Output version information\n\n"
        "Indexing modes:\n"
        "\t-i [file]     Index to file to disk\n"
        "\t-I            Index to file memory\n"
        "\t-b [num]      Select number of bits, 8-64bit supported, use with -i, -I and -m\n"
        "\t-s [file]     Use with -i [file] or -I, searches the specified index against -s [file] \n"
        "\t-k            Use with -s Keep misses on index, otherwise keep misses\n\n",


        argv[0], version,argv[0]);
    return -1;
}

    char *ivalue = NULL, *idxvalue = NULL, *tvalue = NULL, *ovalue = NULL, *rvalue = NULL, *Dvalue = NULL, *Rvalue = NULL, *Svalue = NULL,*Tvalue = NULL, *zValue = NULL, *ZDir = NULL;
    int mFlag = 0, nFlag = 0, cFlag = 0, pFlag = 0, LFlag = 0, qFlag = 0, jFlag = 0, sFlag = 0, kFlag = 0, IFlag = 0 ;

    long long int S_limit_MB = 0;
    size_t maxParts = 0;


    int c;
    JOB *job;
    JOBIndex *indexjob;
    opterr = 0;

    while (1)
    {
        static struct option long_options[] =
            {
                {"output", no_argument, 0, 'o'},
                {"version", no_argument, 0, 'V'},
                {"threads", required_argument, 0, 't'},
                {"nonorder", no_argument, 0, 'n'},
                {"count", no_argument, 0, 'L'},
                {"help", no_argument, 0, 'h'},
                {"json", no_argument, 0, 'j'},
                {"index", no_argument, 0, 'i'},
                {0, 0, 0, 0}};
        int option_index = 0;

        c = getopt_long(argc, argv, "VrnmhcpqsLjkIZ:z:t:o:l:D:i:b:C:S:T:",
                        long_options, &option_index);

        if (c == -1)
            break;

        switch (c)
        {
        case 0:
            if (long_options[option_index].flag != 0)
                break;
            printf("option %s", long_options[option_index].name);
            if (optarg)
                printf(" with arg %s", optarg);
            printf("\n");
            break;
        case 'V':
            printf("%s\n\n",version);
            return(0);
        case 'z':
            zValue = optarg;
            setShigle_size(atoi(zValue));
            fprintf(stderr,"Processing in similarty mode. Shingle set to: %d\n",atoi(zValue));
            break;
        case 'Z':
            ZDir = optarg;
            break;
        case 'I':
            IFlag = true;
            indxmode = 3;
            pFlag = 1;
            nFlag = 1;
            mFlag = 1;
            break;
        case 'b':
            if (isdigit(*optarg))
            {
                int readNum = atoi(optarg);
                if (readNum >= 8 && readNum <= 64)
                {
                    mask_bits = (1ULL << readNum) - 1;
                }
                else{
                    fprintf(stderr,"Invalid bit selection must be 8-64bits\n");
                    exit(1);
                }
            }

            else{
                fprintf(stderr,"Invalid argument supplied for -b\n");
                exit(1);
            }
            break;
        case 'L':
            LFlag = true;
            break;
        case 's':
            sFlag = true;
            break;
        case 'C':
            count_limit = atoi(optarg);
            qFlag = true;
            break;
        case 'k':
            kFlag = true;
            break;
        case 'i':
            idxvalue = optarg;
            break;
        case 'j':
            jFlag = 1;
            break;
        case 'h':
            argc = 1;
            goto help;
            break;
        case 't':
            tvalue = optarg;
            break;
        case 'q':
            qFlag = true;
            break;
        case 'e':

			if (strlen(optarg) < 1)
			{
				fprintf(stderr, "Delimter requires an argument");
				exit(1);
			}
			DelimitChar = optarg;
			fprintf(stderr, "Delimit mode -e enabled, matching will occur upto the following delimiter %s\n", DelimitChar);
			break;

        case 'o':
            ovalue = optarg;
            break;
        case 'c':
            cFlag = true;
            break;
        case 'm':
            mFlag = true;
            break;
        case 'p':
            pFlag = true;
            break;
        case 'D':
            Dvalue = optarg;
            break;
        case 'T':
            Tvalue = optarg;
            break;
        case 'l':
            if (isdigit(*optarg))
                LenMatch = atoi(optarg);
            break;
        case 'n':
            nFlag = true;
            break;
        case '?':
            fprintf(stderr, "Unknown argument -%c\n", optopt);
            break;
        case 'S':
                Svalue = optarg;

                if (Svalue[0] == '-') {
                    // Attempt to parse as integer
                    long negative_val = atol(Svalue);

                    // Error handling: ensure it is less than -1
                    if (negative_val >= -1) {
                        fprintf(stderr, "Error: Argument for -R must be less than -1 (e.g., -2, -20). Value provided: %s\n", Svalue);
                        exit(1);
                    }


                    // Inverse the negative value to store in maxParts (e.g., -5 becomes 5)
                    maxParts = negative_val * -1;

            } else {


                // Parse the size string (e.g., "2.4GB")
                char *endptr;
                double size_input = strtod(Svalue, &endptr);

                // Check if parsing failed or nothing was read
                if (endptr == Svalue) {
                    fprintf(stderr, "Error: Invalid size format for -R (%s). Expected format like 1MB, 2.5GB.\n", Svalue);
                    exit(1);
                }

                if (strcasecmp(endptr, "KB") == 0 || strcasecmp(endptr, "K") == 0) {
                    size_input /= 1000.0;
                } else if (strcasecmp(endptr, "MB") == 0 || strcasecmp(endptr, "M") == 0) {
                    // Already in MB
                } else if (strcasecmp(endptr, "GB") == 0 || strcasecmp(endptr, "G") == 0) {
                    size_input *= 1000.0;
                } else if (strcasecmp(endptr, "TB") == 0 || strcasecmp(endptr, "T") == 0) {
                    size_input *= 1000000.0;
                } else {
                    fprintf(stderr, "Error: Unknown size suffix '%s' for -R. Supported: KB, MB, GB, TB, use negatives for parts eg -R -2\n", endptr);
                    exit(1);
                }

                S_limit_MB = (long long)size_input;
            }
            break;
        default:
            abort();
        }
    }

    if (S_limit_MB != 0 || maxParts != 0)
    {
        if (S_limit_MB) fprintf(stderr,"Limiting RAM usage to %lld MBytes\n",S_limit_MB);
        if (maxParts) fprintf(stderr,"Running the sort in %lld parts\n",maxParts);
    }
    ProcessingStats stats;

    if (ZDir != NULL)
    {
        fprintf(stderr,"Writing clusters to DIR: %s\n",ZDir);
    }

    if (Dvalue != NULL)
    {
        dupePtr = fopen(Dvalue,"wb");
    }

    if (LenMatch != 0 && mFlag == true)
    {
        mFlag = false;
        fprintf(stderr,"Unable to run -m with -l switch, disabling -m");
    }

    if (optind < argc)
    {
        ivalue = argv[optind++];
    }

    if (ivalue == NULL)
    {
        fprintf(stderr, "No input file or stdin specified\n");
        return -1;
    }

    if (IFlag != 0)
    {
        if (sFlag == 0)
        {
            fprintf(stderr, "I mode requires argument -s specified\n");
            exit(1);
        }
    }


    static const char *stdoutString = "stdout";
    if (ovalue != NULL)
    {
        if (strcmp(ovalue,ivalue) == 0)
        {
            fprintf(stderr,"Unable to write output to input, use >> to %s instead\n",ivalue);
            return -1;
        }
        writePtr = fopen(ovalue,"wb");
        stats.outFile = ovalue;
    }
    else
    {
        writePtr = stdout;
        stats.outFile = stdoutString;
    }


    int opCheck = optind;
    int validFiles = 0;

    while (opCheck < argc)
    {
        if (FileExists(argv[opCheck]))
        {
            validFiles++;
        }
        opCheck++;
    }

    if (validFiles == 0 && IFlag != 0)
    {
        fprintf(stderr, "No files specified for checking, exiting.\n");
        exit(1);

    }
    if (validFiles == 0)
    {
        if (!LFlag)
        {
            if (idxvalue == NULL)
            {
                if (qFlag)
                {
                    if (count_limit == 0){
                        if (!jFlag) fprintf(stderr, "Occurance analysis\n");
                    }
                    else{
                        if (!jFlag) fprintf(stderr, "Output occurance limit to %zu\n",count_limit);
                    }

                }
                else{
                    if (!jFlag) fprintf(stderr, "No valid reference files specified, running in sort mode\n");
                }

                if (nFlag)
                {
                    fprintf(stderr, "De-duplicator: Disabled\n");
                }
            }
            else
            {
                fprintf(stderr, "Index mode (disk)\n");
            }
        }
        else
        {
            if (!jFlag) fprintf(stderr, "Count mode\n");

        }

    }
    else
    {
        nFlag = false;
    }

    if (IFlag)
    {
         fprintf(stderr, "Index mode (memory)\n");
    }

    if (sFlag != 0)
    {
        if (kFlag ==0)
            fprintf(stderr, "Writing matches on index\n");
        else
            fprintf(stderr, "Writing misses on index\n");
    }


    if (idxvalue != NULL && sFlag ==0)
    {
        pFlag = 1;
        nFlag = 1;
        mFlag = 1;


        if (FileExists(idxvalue))
        {
            fprintf(stderr,"Index exits, appending to index: %s\n",idxvalue);
            indxmode = 3; //add

            size_t file_size = FileSize(idxvalue);
            fprintf(stderr,"size if %zu\n",file_size);
            if (file_size == 0)
            {
                fprintf(stderr,"Index file is 0 bytes\n");
                exit(1);
            }
            idx = fopen(idxvalue, "rb");
            char* buffer = (char*)malloc(file_size);
            if (buffer == NULL) return 0;
            size_t read_size = fread(buffer, 1, file_size, idx);
            if (read_size != file_size) {
                free(buffer);
                fprintf(stderr,"Read error, size did not match expected\n");
                exit(1);
            }

            FinalMap = roaring64_bitmap_create();

            if (FinalMap == NULL) {
                free(buffer);
                fprintf(stderr,"Unable to create index, exiting\n");
                exit(1);
            }
            fclose(idx);

            FinalMap = roaring64_bitmap_portable_deserialize_safe(buffer,read_size);
            free(buffer);
            size_t deserialized_size = roaring64_bitmap_portable_size_in_bytes(FinalMap);
            size_t occupancy = roaring64_bitmap_get_cardinality(FinalMap);

            sizeUsage(deserialized_size);

            size_t_to_pretty(occupancy, pretty, sizeof(pretty));

            fprintf(stderr,"Occupancy: %s Memory used %s\n",pretty,strings );

            const char* reason = NULL;  // Will store the error message if validation fails

            if (!roaring64_bitmap_internal_validate(FinalMap, &reason)) {
                fprintf(stderr, "Index failed validation: %s exiting\n", reason);
                exit(1);
            }
        }
        else
        {
            fprintf(stderr,"Creating new index: %s\n",idxvalue);
            indxmode = 3; //add
        }
    }
    else
    {
        if (FileExists(idxvalue))
        {

            indxmode = 4; //Use
            fprintf(stderr,"Index exits, using index: %s for matching\n",idxvalue);

            idx = fopen(idxvalue, "rb");
            size_t file_size = FileSize(idxvalue);
            char* buffer = (char*)malloc(file_size);
            if (buffer == NULL) return 0;
            size_t read_size = fread(buffer, 1, file_size, idx);
            if (read_size != file_size) {
                free(buffer);
                fprintf(stderr,"Read error, size did not match expected\n");
                exit(1);
            }

            FinalMap = roaring64_bitmap_create();
            if (FinalMap == NULL) {
                free(buffer);
                fprintf(stderr,"Unable to create index, exiting\n");
                exit(1);
            }
            fclose(idx);

            FinalMap = roaring64_bitmap_portable_deserialize_safe(buffer,read_size);
            free(buffer);
            size_t deserialized_size = roaring64_bitmap_portable_size_in_bytes(FinalMap);
            size_t occupancy = roaring64_bitmap_get_cardinality(FinalMap);

            sizeUsage(deserialized_size);

            size_t_to_pretty(occupancy, pretty, sizeof(pretty));

            fprintf(stderr,"Occupancy: %s Memory used %s\n",pretty,strings );
        }

    }

    if (!jFlag) fprintf(stderr, "Reading input: %s\n", ivalue);
    file_struct input;

    if (strcmp(ivalue,"stdin") == 0) // Reading from stdin
    {
        input.f_name = ivalue;
        if (VirtualOpen(&input) != 1)
        {
            fprintf(stderr, "File %s doesn't exist or is invalid, exiting\n", ivalue);
            return -1;
        }
    }
    else // Reading from file
    {
        input.f_name = ivalue;
        if (OpenFile02(&input) != 1)
        {
            fprintf(stderr, "File %s doesn't exist or is invalid, exiting\n", ivalue);
            return -1;
        }
    }

    stats.inputFile = ivalue;
    file_struct reference;

    unsigned int threads_count = get_nprocs();
    if (tvalue != NULL)
    {
        int tcount = atoi(tvalue);
        if (tcount > 0)
        {
            threads_count = tcount;
        }
    }

    // Dynamically create the job units
    unsigned int WUs = threads_count * 4;
    if (Svalue != NULL)
    {
        WUs = threads_count * 1.5; // reduce threads is running in low mem mode
    }
    jobs = (JOB *)malloc(WUs * sizeof(JOB));
    if (jobs == NULL)
    {
        fprintf(stderr,"Unable to allocate memory for jobs\n");
        exit(1);
    }

    buildFree(WUs);

    size_t i = 0;

    WriteOrder = new_lock(0);
    // Rolling work units
    WorkWaiting = new_lock(0);
    FreeWaiting = new_lock(WUs - 1);
    DupeWrite_Lock = new_lock(0);
    WorkHead = NULL;
    WorkTail = &WorkHead;

    // Static work units
    IndexWorkWaiting = new_lock(0);
    IndexFreeWaiting = new_lock(threads_count);
    IndexWorkHead = NULL;
    IndexWorkTail = &IndexWorkHead;

    ThreadLock = new_lock(0);

    if (IFlag)
        idxvalue = ivalue;

    if (SearchMap[0] == NULL && idxvalue != NULL) { // using bitmap, allocate memory
        // bitmap not properly initialized
        fprintf(stderr, "Using %d-bits for index\n",count_mask_bits(mask_bits));
        for (int i = 0; i<17;i++)
        {
            SearchMap[i] = roaring64_bitmap_create();
            if (SearchMap[i] == NULL)
            {
                fprintf(stderr,"Error creating bitmap\n");
                return(1);
            }
        }

    }

    for (int z = 0; z < 17; z++)
    {
        roaring64_lock[z] = new_lock(0);
    }
    struct timespec clock_start, clock_running;

    clock_gettime(CLOCK_REALTIME, &clock_start);
    clock_gettime(CLOCK_REALTIME, &clock_running);
    #define BUFFER_SIZE 104857600

    #ifdef _WIN32
          setmode(0, O_BINARY);
    #endif

    size_t totalRead = 0;

    // Valid reference file and shingvalue z set. Process in partial mode
    if (validFiles > 0 && zValue != NULL){

        fprintf(stderr,"Reading file %s\n",argv[optind]);
        if (FileExists(argv[optind]))
        {
            reference.f_name = argv[optind];
        }

        OpenFile02(&reference);


        /*  // Unsued until parallel code added
        for (int temp = 0; temp<17; temp++)
        {
            SearchMap[temp] = roaring64_bitmap_create();

            if (SearchMap[temp] == NULL) {
                fprintf(stderr,"Unable to create index, exiting\n");
            }
        }
        */

        size_t forkelem;
        forkelem = 65536;
        if (forkelem > reference.itemCount){
            forkelem = reference.itemCount / 2;
        }
        if (forkelem < 1024){
            forkelem = 1024;
        }

        //qsort_mt(reference.pointerMap, reference.itemCount, sizeof(char **), comp_cluster, threads_count, forkelem); // Sim sort
        FinalMap = roaring64_bitmap_create();
        if (FinalMap == NULL) {
            fprintf(stderr,"Unable to create index, exiting\n");
            exit(0);
        }

        //ReadFileAll(&reference);
        //IndexFile(&reference);
        while (reference.f_rem != 0)
        {
            ReadFile02(&reference);


            double read = (double)reference.bytesRead / (double)reference.f_size;
            if (!jFlag) fprintf(stderr, "\r%0.2f%% ", read * 100);
            fflush(stderr);
            IndexFile(&reference);
            for(size_t iLoop = 0; iLoop<reference.itemCount; iLoop++)
            {
                roaring64_bitmap_add(FinalMap,get_cluster_hash(reference.pointerMap[iLoop],0xFFFFFFF));
            }
        }


        OpenFile02(&input);
        for (i = 0; i < WUs; i++)
        {
            jobs[i].node_filestruct = input;
            jobs[i].mode = 5;
            jobs[i].time = 0;
        }

        for (i = 0; i < threads_count; i++)
        {
            launch(thread_search, NULL);
        }
        possess(ThreadLock);
        twist(ThreadLock, BY, threads_count);

        while (input.f_rem != 0)
        {
            double read = (double)input.bytesRead / (double)input.f_size;
            if (!jFlag) fprintf(stderr, "\r%0.2f%% ", read * 100);
            fflush(stderr);

            if (job)
                ReadFile02(&input);

            possess(FreeWaiting);
            wait_for(FreeWaiting, TO_BE_MORE_THAN, 0);
            job = FreeHead;
            FreeHead = job->next;
            twist(FreeWaiting, BY, -1);

            job->next = NULL;
            job->node_filestruct = input;
            input.buffer_size = 0;

            addWork(job, false);

        }

        if (!jFlag) fprintf(stderr, "\r100.00%%");
        fflush(stderr);

        possess(FreeWaiting);
        wait_for(FreeWaiting, TO_BE, WUs - 1);
        release(FreeWaiting);

        size_t_to_pretty(Progress_Reads, pretty, sizeof(pretty));
        size_t_to_pretty(lineCount, pretty2, sizeof(pretty2));
        fprintf(stderr, "\nWrote %s/%s similar matches\n", pretty,pretty2);
        fprintf(stderr, "Process completed in %.3f seconds\n", time_elapsed(&clock_start,1));
        exit(0);
    }

    if (strcmp(input.f_name,"stdin") == 0)
    {
        char* buffer = malloc(BUFFER_SIZE + 16);
        if (!buffer) {
            perror("Failed to allocate initial buffer");
            return (1);
        }

        size_t buffer_size = BUFFER_SIZE;
        size_t total_read = 0;
        size_t bytes_read;

        if (LFlag)
        {
            size_t total_lines = 0;
            size_t total_bytes_read = 0;

            while ((bytes_read = fread(buffer, 1, BUFFER_SIZE, stdin)) > 0) {
                // Count lines in this chunk
                total_lines += count_line_endings_sse_single(buffer,bytes_read);

                // Update progress
                total_bytes_read += bytes_read;
                fprintf(stderr, "\r%zu MBs read, %zu lines found",
                        total_bytes_read/1048576, total_lines);
                fflush(stderr);
            }

            // Print final count
            fprintf(stdout, "\nTotal number of lines: %zu\n", total_lines);

            free(buffer);

            if (ferror(stdin)) {
                perror("Error reading from stdin");
                return 1;
            }

            return 0;

        }
        else
        {
            while ((bytes_read = fread(&buffer[total_read], 1, buffer_size - total_read, stdin)) > 0) {
                total_read += bytes_read;
                if (total_read == buffer_size) {

                    buffer_size += BUFFER_SIZE;
                    fprintf(stderr, "\r%zu MBs read", buffer_size/1048567);
                    char* new_buffer = realloc(buffer, buffer_size);
                    fflush(stderr);

                    if (!new_buffer) {
                        perror("Failed to reallocate buffer");
                        free(buffer);
                        return(1);
                    }
                    buffer = new_buffer;
                }
            }

            input.buffer = buffer;
            input.bytesRead = total_read;
            input.buffer_size = total_read;
            input.f_size = total_read;

            if (!jFlag) fprintf(stderr,"\nFinished reading from stdin\n");
        }

    }
    else if(LFlag || indxmode == 3 || indxmode == 4)
    {

        int useMode = 0;
        if (LFlag) //Length analysis
        {
            useMode = 2; //Analysis
            adjustChunk(&input,5);
        }
        else
            useMode = indxmode;

        for (i = 0; i < WUs; i++)
        {
            jobs[i].node_filestruct = input;
            jobs[i].mode = useMode;
            jobs[i].time = 0;
            jobs[i].keep = kFlag;
        }

        for (i = 0; i < threads_count; i++)
        {
            launch(thread_search, NULL);
        }
        possess(ThreadLock);
        twist(ThreadLock, BY, threads_count);

        if (OpenFile02(&input) != 1)
        {
            fprintf(stderr, "File %s doesn't exist or is invalid\n", ivalue);
        }

        while (input.f_rem != 0)
        {

            double read = (double)input.bytesRead / (double)input.f_size;
            if (!jFlag) fprintf(stderr, "\r%0.2f%% ", read * 100);
            fflush(stderr);

            if (job)
                ReadFile02(&input);

            possess(FreeWaiting);
            wait_for(FreeWaiting, TO_BE_MORE_THAN, 0);
            job = FreeHead;
            FreeHead = job->next;
            twist(FreeWaiting, BY, -1);

            job->next = NULL;
            job->node_filestruct = input;
            input.buffer_size = 0;

            addWork(job, false);

        }

        if (!jFlag) fprintf(stderr, "\r100.00%%");
        fflush(stderr);

        possess(FreeWaiting);
        wait_for(FreeWaiting, TO_BE, WUs - 1);
        release(FreeWaiting);

        if (LFlag) //Analysis
        {
            if (jFlag)
            {
                LineStats stats = {
                    .input_file = ivalue,
                    .line_count = lineCount,
                    .longest_line = lineMax,
                };
                output_json_line_stats(&stats, stderr);
                return(0);
            }
            else{
            size_t_to_pretty(lineCount, pretty, sizeof(pretty));
            fprintf(stderr, "\rLine count: %s\n", pretty);
            size_t_to_pretty(lineMax, pretty, sizeof(pretty));
            fprintf(stderr, "\rLongest line: %s\n", pretty);
            fprintf(stderr, "Process completed in %.3f seconds\n", time_elapsed(&clock_start,1));
            return(0);
            }
        }
        else if(useMode ==3) //Index creation
        {

            if (IFlag == 1) //Temprarily use the index, so not write
            {
                CloseFile(&input);
                if (indxmode == 3)
                {

                    FinalMap = merge_bitmaps_inplace(SearchMap,16);

                    size_t_to_pretty(Progress_Reads, pretty, sizeof(pretty));
                    Progress_Reads = 0;
                    fprintf(stderr,"\nFinished indexing %s lines in %s took %.3f seconds\n",pretty,input.f_name,time_elapsed(&clock_running,1));
                    size_t occupancy = roaring64_bitmap_get_cardinality(FinalMap);
                    size_t_to_pretty(occupancy, pretty, sizeof(pretty));
                    fprintf(stderr,"Index occupancy: %s\n",pretty);
                }
                else if(indxmode == 4) //Just finished processing
                {
                    size_t_to_pretty(lineCount, pretty, sizeof(pretty));
                    fprintf(stderr,"\nFinished processing %s lines took %.3f seconds\n",pretty,time_elapsed(&clock_running,1));
                    totalRead += lineCount;
                    lineCount = 0;
                }

                if (optind < argc)
                {
                    input.f_name = argv[optind];
                    fprintf(stderr,"Reading file %s\n",input.f_name);
                    optind++;
                    indxmode = 4;

                }
                else
                {
                    size_t_to_pretty(Progress_Reads, pretty, sizeof(pretty));
                    size_t_to_pretty(totalRead, pretty2, sizeof(pretty2));
                    fprintf(stderr,"Lines read %s Lines written %s took %.3f seconds\n",pretty2, pretty,time_elapsed(&clock_start,1));
                    return(0);
                }
            }

            //This is a mergemap, final contains a map we need to add to final
            if (FinalMap != NULL)
            {

                roaring64_bitmap_or_inplace(SearchMap[0], FinalMap);
                roaring64_bitmap_free(FinalMap);
            }

            FinalMap = merge_bitmaps_inplace(SearchMap,16);
            size_t size_needed = roaring64_bitmap_portable_size_in_bytes(FinalMap);

            // Allocate the buffer
            char* buffer = (char*)malloc(size_needed);
            if (buffer == NULL) {
                fprintf(stderr, "Error: Could not allocate memory for serialization\n");
                return(1);
            }

            FILE* file = fopen(idxvalue, "wb");
            if (file == NULL) {
                fprintf(stderr, "Error: Could not open file '%s' for writing\n", idxvalue);
                free(buffer);
                return(1);
            }

            if (useMode == 3)
            {
                size_t_to_pretty(Progress_Reads, pretty, sizeof(pretty));
                fprintf(stderr,"\nTotal items indexed: %s took %.3f seconds\n",pretty,time_elapsed(&clock_running,1));

                size_t deserialized_size = roaring64_bitmap_portable_size_in_bytes(FinalMap);
                size_t occupancy = roaring64_bitmap_get_cardinality(FinalMap);
                sizeUsage(deserialized_size);
                size_t_to_pretty(occupancy, pretty, sizeof(pretty));
                fprintf(stderr,"Occupancy: %s Memory used %s\n",pretty,strings );

                fprintf(stderr, "Writing index to file, please wait...\n");
                roaring64_bitmap_buffered_serialize(FinalMap,file);
            }

            // Clean up
            fclose(file);
        }
        else if(useMode ==4)
        {

            if (IFlag == 1) //Temprarily use the index, so not write
            {
                CloseFile(&input);

                if(indxmode == 4) //Just finished processing
                {
                    size_t_to_pretty(lineCount, pretty, sizeof(pretty));
                    fprintf(stderr,"\nFinished processing %s lines took %.3f seconds\n",pretty,time_elapsed(&clock_running,1));
                    totalRead += lineCount;
                    lineCount = 0;
                }

                if (optind < argc)
                {
                    input.f_name = argv[optind];
                    fprintf(stderr,"Reading file %s\n",input.f_name);
                    optind++;
                    indxmode = 4;

                }
                else
                {
                    size_t_to_pretty(Progress_Reads, pretty, sizeof(pretty));
                    size_t_to_pretty(totalRead, pretty2, sizeof(pretty2));
                    fprintf(stderr,"Lines read %s Lines written %s took %.3f seconds\n",pretty2, pretty,time_elapsed(&clock_start,1));
                    return(0);
                }
            }

            size_t_to_pretty(Progress_Reads, pretty, sizeof(pretty));
            fprintf(stderr,"\nTotal matched: %s took %.3f seconds\n",pretty,time_elapsed(&clock_running,1));
        }

        fprintf(stderr, "Process completed in %.3f seconds\n", time_elapsed(&clock_start,1));
        return(0);
    }
    else // Not analysis mode, not stdin
    {
        if (maxParts == 0 && S_limit_MB == 0) // Single part processing
        {
            ReadFileAll(&input);
            if (mFlag)
            {
                for (int temp = 0; temp<17; temp++)
                {
                    SearchMap[temp] = roaring64_bitmap_create();

                    if (SearchMap[temp] == NULL) {
                        fprintf(stderr,"Unable to create index, exiting\n");
                        exit(1);
                    }
                }
            }
        }
        else if(maxParts != 0 || S_limit_MB != 0) // Multi part processing
        {
            size_t splitSize = 0;
            if (maxParts != 0){
                splitSize = (input.f_size + maxParts - 1) / maxParts;
            }
            else if (S_limit_MB != 0){
                splitSize = (S_limit_MB*1000000)/4;
            }

            input.chunkSize = splitSize;
            //setChunksize(splitSize);
            size_t currentPart = 0;
            char part_filename[2000];
            WBuffer Write_Buffer;

            initWriteBuffer(&Write_Buffer);

            size_t totalWrites = 0;


            // Init MT buffer processing
            read_eof_flag = 0;
            read_buf_ready[0] = 0;


            pthread_t reader_tid;
            pthread_create(&reader_tid, NULL, prefetch_reader_thread, &input);

            // Main Consumer Loop

            while (1) {
                // 1. Wait for data to be ready
                pthread_mutex_lock(&read_mutex);
                while (read_buf_ready[0] == 0 && read_eof_flag == 0) {
                    pthread_cond_wait(&read_cond_consumer, &read_mutex);
                }

                // Check for EOF termination
                if (read_eof_flag && read_buf_ready[0] == 0) {
                    pthread_mutex_unlock(&read_mutex);
                    break;
                }

                // 2. Swap data into 'input' struct for processing
                // We map the buffer data into the main struct so existing code works
                input.buffer = read_buffers[0].buffer;
                input.buffer_size = read_buffers[0].buffer_size;
                input.pointerMap = read_buffers[0].pointerMap;
                input.itemCount = read_buffers[0].itemCount;
                input.endAddress = read_buffers[0].endAddress;

                // Mark this buffer slot as consumed
                read_buf_ready[0] = 0;

                // Signal producer it can fill this slot again
                pthread_cond_signal(&read_cond_producer);

                pthread_mutex_unlock(&read_mutex);

                // 3. Process the chunk (Standard Logic)
                // Note: bytesRead is already updated in 'input' by the thread for progress display

                size_t forkelem;
                forkelem = 65536;
                if (forkelem > input.itemCount) forkelem = input.itemCount / 2;
                if (forkelem < 1024) forkelem = 1024;

                // Progress Display (using atomic bytesRead updated by thread)
                double read = (double)input.bytesRead / (double)input.f_size;
                if (!jFlag) fprintf(stderr, "\r%0.2f%% ", read * 100);
                fflush(stderr);


                qsort_mt(input.pointerMap, input.itemCount, sizeof(char **), compareStrings, threads_count, forkelem);
                size_t originalCount = input.itemCount;
                totalRead +=input.itemCount;
                // If de-duping
                if (validFiles)
                {
                    nFlag = false; // Force duplication if cross checking
                }


                if (nFlag == false)
                {
                    if (input.itemCount > 0)
                    {

                        size_t deDuped = 1;

                        for (size_t iLoop = 1; iLoop < input.itemCount; iLoop++)
                        {

                            if (mystrcmp(input.pointerMap[iLoop], input.pointerMap[deDuped - 1]) != 0)
                            {
                                input.pointerMap[deDuped] = input.pointerMap[iLoop];
                                deDuped++;
                            }
                        }
                        input.itemCount = deDuped;
                    }
                }

                input.itemTotal = input.itemCount;


                if (validFiles > 0){

                    memset(binary_index_start, 0, sizeof(binary_index_start));
                    memset(binary_index_end, 0, sizeof(binary_index_end));

                    size_t curr = 0, last = 0;
                    for (size_t iLoop = 0; iLoop < input.itemCount; iLoop++)
                    {
                        curr = (unsigned char)input.pointerMap[iLoop][0];
                        if (curr != last)
                        {
                            binary_index_start[curr] = iLoop;
                            binary_index_end[last] = iLoop;
                        }
                        last = curr;
                    }
                    binary_index_end[curr] = input.itemCount;

                        for (i = 0; i < WUs; i++)
                        {
                            jobs[i].node_filestruct = input;
                            jobs[i].mode = 0;
                            jobs[i].mFlag = mFlag;
                            jobs[i].time = 0;
                        }

                        for (i = 0; i < threads_count; i++)
                        {
                            launch(thread_search, NULL);
                        }
                        possess(ThreadLock);
                        twist(ThreadLock, BY, threads_count);

                        //hitCounters = (size_t*)calloc((argc-optind),sizeof(size_t));
                        size_t num_files = argc - optind;
                        hitCounters = (size_t*)calloc(num_files * COUNTER_STRIDE, sizeof(size_t));
                        if (hitCounters == NULL) {
                            fprintf(stderr, "Failed to allocate memory for hitCounters\n");
                            exit(1);
                        }


                        long file_id = 0;
                        long optind_restore = optind;

                        //Loop through the user specified files and process them
                        while (optind < argc)
                        {rvalue = argv[optind];
                            optind++;
                            reference.f_name = rvalue;

                            if (OpenFile02(&reference) != 1)
                            {
                                fprintf(stderr, "File %s doesn't exist or is invalid\n", rvalue);
                                continue;
                            }

                            if (!jFlag) fprintf(stderr, "Reading file %s\n", rvalue);

                            while (reference.f_rem != 0)
                            {
                                double read = (double)reference.bytesRead / (double)reference.f_size;
                                if (!jFlag)
                                {
                                    fprintf(stderr, "\r%0.2f%% ", read * 100);
                                    fflush(stderr);
                                }

                                if (job)
                                ReadFile02(&reference);

                                possess(FreeWaiting);
                                wait_for(FreeWaiting, TO_BE_MORE_THAN, 0);
                                job = FreeHead;
                                FreeHead = job->next;
                                twist(FreeWaiting, BY, -1);

                                job->next = NULL;
                                job->node_refstruct = reference;
                                reference.buffer_size = 0;
                                job->file_id = file_id;

                                addWork(job, false);

                            }

                            if (!jFlag) fprintf(stderr, "\rFinished reading %s E:%zu\n", reference.f_name, reference.extensions);

                            possess(FreeWaiting);
                            wait_for(FreeWaiting, TO_BE, WUs - 1);
                            release(FreeWaiting);
                            CloseFile(&reference);
                            file_id++;
                        }

                        optind = optind_restore;

                        jFlag ? stats.timings.searching_seconds = time_elapsed(&clock_running,1): fprintf(stderr, "Searching took %.3f seconds\n", time_elapsed(&clock_running,1));

                        possess(FreeWaiting);
                        wait_for(FreeWaiting, TO_BE, WUs - 1);
                        release(FreeWaiting);

                        for (i = 0; i < threads_count; i++)
                        {
                            possess(FreeWaiting);
                            wait_for(FreeWaiting, TO_BE_MORE_THAN, 0);
                            job = FreeHead;
                            FreeHead = job->next;
                            twist(FreeWaiting, BY, -1);
                            job->next = NULL;
                            addWork(job, true);
                        }

                        possess(ThreadLock);
                        wait_for(ThreadLock, TO_BE, 0);
                        release(ThreadLock);

                        i = join_all();
                 }

                if (Tvalue != NULL) {
                    snprintf(part_filename, sizeof(part_filename), "%s/%s.part%zu", Tvalue, basename(input.f_name), currentPart);
                } else {
                    snprintf(part_filename, sizeof(part_filename), "%s.part%zu", input.f_name, currentPart);
                }

                writePtr = fopen(part_filename,"wb");
                //fprintf(stderr,"writing %s\n",part_filename);

                size_t writeCounter = 0;
                //WUs = threads_count * 1.5; // Limit write packets
                for (i = 0; i < WUs; i++)
                {
                    jobs[i].node_filestruct = input;
                    jobs[i].mode = 1;
                    jobs[i].cFlag = cFlag;
                    jobs[i].counter = &writeCounter;
                    jobs[i].qFlag = qFlag;
                }

                // Call batched writer
                for (i = 0; i < threads_count; i++)
                {
                    launch(thread_search, i);
                }

                possess(ThreadLock);
                twist(ThreadLock, BY, threads_count);

                size_t processedItem = 0;
                long write_id = 0;

                // Package for writing
                while (processedItem < input.itemTotal)
                {
                    possess(FreeWaiting);
                    wait_for(FreeWaiting, TO_BE_MORE_THAN, 0);
                    job = FreeHead;
                    FreeHead = job->next;
                    twist(FreeWaiting, BY, -1);

                    job->next = NULL;
                    job->id = write_id;
                    job->start = processedItem;

                    if (processedItem + 200000 < input.itemTotal)
                    {
                        job->end = processedItem + 200000;
                    }
                    else
                    {
                        job->end = input.itemTotal;
                    }
                    processedItem = job->end;
                    addWork(job, false);
                    if (write_id > 2000)
                    {
                        write_id = 0;
                    }
                    else
                    {
                        write_id++;
                    }
                }

                for (i = 0; i < threads_count; i++)
                {
                    possess(FreeWaiting);
                    wait_for(FreeWaiting, TO_BE_MORE_THAN, 0);
                    job = FreeHead;
                    FreeHead = job->next;
                    twist(FreeWaiting, BY, -1);
                    job->next = NULL;
                    addWork(job, true);
                }

                possess(ThreadLock);
                wait_for(ThreadLock, TO_BE, 0);

                release(ThreadLock);
                i = join_all();
                fclose(writePtr);
                currentPart++;


                // Reset all locks/WUs for re-use
                buildFree(WUs);
                FreeHead = &jobs[0];
                possess(FreeWaiting);
                    twist(FreeWaiting, TO, WUs-1);
                possess(WorkWaiting);
                    twist(WorkWaiting, TO, 0);
                possess(WriteOrder);
                    twist(WriteOrder, TO, 0);
                possess(ThreadLock);
                    twist(ThreadLock, TO, 0);
                WorkHead = NULL;
                WorkTail = &WorkHead;

                if(input.buffer_size != 0)
                {
                    free(input.buffer);
                    input.buffer_size = 0;
                    free(input.pointerMap);
                }

            }

             pthread_join(reader_tid, NULL);

            if (S_limit_MB != 0)
           {
               maxParts = currentPart-1; // set this since we know how many parts were created
           }

            fprintf(stderr,"Finished creating %zu parts on disk in %.3f seconds\n",maxParts+1,time_elapsed(&clock_start,0));
            fprintf(stderr,"Merging output please wait...\n");
            file_struct *handlers = (file_struct *)calloc(maxParts+1, sizeof(file_struct));
            if (!handlers) {
                perror("Failed to allocate memory for file handlers");
                exit(EXIT_FAILURE);
            }


            for (size_t i = 0; i <= maxParts; i++) {
                size_t fn_len = sizeof(part_filename);
                handlers[i].f_name = (char *)malloc(fn_len);
                if (!handlers[i].f_name) {
                    perror("Failed to allocate filename");
                    exit(EXIT_FAILURE);
                }

                if (Tvalue != NULL) {
                    snprintf(handlers[i].f_name, sizeof(part_filename), "%s/%s.part%zu", Tvalue, basename(input.f_name), i);
                } else {
                    snprintf(handlers[i].f_name, sizeof(part_filename), "%s.part%zu", input.f_name, i);
                }

                if (OpenFile02(&handlers[i]) != 1) {
                    fprintf(stderr, "Failed to open file %s\n", handlers[i].f_name);
                    continue;
                }

                ReadFile02(&handlers[i]);
                IndexFile(&handlers[i]);
            }

            //Prep double buffering
            pf_queue_size = maxParts + 2;
            pfbufs = calloc(maxParts + 1, sizeof(PrefetchBuffer));
            pf_queue = calloc(pf_queue_size, sizeof(PrefetchRequest));
            pf_head = pf_tail = pf_stop = 0;

            for (size_t i = 0; i <= maxParts; i++) {
                pthread_mutex_init(&pfbufs[i].mutex, NULL);
                pthread_cond_init(&pfbufs[i].cond, NULL);
                pfbufs[i].ready = 0;
                pfbufs[i].eof   = (handlers[i].f_rem == 0);

                if (handlers[i].f_rem > 0) {
                    pf_queue[pf_tail % pf_queue_size] = (PrefetchRequest){
                        .handler = &handlers[i],
                        .pb      = &pfbufs[i]
                    };
                    pf_tail++;
                }

            }

            pthread_t pf_thread;
            pthread_create(&pf_thread, NULL, prefetch_thread, NULL);

            if (ovalue == NULL)
            {
                writePtr =  stdout;
            }
            else
            {
                writePtr = fopen(ovalue,"wb");
            }

            size_t *read_indices = (size_t *)calloc(maxParts+1, sizeof(size_t));


            // MERGE LOOP - LoserTree
            char *last_written_string = NULL;
            char *straggleBuffer = NULL;
            size_t straggleBuffer_size = 0;


            int *ltree = (int *)malloc((maxParts+1) * sizeof(int));
            if (!ltree) {
                fprintf(stderr, "Failed to allocate memory for loser tree\n");
                exit(1);
            }

        // Initial tree build
        build_loser_tree(ltree, maxParts + 1, handlers, read_indices);

        while (1) {
            int winner_idx = ltree[0];

            // Check if the overall winner is exhausted
            if (handlers[winner_idx].f_rem == 0 && read_indices[winner_idx] >= handlers[winner_idx].itemCount) {
                break;
            }

            char *winner_string = handlers[winner_idx].pointerMap[read_indices[winner_idx]];

            // De-duplication check
            if (last_written_string == NULL || mystrcmp(winner_string, last_written_string) != 0) {
                buffer_string2(&Write_Buffer, winner_string, MAX_READ);
                totalWrites++;
                flushBuffer(&Write_Buffer, writePtr, 0);

                // Before advancing, if this is the last string in the buffer and disk data remains,
                // we must copy it to the straggle buffer because the Prefetcher will swap the memory.
                if (read_indices[winner_idx] + 1 >= handlers[winner_idx].itemCount && handlers[winner_idx].f_rem > 0) {
                    size_t needed = strlen(winner_string) + 1;
                    if (straggleBuffer_size < needed) {
                        straggleBuffer = realloc(straggleBuffer, needed);
                        straggleBuffer_size = needed;
                    }
                    memcpy(straggleBuffer, winner_string, needed);
                    last_written_string = straggleBuffer;
                } else {
                    last_written_string = winner_string;
                }
            }

            read_indices[winner_idx]++;

            // ASYNC REFILL: If the winner's buffer is empty but disk data exists
            if (read_indices[winner_idx] >= handlers[winner_idx].itemCount && handlers[winner_idx].f_rem > 0) {
                PrefetchBuffer *pb = &pfbufs[winner_idx];

                pthread_mutex_lock(&pb->mutex);
                 while (!pb->ready && !pb->eof) {
                    pthread_cond_wait(&pb->cond, &pb->mutex);
                }


            if (pb->ready) {
                free(handlers[winner_idx].buffer);
                free(handlers[winner_idx].pointerMap);

                // Swap in the prefetched data
                handlers[winner_idx].buffer = pb->buffer;
                handlers[winner_idx].buffer_size = pb->buffer_size;
                handlers[winner_idx].pointerMap = pb->pointerMap;
                handlers[winner_idx].itemCount = pb->itemCount;
                handlers[winner_idx].endAddress = pb->endAddress;
                handlers[winner_idx].f_rem = pb->next_f_rem;

                // Reset flags and index
                pb->ready = 0;
                read_indices[winner_idx] = 0;
            } else {
                // No data ready and EOF flagged -> File is truly finished
                handlers[winner_idx].f_rem = 0;
            }
            pthread_mutex_unlock(&pb->mutex);

            if (handlers[winner_idx].f_rem > 0) {
                pthread_mutex_lock(&pf_queue_mutex);
                PrefetchRequest req = { &handlers[winner_idx], pb };
                pf_queue[pf_tail % pf_queue_size] = req;
                pf_tail++;
                pthread_cond_signal(&pf_queue_cond);
                pthread_mutex_unlock(&pf_queue_mutex);
            }
        }

            // Challenge the tree with the new (or exhausted) state
            adjust_loser_tree(ltree, winner_idx, maxParts + 1, handlers, read_indices);
        }


            free(ltree);
            free(straggleBuffer);
            flushBuffer(&Write_Buffer,writePtr,1);
            pthread_mutex_lock(&pf_queue_mutex);
            pf_stop = 1;
            pthread_cond_signal(&pf_queue_cond);
            pthread_mutex_unlock(&pf_queue_mutex);
            pthread_join(pf_thread, NULL);

            for (size_t i = 0; i <= maxParts; i++) {
                pthread_mutex_destroy(&pfbufs[i].mutex);
                pthread_cond_destroy(&pfbufs[i].cond);
            }
            free(pfbufs);
            free(pf_queue);

            size_t_to_pretty(totalRead-totalWrites, pretty, sizeof(pretty));
            size_t_to_pretty(totalWrites,pretty2, sizeof(pretty2));
            fprintf(stderr,"Unique matches %s Wrote %s lines\n",pretty,pretty2);
            fclose(writePtr);
            fprintf(stderr,"Cleaning up temp files, please wait...\n");
            for (size_t i = 0; i <= maxParts; i++) {
                CloseFile(&handlers[i]); // close handle, so we can unlink
                if (unlink(handlers[i].f_name) != 0) {
                    fprintf(stderr, "Warning: Failed to remove temp file %s\n",
                            handlers[i].f_name);
                }
            }
            free(handlers);
            free(read_indices);
            fprintf(stderr,"Total time took %.3f seconds\n",time_elapsed(&clock_start,1));
            return(1);
        }

    }
    size_t segment = 0;
    size_t chunksize = input.f_size;

    if (threads_count == 1)
    {
        threads_count = 2;
    }

    if (threads_count > 1)
    {
        chunksize = chunksize / (threads_count - 1);

        if (chunksize < 100000)
        {
            // Read it all in one shot if the file is small.
            chunksize = input.f_size;
        }
    }

    indexjobs = (JOBIndex *)malloc((threads_count + 1) * sizeof(JOBIndex));
    if (indexjobs == NULL)
    {
        fprintf(stderr, "Unable to allocate index jobs");
        exit(1);
    }
    buildIndexFree(threads_count + 1);

    // Create sub array of array of pointers
    char ***subPointerMap = (char ***)calloc((threads_count + 1),sizeof(char **));
    if (subPointerMap ==NULL)
    {
        fprintf(stderr,"Error allocating subPointerMap\n");
    }

    for (i = 0; i <= threads_count; i++)
    {
        indexjobs[i].node_filestruct = &input;
        indexjobs[i].chunk_count = 0;
        indexjobs[i].pointerMap = subPointerMap;
        indexjobs[i].id = i;
        indexjobs[i].mFlag = mFlag;
        indexjobs[i].mode = 0; //Index mode
    }

    for (i = 0; i < threads_count; i++)
    {
        launch(thread_index, NULL);
    }

    possess(ThreadLock);
    twist(ThreadLock, BY, threads_count);

    int id = 0;

    while (segment < input.bytesRead)
    {
        possess(IndexFreeWaiting);
        wait_for(IndexFreeWaiting, TO_BE_MORE_THAN, 0);
        indexjob = IndexFreeHead;
        IndexFreeHead = indexjob->next;
        twist(IndexFreeWaiting, BY, -1);

        indexjob->next = NULL;
        if (segment !=0)
            segment= segment+1;
        indexjob->start = segment;
        segment = GetChunk(&input, segment, chunksize);
        indexjob->end = segment;

        indexjob->id = id;
        id++;
        addWorkIndex(indexjob, false);
    }

    for (i = 0; i < threads_count; i++)
    {
        possess(IndexFreeWaiting);
        wait_for(IndexFreeWaiting, TO_BE_MORE_THAN, 0);
        indexjob = IndexFreeHead;
        IndexFreeHead = indexjob->next;
        twist(IndexFreeWaiting, BY, -1);

        indexjob->next = NULL;
        addWorkIndex(indexjob, true);
    }

    possess(ThreadLock);
    wait_for(ThreadLock, TO_BE, 0);
    release(ThreadLock);

    i = join_all();
    sizeUsage((input.itemTotal*8)+input.buffer_size);
    size_t_to_pretty(input.itemTotal, pretty, sizeof(pretty));
    if (!jFlag) fprintf(stderr, "Total number of lines %s Memory required (~%s)\n", pretty,strings);

    stats.input_stats.total_lines = input.itemTotal; //Return size in GB
    stats.input_stats.memory_required_bytes = ((input.itemTotal*8)+input.buffer_size); //Return size in GB

    if (mFlag == true)
    {
        FinalMap = merge_bitmaps_inplace(SearchMap,16);
        size_t occupancy = roaring64_bitmap_get_cardinality(FinalMap);
        sizeUsage(roaring64_bitmap_portable_size_in_bytes(FinalMap));
        size_t_to_pretty(occupancy, pretty, sizeof(pretty));
        fprintf(stderr, "Occupancy %s Memory used (%s)\n", pretty,strings);
    }

    char **fullPointerMap = (char **)malloc((input.itemTotal + 10) * sizeof(char *));
    size_t cumulative = 0;

    //Merge the indexed chunks into a single continuous pointer array
    for (size_t mapI = 0; mapI < threads_count; mapI++)
    {
        for (size_t subI = 0; subI < indexjobs[mapI].chunk_count; subI++)
        {
            fullPointerMap[cumulative] = subPointerMap[mapI][subI];
            cumulative++;
        }
        free(subPointerMap[mapI]);
    }

    free(subPointerMap);
    jFlag ? stats.timings.reading_seconds = time_elapsed(&clock_running,1): fprintf(stderr, "Reading took %.3f seconds\n", time_elapsed(&clock_running,1));


    if (pFlag == false) // Not presorted
    {
        if (!jFlag) fprintf(stderr, "Sorting");
        size_t forkelem;
        forkelem = 65536;
        if (forkelem > input.itemTotal){
            forkelem = input.itemTotal / 2;
        }
        if (forkelem < 1024){
            forkelem = 1024;
        }

        if (zValue != NULL)
        {
            qsort_mt(fullPointerMap, input.itemTotal, sizeof(char **), comp_cluster, threads_count, forkelem);
        }
        else{
            qsort_mt(fullPointerMap, input.itemTotal, sizeof(char **), compareStrings, threads_count, forkelem);
        }

        jFlag ? stats.timings.sorting_seconds = time_elapsed(&clock_running,1) : fprintf(stderr, " took %.3f seconds\n", time_elapsed(&clock_running,1));

    }


    WBuffer Write_Buffer;
    initWriteBuffer(&Write_Buffer);
    size_t writeCount = 0;
    if (zValue != NULL) // Similar mode processing
    {
        if (ZDir == NULL) // Writing to single file
        {
            fprintf(stderr,"Writing please wait...\n");
          if (count_limit == 0) { // No count specified
                for (size_t i = 0; i < input.itemTotal; i++) {
                    buffer_string2(&Write_Buffer, fullPointerMap[i], MAX_READ);
                    writeCount++;
                    flushBuffer(&Write_Buffer, writePtr, 0);
                }
                flushBuffer(&Write_Buffer, writePtr, 1);
            } else {

                size_t i = 0;
                while (i < input.itemTotal) {
                    uint64_t cluster = get_cluster_hash(fullPointerMap[i], 0xFFFFFFFF);
                    size_t cluster_start = i;
                    size_t cluster_count = 0;
                    // Lookahead
                    while (i < input.itemTotal &&
                           get_cluster_hash(fullPointerMap[i], 0xFFFFFFFF) == cluster) {
                        cluster_count++;
                        i++;
                    }

                    if (cluster_count >= count_limit) {
                        for (size_t j = cluster_start; j < cluster_start + cluster_count; j++) {
                            buffer_string2(&Write_Buffer, fullPointerMap[j], MAX_READ);
                            flushBuffer(&Write_Buffer, writePtr, 0);
                            writeCount++;
                        }
                    }
                }
                flushBuffer(&Write_Buffer, writePtr, 1);
            }
            size_t_to_pretty(writeCount, pretty, sizeof(pretty));
            fprintf(stderr,"Wrote %s items\n",pretty);
            fprintf(stderr, "Processing completed in %.3f seconds\n", time_elapsed(&clock_start,1));
        }
        else if(ZDir)
        {
            #ifdef _WIN
            _mkdir(ZDir);
            #else
            mkdir(ZDir, 0755);
            #endif

            uint64_t cluster = get_cluster_hash(fullPointerMap[0], 0xFFFFFFFF);
            uint64_t curr_cluster = 0;
            FILE *cluster_writePtr = NULL;
            char cluster_filename[2048];
            size_t cluster_item_count = 0;
            size_t cluster_buffer_start = 0;
            size_t last_flushed_pos = 0;

            for (size_t i = 0; i < input.itemTotal; i++)
            {
                curr_cluster = get_cluster_hash(fullPointerMap[i], 0xFFFFFFFF);

                if (curr_cluster != cluster) {
                    // Previous cluster ended, check if we keep it
                    if (count_limit > 0 && cluster_item_count < count_limit) {
                        // Discard this cluster by rewinding buffer to before it started
                        Write_Buffer.bufferUsed = cluster_buffer_start;
                        if (cluster_writePtr != NULL) {
                            fclose(cluster_writePtr);
                            cluster_writePtr = NULL;
                            unlink(cluster_filename);
                        }
                    } else if (Write_Buffer.bufferUsed > cluster_buffer_start) {
                        // Write the accepted cluster data to file
                        if (cluster_writePtr == NULL) {
                            snprintf(cluster_filename, sizeof(cluster_filename), "%s/%016llx.txt", ZDir, cluster);
                            fprintf(stderr,"Writing cluster %016llx (%zu items)\n",cluster, cluster_item_count);
                            cluster_writePtr = fopen(cluster_filename, "wb");
                            if (cluster_writePtr == NULL) {
                                fprintf(stderr, "Error opening cluster file: %s\n", cluster_filename);
                                exit(1);
                            }
                        }
                        fwrite(Write_Buffer.buffer + cluster_buffer_start, 1,
                               Write_Buffer.bufferUsed - cluster_buffer_start, cluster_writePtr);
                        fclose(cluster_writePtr);
                        cluster_writePtr = NULL;
                    }

                    // New cluster starts
                    cluster = curr_cluster;
                    cluster_item_count = 0;
                    cluster_buffer_start = Write_Buffer.bufferUsed;
                }

                buffer_string2(&Write_Buffer, fullPointerMap[i], MAX_READ);
                cluster_item_count++;

                if (Write_Buffer.bufferUsed > 0.9 * WriteBufferSize) {
                    // Buffer full, flush if current cluster meets threshold
                    if (count_limit == 0 || cluster_item_count >= count_limit) {
                        if (cluster_writePtr == NULL) {
                            snprintf(cluster_filename, sizeof(cluster_filename), "%s/%016llx.txt", ZDir, cluster);
                            fprintf(stderr,"Flushing cluster %016llx\n",cluster);
                            cluster_writePtr = fopen(cluster_filename, "ab");
                            if (cluster_writePtr == NULL) {
                                fprintf(stderr, "Error opening cluster file: %s\n", cluster_filename);
                                exit(1);
                            }
                        }
                        fwrite(Write_Buffer.buffer + last_flushed_pos, 1,
                               Write_Buffer.bufferUsed - last_flushed_pos, cluster_writePtr);
                    }

                    // Shift remaining buffer data and reset positions
                    size_t remaining = Write_Buffer.bufferUsed - cluster_buffer_start;
                    if (remaining > 0) {
                        memmove(Write_Buffer.buffer, Write_Buffer.buffer + cluster_buffer_start, remaining);
                    }
                    Write_Buffer.bufferUsed = remaining;
                    cluster_buffer_start = 0;
                    last_flushed_pos = 0;
                }
            }

            // Handle final cluster
            if (Write_Buffer.bufferUsed > cluster_buffer_start) {
                if (count_limit == 0 || cluster_item_count >= count_limit) {
                    if (cluster_writePtr == NULL) {
                        snprintf(cluster_filename, sizeof(cluster_filename), "%s/%016llx.txt", ZDir, cluster);
                        fprintf(stderr,"Writing cluster %016llx (%zu items)\n",cluster, cluster_item_count);
                        cluster_writePtr = fopen(cluster_filename, "wb");
                        if (cluster_writePtr == NULL) {
                            fprintf(stderr, "Error opening cluster file: %s\n", cluster_filename);
                            exit(1);
                        }
                    }
                    fwrite(Write_Buffer.buffer + cluster_buffer_start, 1,
                           Write_Buffer.bufferUsed - cluster_buffer_start, cluster_writePtr);
                }
            }

            if (cluster_writePtr != NULL) {
                fclose(cluster_writePtr);
            }
        }

        exit(1);

    }
    //Only run if actually searching
    if (nFlag ==0)
    {

        buildIndexFree(threads_count + 1);

        if (qFlag)
        {
            sizeUsage((input.itemTotal*8));
            fprintf(stderr, "Allocating additional memory (~%s)\n", strings);
            occuranceCounter = (size_t*) calloc(input.itemTotal,sizeof(size_t));
            if (occuranceCounter == NULL)
            {
                fprintf(stderr,"Unable to allocate memory");
            }
        }

        if (qFlag)
        {
            if (count_limit == 0){
                if (!jFlag) fprintf(stderr, "Analysis beginning\n");
            }
            else{
                if (!jFlag) fprintf(stderr, "Occurance limit starting\n");
            }
        }
        else
        {
            if (!jFlag) fprintf(stderr, "De-duplicating ");
        }
        size_t Part_Size = input.itemTotal / (threads_count + 1);

        long temp_threads_count = threads_count;

        if (Part_Size < 100000)
        {
            Part_Size = input.itemTotal;
            temp_threads_count = 1;
        }

        for (long int i = 0; i <= temp_threads_count; i++)
        {
            indexjobs[i].node_filestruct = &input;
            indexjobs[i].mode = 1; // De-dupe mode
            indexjobs[i].DeDupeMap = fullPointerMap;
            indexjobs[i].qFlag = qFlag;
            indexjobs[i].countedArr = occuranceCounter;

            if (Dvalue != NULL)
            {
                indexjobs[i].write_dupe = 1;
            }
        }

        for (long int i = 0; i < temp_threads_count; i++)
        {
            launch(thread_index, NULL);
        }

        possess(ThreadLock);
        twist(ThreadLock, BY, temp_threads_count);

        //Deduplicate the file by dividing into chunks
        for (long int i = 0; i < temp_threads_count; i++)
        {
            possess(IndexFreeWaiting);
            wait_for(IndexFreeWaiting, TO_BE_MORE_THAN, 0);
            indexjob = IndexFreeHead;
            IndexFreeHead = indexjob->next;
            twist(IndexFreeWaiting, BY, -1);

            indexjob->next = NULL;
            indexjob->start = Part_Size * i;
            indexjob->end = Part_Size * (i + 1);
            indexjob->DeDupeMap = fullPointerMap;
            indexjob->mode = 1;

            if (i == temp_threads_count - 1)
            {
                indexjob->end = input.itemTotal;
            }

            addWorkIndex(indexjob, false);
        }

        for (long int i = 0; i < temp_threads_count; i++)
        {
            possess(IndexFreeWaiting);
            wait_for(IndexFreeWaiting, TO_BE_MORE_THAN, 0);
            indexjob = IndexFreeHead;
            IndexFreeHead = indexjob->next;
            twist(IndexFreeWaiting, BY, -1);

            indexjob->next = NULL;
            addWorkIndex(indexjob, true);
        }

        possess(ThreadLock);
        wait_for(ThreadLock, TO_BE, 0);
        release(ThreadLock);

        i = join_all();

        size_t actualEnd = indexjobs[0].end;

        //De-dupe between each chunks cross border
        for (long int i = 1; i < temp_threads_count; i++)
        {
            int offset = 0;

            if (mystrcmp(fullPointerMap[indexjobs[i - 1].end - 1], fullPointerMap[indexjobs[i].start]) == 0)
            {
                offset = 1;

                if (qFlag)
                {
                    occuranceCounter[actualEnd-1] = occuranceCounter[(indexjobs[i - 1].end - 1)] + occuranceCounter[(indexjobs[i].start)];
                }
            }
            size_t new_start = indexjobs[i].start + offset;

            for (size_t z = new_start; z < indexjobs[i].end; z++)
            {
                if (qFlag)
                {
                    occuranceCounter[actualEnd] = occuranceCounter[z];
                }

                fullPointerMap[actualEnd] = fullPointerMap[z];
                actualEnd++;
            }
        }


        if (qFlag)
        {
            //heap_sort(occuranceCounter, fullPointerMap, actualEnd,1);
            sort_arrays(occuranceCounter, fullPointerMap, actualEnd);
            fprintf(stderr,"Finished\n");
        }


        if (jFlag){
            stats.timings.deduplicating.deduplication_seconds = time_elapsed(&clock_running,1);
            stats.timings.deduplicating.duplicate_lines =input.itemTotal - actualEnd;
            stats.timings.deduplicating.unique_lines =actualEnd;
        }
        else{
            if (input.itemTotal < actualEnd)
            {
                input.itemTotal = actualEnd;
            }
            size_t_to_pretty(input.itemTotal - actualEnd, pretty, sizeof(pretty));
            size_t_to_pretty(actualEnd, pretty2, sizeof(pretty2));
            fprintf(stderr, "%s unique (%s duplicate lines)", pretty2,pretty);
            fprintf(stderr, " took %.3f seconds\n", time_elapsed(&clock_running,0));
        }

        if (Dvalue != NULL)
            fprintf(stderr,"Duplicates written to file: %s\n",Dvalue);

        input.itemTotal = actualEnd;
        input.itemCount = actualEnd;
    }

    //Index the pointers for faster binary searches
    if (validFiles > 0)
    {
        if (!jFlag) fprintf(stderr, "Indexing");
        size_t curr = 0;
        size_t last = 0;

        for (i = 0; i<input.itemTotal;i++)
        {
            if (maxParts != 0 || S_limit_MB != 0){ // depending on multi-part
                curr =(unsigned char)input.pointerMap[i][0];
            }
            else{
                curr =(unsigned char)fullPointerMap[i][0];
            }

            if (curr != last)
            {
                binary_index_start[curr] = i;
                binary_index_end[last] = i;
            }
            last = curr;
        }
        binary_index_end[curr] =input.itemTotal;

        jFlag ? stats.timings.indexing_seconds =time_elapsed(&clock_running,1): fprintf(stderr, " took %.3f seconds\n", time_elapsed(&clock_running,1));

    }

    if (LenMatch !=0)
    {
        if (!jFlag) fprintf(stderr,"Search will match upto length: %d\n",LenMatch);
        for (int i = 0; i < NUM_SEGMENTS; i++) {
            pthread_mutex_init(&pointerMapMutexes[i].mutex, NULL);
        }
    }

    if (maxParts == 0 && S_limit_MB == 0)
        input.pointerMap = fullPointerMap;

    for (i = 0; i < WUs; i++)
    {
        jobs[i].node_filestruct = input;
        jobs[i].mode = 0;
        jobs[i].mFlag = mFlag;
        jobs[i].time = 0;
    }

    for (i = 0; i < threads_count; i++)
    {
        launch(thread_search, NULL);
    }
    possess(ThreadLock);
    twist(ThreadLock, BY, threads_count);

    hitCounters = (size_t*)calloc((argc-optind) * COUNTER_STRIDE, sizeof(size_t));
    long file_id = 0;
    long optind_restore = optind;

    //Loop through the user specified files and process them
    while (optind < argc)
    {rvalue = argv[optind];
        optind++;
        reference.f_name = rvalue;

        if (OpenFile02(&reference) != 1)
        {
            fprintf(stderr, "File %s doesn't exist or is invalid\n", rvalue);
            continue;
        }

        if (!jFlag) fprintf(stderr, "Reading file %s\n", rvalue);
        size_t cycles = 0;
        size_t adjusted = 0;
        while (reference.f_rem != 0)
        {
            double read = (double)reference.bytesRead / (double)reference.f_size;
            if (!jFlag)
            {
                fprintf(stderr, "\r%0.2f%% ", read * 100);
                fflush(stderr);
            }

            if (job)
            ReadFile02(&reference);

            possess(FreeWaiting);
            wait_for(FreeWaiting, TO_BE_MORE_THAN, 0);
            job = FreeHead;
            FreeHead = job->next;
            twist(FreeWaiting, BY, -1);

            job->next = NULL;
            job->node_refstruct = reference;
            reference.buffer_size = 0;
            job->file_id = file_id;
            if (job->time != 0)
            {
                if (job->time < 0.02)
                {
                    if (adjusted + WUs < cycles) //We only run the check when the batch of threads finish (which is what cycles holds)
                    {
                        if (!jFlag) fprintf(stderr,"\rAdjusting workload\n");
                        adjustChunk(&reference,1);
                        adjusted = cycles;
                    }
                }
            }

            addWork(job, false);
            cycles++;
        }


        if (!jFlag) fprintf(stderr, "\rFinished reading %s E:%zu\n", reference.f_name, reference.extensions);

        possess(FreeWaiting);
        wait_for(FreeWaiting, TO_BE, WUs - 1);
        release(FreeWaiting);
        CloseFile(&reference);
        file_id++;
    }

    jFlag ? stats.timings.searching_seconds = time_elapsed(&clock_running,1): fprintf(stderr, "Searching took %.3f seconds\n", time_elapsed(&clock_running,1));

    possess(FreeWaiting);
    wait_for(FreeWaiting, TO_BE, WUs - 1);
    release(FreeWaiting);

    for (i = 0; i < threads_count; i++)
    {
        possess(FreeWaiting);
        wait_for(FreeWaiting, TO_BE_MORE_THAN, 0);
        job = FreeHead;
        FreeHead = job->next;
        twist(FreeWaiting, BY, -1);
        job->next = NULL;
        addWork(job, true);
    }

    possess(ThreadLock);
    wait_for(ThreadLock, TO_BE, 0);
    release(ThreadLock);

    i = join_all();


    //Print out the stats for each reference file
    file_id = 0;
    while(optind_restore < argc)
    {
        size_t_to_pretty(hitCounters[file_id * COUNTER_STRIDE], pretty, sizeof(pretty));
        if (!jFlag) fprintf(stderr, "%s %s\n",argv[optind_restore],pretty);
        optind_restore++;
        file_id++;
    }

    if (ovalue != NULL)
    {
        if (!jFlag) fprintf(stderr, "Writing ");
    }
    else
    {
        if (!jFlag) fprintf(stderr, "Writing\n");
    }

    size_t writeCounter = 0;
    for (i = 0; i < WUs; i++)
    {
        jobs[i].node_filestruct = input;
        jobs[i].mode = 1;
        jobs[i].cFlag = cFlag;
        jobs[i].counter = &writeCounter;
        jobs[i].qFlag = qFlag;
    }

    for (i = 0; i < threads_count; i++)
    {
        launch(thread_search, i);
    }

    possess(ThreadLock);
    twist(ThreadLock, BY, threads_count);

    size_t processedItem = 0;
    long write_id = 0;

    while (processedItem < input.itemTotal)
    {
        possess(FreeWaiting);
        wait_for(FreeWaiting, TO_BE_MORE_THAN, 0);
        job = FreeHead;
        FreeHead = job->next;
        twist(FreeWaiting, BY, -1);

        job->next = NULL;
        job->id = write_id;
        job->start = processedItem;

        if (processedItem + 800000 < input.itemTotal)
        {
            job->end = processedItem + 800000;
        }
        else
        {
            job->end = input.itemTotal;
        }

        processedItem = job->end;
        addWork(job, false);
        if (write_id > 2000)
        {
            write_id = 0;
        }
        else
        {
            write_id++;
        }
    }

    for (i = 0; i < threads_count; i++)
    {
        possess(FreeWaiting);
        wait_for(FreeWaiting, TO_BE_MORE_THAN, 0);
        job = FreeHead;
        FreeHead = job->next;
        twist(FreeWaiting, BY, -1);
        job->next = NULL;
        addWork(job, true);
    }

    possess(ThreadLock);
    wait_for(ThreadLock, TO_BE, 0);
    release(ThreadLock);

    i = join_all();

    size_t_to_pretty(input.itemTotal-writeCounter, pretty, sizeof(pretty));
    size_t_to_pretty(writeCounter, pretty2, sizeof(pretty2));

    jFlag ? stats.timings.writing_seconds = time_elapsed(&clock_running,1) : fprintf(stderr, "took %.3f seconds\n", time_elapsed(&clock_running,1));

    if (jFlag){
        stats.search_results.lines_written = writeCounter;
        stats.search_results.unique_matches = input.itemTotal-writeCounter;
    }
    else{
        fprintf(stderr, "Unique matches: %s Wrote %s lines\n",pretty,pretty2);
    }

    jFlag ? stats.timings.total_time_seconds = time_elapsed(&clock_start,1) : fprintf(stderr, "Total time took %.3f seconds\n", time_elapsed(&clock_start,1));


    if (jFlag)
         output_json_stats(&stats, stderr);

    return 0;
}
