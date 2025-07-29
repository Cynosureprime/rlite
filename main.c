#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>
#include <string.h>
#include <fcntl.h>


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

#define XXH_INLINE_ALL
#include "xxhash.h"
#include "roaring.h"


#define NUM_SEGMENTS 4096

#include <pthread.h>
pthread_mutex_t pointerMapMutexes[NUM_SEGMENTS];

#define WriteBufferSize 10240000
#define version "0.33-x"

roaring64_bitmap_t *SearchMap[17];
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
long count_limit = 0;
//mode 0:build, 1:use, 2:add

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

int comp2(const void *a, const void *b) {
    char *a1 = (char *)a;

    char * ptr = b;

    if (isTagged(ptr))
        ptr = untagPointer(ptr);

    return(mylstrcmp(a1,ptr));
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

// not as fancy as rling vectorization code used for debugging
size_t mystrlen(char *line)
{
    char *ret = strchr(line, 0xA);
    if (ret != NULL)
    {
        return ret - line;
    }
    else
    {
        for(int i =0; i<100;i++)
        {
            fprintf(stderr,"%c\t %d\n",line[i],(int)line[i]);
        }
        fprintf(stderr, "Error getting line length\n");
        exit(1);
    }
}


// not as fancy as rling vectorization code, but this works
size_t mystrlen2(char *line,size_t bytes)
{
    char *ret = memchr(line, 0xA,bytes);
    if (ret != NULL)
    {
        size_t len = ret -line;
        if ((ret -line) > 0)
        {
            if ((unsigned char)line[len-1] == 0xD)
            {
                return (len-1);
            }
        }
        return len;
    }
    else
    {

        fprintf(stderr, "Error getting line length %zu \n",bytes);
        exit(1);
    }
}

//Gets a stringth length, requires the starter pointer and also max address (for debugging)
size_t mystrlen3(char *line,size_t bytes,size_t one, size_t two)
{

    char *ret = memchr(line, 0xA,bytes);
    if (ret != NULL)
    {
        size_t len = ret -line;
        if ((ret -line) > 0)
        {
            if ((unsigned char)line[len-1] == 0xD)
            {
                return (len-1);
            }
        }
        return len;
    }
    else
    {
        fprintf(stderr, "Error getting line length %zu %zu %zu\n",bytes,one, two);
        exit(1);
    }
    return 0;
}

//Used for debugging
void myputs(char *line)
{
    char buffer[BUFSIZ];
    fprintf(stderr,"string length is %zu\n",mystrlen2(line,100000000));
    memcpy(buffer,line,mystrlen2(line,100000000));
    buffer[mystrlen(line)] = 0;
    fprintf(stderr,"%s\n",buffer);
}

char* size_t_to_pretty(size_t num, char* buffer, size_t buffer_size) {
    char temp[64];
    int len, i, j;

    // Convert number to string
    snprintf(temp, sizeof(temp), "%zu", num);
    len = strlen(temp);

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


//Used for debugging
size_t myputs2(char *line,size_t bytes)
{
    char buffer[BUFSIZ];
    fprintf(stderr,"string length is %zu\n",mystrlen2(line,100000000));

    memcpy(buffer,line,mystrlen2(line,bytes));
    buffer[mystrlen2(line,bytes)] = 0;
    for (size_t i =0; i<=mystrlen2(line,bytes); i++)
    {
        fprintf(stderr,"%d\t",buffer[i]);
    }
    fprintf(stderr,"%s\n",buffer);
}

//Used for locking pointermap during inner tagging, will re-aquire new lock when needed
long long int mutex_relock(long long int currIdx, long long int nextIdx)
{
    pthread_mutex_unlock(&pointerMapMutexes[currIdx]);
    pthread_mutex_lock(&pointerMapMutexes[nextIdx]);

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
} JOB;

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

} JOBIndex;

typedef struct writeBuffer
{
    size_t bufferSize;
    size_t bufferUsed;
    char *buffer;
    size_t writeCount;
} WBuffer;

void buffer_string2(WBuffer *WStruct, char *string,size_t max)
{
    size_t len = mystrlen2(string,max); //Since our buffer is always delimieted with 0xA we can make this unsafe call

        if (len >= (WStruct->bufferSize - WStruct->bufferUsed))
        {
            // We need to increase the buffer larger than the default size fo accomodate the len
            if (len > WriteBufferSize)
            {
                WStruct->buffer = (char *)realloc(WStruct->buffer, WStruct->bufferSize + (len * 2) + 1);
                if (WStruct->buffer == NULL)
                {

                    fprintf(stderr, "Unable to realloc buffer for writing exiting");
                    exit(1);
                }
                else
                {
                    fprintf(stderr, "Increasing write buffer\n");
                    WStruct->bufferSize += (len * 2);
                }
            }
            else
            {
                WStruct->buffer = (char *)realloc(WStruct->buffer, WStruct->bufferSize + WriteBufferSize + 1);
                if (WStruct->buffer == NULL)
                {
                    fprintf(stderr, "Unable to realloc buffer for writing exiting");
                    exit(1);
                }
                else
                {
                    WStruct->bufferSize += (WriteBufferSize);
                }
            }
        }

        memcpy(WStruct->buffer + WStruct->bufferUsed, string, len);

        WStruct->buffer[len + WStruct->bufferUsed] = 10;
        WStruct->bufferUsed += (len + 1);
        WStruct->writeCount++;

}

//Write string with counts
void buffer_string3(size_t * arr, size_t iLoopIdx, WBuffer *WStruct, char *string,size_t max)
{
    size_t len = mystrlen2(string,max); //Since our buffer is always delimieted with 0xA we can make this unsafe call


        if ((len + 12)>= (WStruct->bufferSize - WStruct->bufferUsed))
        {
            // We need to increase the buffer larger than the default size fo accomodate the len
            if (len > WriteBufferSize)
            {
                WStruct->buffer = (char *)realloc(WStruct->buffer, WStruct->bufferSize + (len * 2) + 1);
                if (WStruct->buffer == NULL)
                {
                    fprintf(stderr, "Unable to realloc buffer for writing exiting");
                    exit(1);
                }
                else
                {
                    fprintf(stderr, "Increasing write buffer\n");
                    WStruct->bufferSize += (len * 2);
                }
            }
            else
            {
                WStruct->buffer = (char *)realloc(WStruct->buffer, WStruct->bufferSize + WriteBufferSize + 1);
                if (WStruct->buffer == NULL)
                {
                    fprintf(stderr, "Unable to realloc buffer for writing, exiting");
                    exit(1);
                }
                else
                {
                    WStruct->bufferSize += (WriteBufferSize);
                }
            }
        }

        //atoi is not standard use snprintf (max 4B u32, but provision len 12)
        if (count_limit == 0)
        {
            char str[12];
            snprintf(str, sizeof(str), "%zu", arr[iLoopIdx]);
            memcpy(WStruct->buffer + WStruct->bufferUsed,str,strlen(str));
            WStruct->buffer[WStruct->bufferUsed+strlen(str)] =9;
            WStruct->bufferUsed +=(strlen(str) +1);

            memcpy(WStruct->buffer + WStruct->bufferUsed, string, len);
            WStruct->buffer[len + WStruct->bufferUsed] = 10;
            WStruct->bufferUsed += (len + 1);
            WStruct->writeCount++;
        }
        else
        {
            if (arr[iLoopIdx] >= count_limit)
            {
                memcpy(WStruct->buffer + WStruct->bufferUsed, string, len);
                WStruct->buffer[len + WStruct->bufferUsed] = 10;
                WStruct->bufferUsed += (len + 1);
                WStruct->writeCount++;
            }
        }



}
JOBIndex *indexjobs;
JOB *jobs;

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

    FreeTail = NULL;
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

    IndexFreeTail = NULL;
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
                Write_Buffer.bufferSize = WriteBufferSize + 1;
                Write_Buffer.bufferUsed = 0;
                Write_Buffer.writeCount = 0;

                Write_Buffer.buffer = (char *)malloc(WriteBufferSize + 1);
                if (Write_Buffer.buffer ==NULL)
                {
                    fprintf(stderr,"Error allocating write buffer");
                    exit(1);
                }
            }


            //Analysis
            if (job->qFlag)
            {

                for (size_t line = (job->start + 1); line < job->end; line++)
                {

                    if (mystrcmp(job->DeDupeMap[line], job->DeDupeMap[actualIndex]) == 0)
                    {

                        job->countedArr[actualIndex]++;

                        if (job->write_dupe ==1)
                        {

                            buffer_string2(&Write_Buffer,job->DeDupeMap[line],MAX_READ);

                            if (Write_Buffer.bufferUsed > 30000000)
                            {
                                possess(DupeWrite_Lock);
                                    fwrite(Write_Buffer.buffer, 1, Write_Buffer.bufferUsed, dupePtr);
                                release(DupeWrite_Lock);
                                Write_Buffer.bufferSize = WriteBufferSize + 1;
                                Write_Buffer.bufferUsed = 0;
                                Write_Buffer.writeCount = 0;
                            }
                        }

                        job->DeDupeMap[line][0] = 10;
                    }
                    else
                    {

                        job->countedArr[actualIndex]++;

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

                            if (Write_Buffer.bufferUsed > 30000000)
                            {
                                possess(DupeWrite_Lock);
                                    fwrite(Write_Buffer.buffer, 1, Write_Buffer.bufferUsed, dupePtr);
                                release(DupeWrite_Lock);
                                Write_Buffer.bufferSize = WriteBufferSize + 1;
                                Write_Buffer.bufferUsed = 0;
                                Write_Buffer.writeCount = 0;
                            }
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
                if (Write_Buffer.bufferUsed > 0)
                {
                    possess(DupeWrite_Lock);
                        fwrite(Write_Buffer.buffer, 1, Write_Buffer.bufferUsed, dupePtr);
                    release(DupeWrite_Lock);
                    Write_Buffer.bufferUsed = 0;
                }
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
            release(WorkWaiting);
            exit(1);
        }

        job = WorkHead;
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
                __sync_fetch_and_add(&hitCounters[file_id], searchHits);
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
                    __sync_fetch_and_add(&hitCounters[file_id], searchHits);
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

                            pthread_mutex_lock(&pointerMapMutexes[segmentIndex]);

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
                            pthread_mutex_unlock(&pointerMapMutexes[segmentIndex]);
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
            Write_Buffer.bufferSize = WriteBufferSize + 1;
            Write_Buffer.bufferUsed = 0;
            Write_Buffer.writeCount = 0;

            Write_Buffer.buffer = (char *)malloc(WriteBufferSize + 1);
            if (Write_Buffer.buffer ==NULL)
            {
                fprintf(stderr,"Error allocating write buffer");
                exit(1);
            }

            char * ptr;

            size_t localWriteCount = 0;
            size_t BuffRem = 0;

            if (!job->qFlag)
            {
                for (iLoop = job->start; iLoop < job->end; iLoop++)
                {
                    if (isTagged(job->node_filestruct.pointerMap[iLoop]) == job->cFlag)
                    {
                        BuffRem = job->node_filestruct.endAddress - (uintptr_t)job->node_filestruct.pointerMap[iLoop];
                        ptr = untagPointer(job->node_filestruct.pointerMap[iLoop]);
                        buffer_string2(&Write_Buffer, ptr,BuffRem);
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
                        buffer_string3(occuranceCounter,iLoop,&Write_Buffer, ptr,BuffRem);
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


            break;

        case 3: //Index mode (build by chunks)
            IndexFile(&job->node_filestruct);

            size_t buf_max = &(job->node_filestruct.buffer) + job->node_filestruct.buffer_size;
            buf_max++;

            uint32_t digests[4];
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
            Write_Buffer.bufferSize = WriteBufferSize + 1;
            Write_Buffer.bufferUsed = 0;
            Write_Buffer.writeCount = 0;

            Write_Buffer.buffer = (char *)malloc(WriteBufferSize + 1);
            if (Write_Buffer.buffer ==NULL)
            {
                fprintf(stderr,"Error allocating write buffer");
                exit(1);
            }

            buf_max = &(job->node_filestruct.buffer) + job->node_filestruct.buffer_size;
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

        }

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
    }
}

void sizeUsage(size_t in) {
    int counter = 0;
    double size = in;
    while(size > 1000)
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
        "\t-m            Enable lookup map, useful for high number of searches\n"
        "\t-o [file]     Output to a file, defaults to stdout\n"
        "\t-n            Do not remove duplicates\n"
        "\t-c            Write items in common between lists\n"
        "\t-l [len]      Limit matching up to a specified length\n"
        "\t-e [char]     Limit matching up to a specified character (not implemented)\n"
        "\t-p            Input list is pre-sorted, do not perform sort\n"
        "\t-D [file]     Write duplicates to file\n"
        "\t-r [dir]      Read and recurse a directory (not implemented)\n"
        "\t-L, --count   Line Count with longest count\n"
        "\t-q            Occurance analysis outputs as TSV format\n"
        "\t-C [count]    Limit output to min count occurrance (outputs 1 instance)\n"
        "\t-j, --json    Output stats as json\n"
        "V-j, --version  Output version information\n\n"
        "Indexing modes:\n"
        "\t-i [file]     Index to file to disk\n"
        "\t-I            Index to file memory\n"
        "\t-b [num]      Select number of bits, 8-64bit supported, use with -i, -I and -m\n"
        "\t-s [file]     Use with -i [file] or -I, searches the specified index against -s [file] \n"
        "\t-k            Use with -s Keep misses on index, otherwise keep misses\n\n",


        argv[0], version,argv[0]);
    return -1;
}

    char *ivalue = NULL, *idxvalue = NULL, *bitsValue = NULL, *tvalue = NULL, *ovalue = NULL, *rvalue = NULL, *Dvalue = NULL;
    int mFlag = 0, nFlag = 0,cFlag = 0, pFlag = 0, LFlag = 0, qFlag = 0, jFlag = 0, sFlag = 0, kFlag = 0, IFlag = 0;
    long limit = 0;

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

        c = getopt_long(argc, argv, "VrnmhcpqsLjkIt:o:l:D:i:b:C:",
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

        default:
            abort();
        }
    }

    ProcessingStats stats;


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
        if (FileExists(argv[optind]))
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
                    if (count_limit == 0)
                        if (!jFlag) fprintf(stderr, "Occurance analysis\n");
                    else
                        if (!jFlag) fprintf(stderr, "Output occurance limit to %ld\n",count_limit);
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

            if (!roaring64_bitmap_internal_validate(SearchMap, &reason)) {
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

    if (strcmp(ivalue,"stdin") == 0)
    {
        input.f_name = ivalue;
        if (VirtualOpen(&input) != 1)
        {
            fprintf(stderr, "File %s doesn't exist or is invalid, exiting\n", ivalue);
            return -1;
        }
    }
    else
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

    if (SearchMap[0] == NULL && idxvalue != NULL) {
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
ReRun:

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
            adjustChunk(5);
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
            fprintf(stderr, "Finished took %.3f seconds\n", time_elapsed(&clock_start,1));
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
                    goto ReRun;
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
                    goto ReRun;
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

        fprintf(stderr, "Finished took %.3f seconds\n", time_elapsed(&clock_start,1));
        return(0);
    }
    else
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

    //Perform a quicksort on the the pointers
    if (pFlag == false)
    {
        if (!jFlag) fprintf(stderr, "Sorting");
        long long int forkelem;
        forkelem = 65536;
        if (forkelem > input.itemTotal)
            forkelem = input.itemTotal / 2;
        if (forkelem < 1024)
            forkelem = 1024;

        qsort_mt(fullPointerMap, input.itemTotal, sizeof(char **), compareStrings, threads_count, forkelem);
        jFlag ? stats.timings.sorting_seconds = time_elapsed(&clock_running,1) : fprintf(stderr, " took %.3f seconds\n", time_elapsed(&clock_running,1));

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
            if (count_limit == 0)
                if (!jFlag) fprintf(stderr, "Analysis beginning\n");
            else
                if (!jFlag) fprintf(stderr, "Occurance limit starting\n");
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
            sort_arrays(occuranceCounter, fullPointerMap, actualEnd,threads_count);
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

            curr =(unsigned char)fullPointerMap[i][0];
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
            pthread_mutex_init(&pointerMapMutexes[i], NULL);
        }
    }

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

    hitCounters = (size_t*)calloc((argc-optind),sizeof(size_t));
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

            // printf("%zu remaining\n",reference.f_rem);
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
                        adjustChunk(1);
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
        size_t_to_pretty(hitCounters[file_id], pretty, sizeof(pretty));
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
