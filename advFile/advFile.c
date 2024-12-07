/*-
 * AdvFile framework coded by Simon G
 * A framework written specifically for Unified List Manager (ULM)
 * This framework consists of functions to read, write and quickly index lists
 * Last updated 7/12/2024
 */

#include <emmintrin.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <fcntl.h>
#include <stdint.h>
//#define chunksize 4251200
#define _FILE_OFFSET_BITS 64
#define _LARGEFILE64_SOURCE
#include "advFile.h"
size_t chunksize = 1048576;

void adjustChunk(int direction)
{
    if (direction > 0)
        chunksize = chunksize *(2*direction);
    else if (direction < 0)
        chunksize = chunksize /(2*abs(direction));
}

size_t count_line_endings_sse_single(const char* buffer, size_t length) {

    if (length == 0) return 0;

    size_t count = 0;
    const char* p = buffer;

    // Handle unaligned bytes at start
    while (((uintptr_t)p & 0xF) && length > 0) {
        if (*p == '\n') {
            count++;
        }
        p++;
        length--;
    }

    // Create comparison vectors
    __m128i lf_vec = _mm_set1_epi8('\n');  // LF = 0x0A
    __m128i cr_vec = _mm_set1_epi8('\r');  // CR = 0x0D

    // Process 16 bytes at a time
    while (length >= 16) {
        __m128i chunk = _mm_load_si128((__m128i*)p);

        // Find all LFs
        __m128i cmp_lf = _mm_cmpeq_epi8(chunk, lf_vec);
        unsigned lf_mask = _mm_movemask_epi8(cmp_lf);

        // Find all CRs
        __m128i cmp_cr = _mm_cmpeq_epi8(chunk, cr_vec);
        unsigned cr_mask = _mm_movemask_epi8(cmp_cr);

        // Process each LF found
        while (lf_mask) {
            int pos = __builtin_ctz(lf_mask);  // Position of current LF

            // Check if this LF is preceded by CR
            // If we're at position 0 of the chunk, need to check previous byte
            if (pos > 0) {
                // Check if there's a CR in the bit before this LF in current chunk
                if (cr_mask & (1 << (pos - 1))) {
                    // This is part of CRLF - we'll count it as one line ending
                    count++;
                } else {
                    // This is a standalone LF
                    count++;
                }
            } else if (p != buffer) {
                // We're at position 0, check previous byte for CR
                if (*(p - 1) == '\r') {
                    // This is part of CRLF
                    count++;
                } else {
                    // This is a standalone LF
                    count++;
                }
            } else {
                // This is the first byte of the buffer, must be standalone LF
                count++;
            }

            // Clear the processed bit and continue
            lf_mask &= ~(1 << pos);
        }

        p += 16;
        length -= 16;
    }


    // Handle remaining bytes
    while (length > 0) {
        if (*p == '\n') {
            if (p != buffer && *(p - 1) == '\r') {
               count++;
            } else {
                // This is a standalone LF
                count++;
            }
        }
        p++;
        length--;
    }

    //count++;

    return count;
}


void CountChunkV2(file_struct *file)
{

    // Check if the file has been read first
    if (file->buffer == NULL)
    {
        fputs("File has not been read yet", stderr);
        exit(3);
    }

    long long int i = 0;

    file->itemCount = 0;

    int override = 0;

    // Account for BOM (Byte order marking in UTF-8 lists)
    // Only run if we haven't indexed since BOM is at BOF
    if (file->itemTotal == 0)
    {
        if (file->buffer_size > 2)
        {
            // UTF-8 BOM
            if ((unsigned char)file->buffer[0] == 239 && (unsigned char)file->buffer[2] == 187 && (unsigned char)file->buffer[2] == 191)
            {
                override = 3;
            }
            // UTF-16 BOM
            else if ((unsigned char)file->buffer[0] == 254 && (unsigned char)file->buffer[2] == 255)
            {
                override = 2;
            }
        }
    }

    long long int ActualEnd = 0;
    for (i = file->buffer_size - 16; i >= 0; i--)
    {
        if ((unsigned char)file->buffer[i] != 10)
        {
            // Actual end should be char after the char not crlf (we should have allocated extra space for this char)
            ActualEnd = i;
            break;
        }
    }
    for (i = override; i < ActualEnd; i++)
    {
        if ((unsigned char)file->buffer[i] != 10)
        {
            override = i;
            break;
        }
    }

    // Important:
    // The itemCount as this stage is not 100% accurate and can sometimes be overestimate (which is good) since we always have enough malloc space
    // The over estimate is caused by the fact that some files end with Cr/Lf while some simply end with the last string as is
    // The itemCount is later realigned and readjusted to the correct value in the marked code below.

    if (file->pointerMap_size != 0)
    {
        free(file->pointerMap);
    }

    int trigger = 1;
    size_t counter = 0;
    size_t currentLen = 0;
    size_t longestLen = 0;

    for (i = override; i <= ActualEnd; i++)
    {
        if ((unsigned char)file->buffer[i] != 0xA && trigger == 1)
        {
            if((unsigned char)file->buffer[i] != 0xD)
            {
                if (i>2)
                {
                    if((unsigned char)file->buffer[i-1] == 0xA && (unsigned char)file->buffer[i-2] == 0xD)
                        currentLen--;
                }
                trigger = 0;

                if (currentLen > longestLen)
                    longestLen = currentLen;
                currentLen = 0;
                counter++;
            }
            else if((unsigned char)file->buffer[i] == 0xD && (unsigned char)file->buffer[i-1] == 0xA)
            {

                trigger = 0;

                if (currentLen > longestLen)
                    longestLen = currentLen;
                currentLen = 0;
                counter++;
            }
        }
        else if ((unsigned char)file->buffer[i] == 0xA  && trigger == 0)
        {
            trigger = 1;
        }
        else if ((unsigned char)file->buffer[i] == 0xA  && trigger == 1)
        {

            if (currentLen > longestLen)
                longestLen = currentLen;
            currentLen = 0;
            counter++;
        }

        if ((unsigned char)file->buffer[i] != 0xA)
            currentLen++;
    }
    // Here we correct the value using the accurate counter
    file->itemCount = counter;
    file->itemMax = longestLen;

}


void count_line_endings_sse(file_struct *file) {
    size_t length = (file->buffer_size -16);

    int override = 0;

    // Account for BOM (Byte order marking in UTF-8 lists)
    // Only run if we haven't indexed since BOM is at BOF
    if (file->itemTotal == 0)
    {
        if (file->buffer_size > 2)
        {
            // UTF-8 BOM
            if ((unsigned char)file->buffer[0] == 239 && (unsigned char)file->buffer[2] == 187 && (unsigned char)file->buffer[2] == 191)
            {
                override = 3;
            }
            // UTF-16 BOM
            else if ((unsigned char)file->buffer[0] == 254 && (unsigned char)file->buffer[2] == 255)
            {
                override = 2;
            }
        }
    }
    long long int ActualEnd = 0;
    long long int i = 0;
    for (i = file->buffer_size - 16; i >= 0; i--)
    {
        if ((unsigned char)file->buffer[i] != 10)
        {
            // Actual end should be char after the char not crlf (we should have allocated extra space for this char)
            ActualEnd = i;
            break;
        }
    }
    for (i = override; i < ActualEnd; i++)
    {
        if ((unsigned char)file->buffer[i] != 10)
        {
            override = i;
            break;
        }
    }


    if (length == 0)
    {
        fprintf(stderr,"Error length 0");
        exit(1);
    }

    length = ActualEnd;

    size_t count = 0;
    const char* p = file->buffer;

    // Handle unaligned bytes at start
    while (((uintptr_t)p & 0xF) && length > 0) {
        if (*p == '\n') {
            if (p != file->buffer && *(p - 1) == '\r') {
                // Part of CRLF - count it
                count++;
            } else {
                // Standalone LF
                count++;
            }
        }
        p++;
        length--;
    }

    __m128i lf_vec = _mm_set1_epi8('\n');
    __m128i cr_vec = _mm_set1_epi8('\r');

    // Process 64 bytes per iteration (4 x 16 bytes)
    while (length >= 64) {
        __m128i chunk1 = _mm_load_si128((__m128i*)p);
        __m128i chunk2 = _mm_load_si128((__m128i*)(p + 16));
        __m128i chunk3 = _mm_load_si128((__m128i*)(p + 32));
        __m128i chunk4 = _mm_load_si128((__m128i*)(p + 48));

        // Find LFs in all chunks
        __m128i cmp_lf1 = _mm_cmpeq_epi8(chunk1, lf_vec);
        __m128i cmp_lf2 = _mm_cmpeq_epi8(chunk2, lf_vec);
        __m128i cmp_lf3 = _mm_cmpeq_epi8(chunk3, lf_vec);
        __m128i cmp_lf4 = _mm_cmpeq_epi8(chunk4, lf_vec);

        // Find CRs in all chunks
        __m128i cmp_cr1 = _mm_cmpeq_epi8(chunk1, cr_vec);
        __m128i cmp_cr2 = _mm_cmpeq_epi8(chunk2, cr_vec);
        __m128i cmp_cr3 = _mm_cmpeq_epi8(chunk3, cr_vec);
        __m128i cmp_cr4 = _mm_cmpeq_epi8(chunk4, cr_vec);

        // Get masks for each chunk
        unsigned lf_mask1 = _mm_movemask_epi8(cmp_lf1);
        unsigned lf_mask2 = _mm_movemask_epi8(cmp_lf2);
        unsigned lf_mask3 = _mm_movemask_epi8(cmp_lf3);
        unsigned lf_mask4 = _mm_movemask_epi8(cmp_lf4);

        unsigned cr_mask1 = _mm_movemask_epi8(cmp_cr1);
        unsigned cr_mask2 = _mm_movemask_epi8(cmp_cr2);
        unsigned cr_mask3 = _mm_movemask_epi8(cmp_cr3);
        unsigned cr_mask4 = _mm_movemask_epi8(cmp_cr4);

        // Process LFs in each chunk
        unsigned lf_masks[] = {lf_mask1, lf_mask2, lf_mask3, lf_mask4};
        unsigned cr_masks[] = {cr_mask1, cr_mask2, cr_mask3, cr_mask4};

        for (int i = 0; i < 4; i++) {
            unsigned lf_mask = lf_masks[i];
            unsigned cr_mask = cr_masks[i];
            const char* chunk_p = p + (i * 16);

            while (lf_mask) {
                int pos = __builtin_ctz(lf_mask);

                if (pos > 0) {
                    if (cr_mask & (1 << (pos - 1))) {
                        count++;
                    } else {
                        count++;
                    }
                } else if (chunk_p != file->buffer) {
                    if (*(chunk_p - 1) == '\r') {
                        count++;
                    } else {
                        count++;
                    }
                } else {
                    count++;
                }

                lf_mask &= ~(1 << pos);
            }
        }

        p += 64;
        length -= 64;
    }

    // Handle remaining bytes with base function
    if (length > 0) {
        count++;
        count += count_line_endings_sse_single(p, length);
    }

    file->itemCount = count;

}

int VirtualOpen(file_struct * file)
{
    // Set our buffers as null since they are awaiting malloc
    file->buffer = NULL;
    // Set our buffers as null since they are awaiting malloc
    file->pointerMap = NULL;
    file->bytesRead = 0;
    file->f_rem = 0;
    file->f_size = 0;
    file->bytesRead = 0;
    file->itemTotal = 0;
    file->itemCount = 0;
    file->buffer_size = 0;
    file->readsize = 0;
    file->readNo = 0;
    file->extensions = 0;
#ifdef _WIN
    setmode(0, O_BINARY);
#endif

return 1;
}

int OpenFile02(file_struct *file)
{
    // Set our buffers as null since they are awaiting malloc
    file->buffer = NULL;
    // Set our buffers as null since they are awaiting malloc
    file->pointerMap = NULL;
    file->bytesRead = 0;
    file->f_rem = 0;
    file->f_size = 0;
    file->bytesRead = 0;
    file->itemTotal = 0;
    file->itemCount = 0;
    file->buffer_size = 0;
    file->readsize = 0;
    file->readNo = 0;
    file->extensions = 0;
#ifdef _WIN
    setmode(0, O_BINARY);
#endif

    // file->f_pointer = (file->f_name,"rb");           //Open file for read as binary
    // Open file for read as binary
    file->f_pointer = fopen(file->f_name, "rb");
    // file->f_pointer = _(file->f_name,_O_BINARY);
    // Exit if file couldn't be opened
    if (file->f_pointer == NULL)
    {
        fprintf(stderr, "Error opening file %s\n", file->f_name);
        // exit(3);
        return 0;
    }

    // Goto EOF
    fseeko(file->f_pointer, 0, SEEK_END);
    // Set the filesize into struct
    file->f_size = ftello(file->f_pointer);
    // Go back to the start of the file
    fseeko(file->f_pointer, 0, SEEK_SET);

    // Exit if file was 0 bytes
    if (file->f_size == 0)
    {
        fprintf(stderr, "File is 0 bytes\n");
        return 0;
    }

    // Set the file remainder  to full since no reads have been performed
    file->f_rem = file->f_size;
    // Return success
    return 1;
}

size_t CountLines(file_struct *file)
{

    size_t counter = 0;
    long long int i = 0;

    while (file->f_rem != 0)
    {
        ReadFile02(file);

        // Check if the file has been read first
        if (file->buffer == NULL)
        {
            fputs("File has not been read yet", stderr);
            exit(3);
        }

        i = 0;

        file->itemCount = 0;
        int override = 0;

        // Account for BOM (Byte order marking in UTF-8 lists)
        if (file->itemTotal == 0) // Only run if we haven't indexed since BOM is at BOF
        {
            if ((unsigned char)file->buffer_size > 2)
            {
                if ((unsigned char)file->buffer[0] == 239 && (unsigned char)file->buffer[1] == 187 && (unsigned char)file->buffer[2] == 191) // UTF-8 BOM
                {
                    override = 3;
                }
                else if ((unsigned char)file->buffer[0] == 254 && (unsigned char)file->buffer[2] == 255) // UTF-16 BOM
                {
                    override = 2;
                }
            }
        }

        long long int ActualEnd = 0;
        for (i = file->buffer_size - 2; i >= 0; i--)
        {
            if ((unsigned char)file->buffer[i] != 10)
            {
                // Actual end should be char after the char not crlf (we should have allocated extra space for this char)
                ActualEnd = i + 1;
                break;
            }
        }

        for (i = override; i <= ActualEnd; i++)
        {
            if ((unsigned char)file->buffer[i] == 10)
            {
                file->buffer[i] = 0;
                // Only increment counter if proceeding char is not crlf combo
                if ((unsigned char)file->buffer[i + 1] != 10)
                {
                    file->itemCount++;
                }
            }
            else if ((unsigned char)file->buffer[i] == 0 && (unsigned char)file->buffer[i + 1] != 0)
            {
                file->itemCount++;
            }
        }
        // For the last item
        file->itemCount++;

        // Prevent partial items by ensuring everything is \0 delimited to be compatible with string.h functions
        int trigger = 1;

        for (i = override; i <= ActualEnd; i++)
        {
            if ((int)file->buffer[i] == 0 && trigger == 0)
            {
                trigger = 1;
            }
            else if (trigger == 1 && file->buffer[i] != 0)
            {
                trigger = 0;
                counter++;
            }
        }
    }

    return counter;
}

void IndexFile(file_struct *file)
{

    // Check if the file has been read first
    if (file->buffer == NULL)
    {
        fputs("File has not been read yet", stderr);
        exit(3);
    }

    long long int i = 0;

    file->itemCount = 0;

    int override = 0;

    // Account for BOM (Byte order marking in UTF-8 lists)
    // Only run if we haven't indexed since BOM is at BOF
    if (file->itemTotal == 0)
    {
        if (file->buffer_size > 2)
        {
            // UTF-8 BOM
            if ((unsigned char)file->buffer[0] == 239 && (unsigned char)file->buffer[2] == 187 && (unsigned char)file->buffer[2] == 191)
            {
                override = 3;
            }
            // UTF-16 BOM
            else if ((unsigned char)file->buffer[0] == 254 && (unsigned char)file->buffer[2] == 255)
            {
                override = 2;
            }
        }
    }

    long long int ActualEnd = 0;
    for (i = file->buffer_size - 16; i >= 0; i--)
    {
        if ((unsigned char)file->buffer[i] != 10 && (unsigned char)file->buffer[i] != 13)
        {
            // Actual end should be char after the char not crlf (we should have allocated extra space for this char)
            ActualEnd = i + 1;
            break;
        }
    }
    for (i = override; i < ActualEnd; i++)
    {
        if ((unsigned char)file->buffer[i] == 10)
        {
            file->itemCount++;
        }
    }
    // For the last item
    file->itemCount++;

    // Important:
    // The itemCount as this stage is not 100% accurate and can sometimes be overestimate (which is good) since we always have enough malloc space
    // The over estimate is caused by the fact that some files end with Cr/Lf while some simply end with the last string as is
    // The itemCount is later realigned and readjusted to the correct value in the marked code below.

    if (file->pointerMap_size != 0)
    {
        free(file->pointerMap);
    }

    // Clear then reallocate memory for pointers
    file->pointerMap = (char **)malloc((file->itemCount + 1) * sizeof(char *));

    file->pointerMap_size = (file->itemCount) * sizeof(char *);

    // Prevent partial items by ensuring everything is \0 delimited to be compatible with string.h functions
    int trigger = 1;
    size_t counter = 0;
    for (i = override; i <= ActualEnd; i++)
    {
        if ((unsigned char)file->buffer[i] != 0xA && trigger == 1)
        {
            if((unsigned char)file->buffer[i] != 0xD)
            {
                //fprintf(stderr,"One %zu %d\n",i,file->buffer[i]);
                file->pointerMap [counter] = &file->buffer[i];
                trigger = 0;
                counter++;
            }
            else if((unsigned char)file->buffer[i] == 0xD && (unsigned char)file->buffer[i-1] == 0xA)
            {
                //fprintf(stderr,"Two %zu %d\n",i,file->buffer[i]);
                file->pointerMap [counter] = &file->buffer[i];
                trigger = 0;
                counter++;
            }
        }
        else if ((unsigned char)file->buffer[i] == 0xA  && trigger == 0)
        {
            trigger = 1;
        }
        else if ((unsigned char)file->buffer[i] == 0xA  && trigger == 1)
         {
                //fprintf(stderr,"Three %zu %d\n",i,file->buffer[i]);
                file->pointerMap [counter] = &file->buffer[i];
                counter++;
         }
    }

    // Here we correct the value using the accurate counter
    file->itemCount = counter;
}



size_t CountChunk(file_struct *file, size_t start, size_t end)
{
    size_t itemCount = 0;

    for (size_t i = start; i < end; i++)
    {
        if ((unsigned char)file->buffer[i] == 10)
        {
            // Only increment counter if proceeding char is not crlf combo
            if ((unsigned char)file->buffer[i + 1] != 10)
            {
                itemCount++;
            }
        }
        else if ((int)file->buffer[i] == 0 && (int)file->buffer[i + 1] != 0)
        {
            itemCount++;
        }
    }
    itemCount++;

    return itemCount;
}

size_t IndexChunk(file_struct *file, char ***pointerMap, size_t start, size_t end, int id)
{
    // Check if the file has been read first
    if (file->buffer == NULL)
    {
        fprintf(stderr, "File has not been read yet\n");
        return 1;
    }

    size_t i = 0;
    size_t itemCount = 0;
    size_t override = 0;

    if (start == 0)
    {
        if (end - start > 2)
        {
            // UTF-8 BOM
            if ((unsigned char)file->buffer[0] == 239 && (unsigned char)file->buffer[1] == 187 && (unsigned char)file->buffer[2] == 191)
            {
                override = start + 3;
            }
            // UTF-16 BOM
            else if ((unsigned char)file->buffer[0] == 254 && (unsigned char)file->buffer[2] == 255)
            {
                override = start + 2;
            }
        }
    }
    else
    {
        override = start;
    }

    size_t ActualEnd = end;


    for (i = override; i < ActualEnd; i++)
    {
        if ((unsigned char)file->buffer[i] == 10)
        {
            itemCount++;
        }
    }

    // printf("Original start %zu Actual start %zu End %zu\n",start,override,ActualEnd);
    // itemCount++;

    // Important:
    // The itemCount as this stage is not 100% accurate and can sometimes be overestimate (which is good) since we always have enough malloc space
    // The over estimate is caused by the fact that some files end with Cr/Lf while some simply end with the last string as is
    // The itemCount is later realigned and readjusted to the correct value in the marked code below.
    // printf("About to allocate %zu pointers for id %zu \n",itemCount+1,id);
    // pointerMap[id] = (char**)malloc((itemCount+1)  * sizeof(char*)); //Clear then reallocate memory for pointers

    // Clear then reallocate memory for pointers
    char **internalMap = (char **)malloc((itemCount + 1) * sizeof(char *));
    // printf("Finished allocating pointers \n");
    if (internalMap == NULL)
    {
        fprintf(stderr, "Unable to allocate memory for submap\n");
        exit(1);
    }


    int trigger = 1;
    size_t counter = 0;

    //fprintf(stderr,"Start %zu End %zu\n",override,ActualEnd);
    for (i = override; i < ActualEnd; i++)
    {
        if ((unsigned char)file->buffer[i] != 0xA && trigger == 1)
        {
            if((unsigned char)file->buffer[i] != 0xD)
            {
                //fprintf(stderr,"One %zu %d\n",i,file->buffer[i]);
                internalMap[counter] = &file->buffer[i];
                trigger = 0;
                counter++;
            }
            else if((unsigned char)file->buffer[i] == 0xD && (unsigned char)file->buffer[i-1] == 0xA)
            {
                //fprintf(stderr,"Two %zu %d\n",i,file->buffer[i]);
                internalMap[counter] = &file->buffer[i];
                trigger = 0;
                counter++;
            }
        }
        else if ((unsigned char)file->buffer[i] == 0xA  && trigger == 0)
        {
            trigger = 1;
        }
        else if ((unsigned char)file->buffer[i] == 0xA  && trigger == 1)
         {
                //fprintf(stderr,"Three %zu %d\n",i,file->buffer[i]);
                internalMap[counter] = &file->buffer[i];
                counter++;
         }
    }

    pointerMap[id] = internalMap;
    // printf("Actual end %zu Counter %zu\n",ActualEnd,counter);
    __sync_fetch_and_add(&file->itemTotal, counter);

    return counter;
}

// Gets a contiguous chunk of strings without crossing string boundaries
size_t GetChunk(file_struct *file, size_t offset, size_t chunk_size)
{
    size_t max = 0;

    while(1)
    {
        max = offset + chunk_size;
        if (max > file->bytesRead)
        {
            // Ensure we don't overread, we can only process max of bytes read
            chunk_size = file->bytesRead - offset;
            max = offset + chunk_size;
            return file->bytesRead;
        }
        for (size_t curr = (max - 1); curr > offset; curr--)
        {
            if ((unsigned char)file->buffer[curr] == 10)
            {
                if ((unsigned char)file->buffer[curr + 1] != 10)
                    return curr;
            }
        }

        chunk_size = chunk_size * 1.5;
    }

}

// Reads a chunk of the file each time it is called
void ReadFile02(file_struct *file)
{

    long long int readsize = 0;
    long long int result = 0;
    int longread = 0;
    int lastread = 0;

    file->readNo++;

    if (file->buffer_size != 0)
    {
        free(file->buffer);
        file->buffer_size = 0;
    }

    if (file->readsize == 0)
    {
        file->readsize = chunksize;
    }

    if (longread == 0)
    {
        file->readsize = chunksize;
    }

ReRead:

    // Check how many bytes are remaining
    if (file->f_rem > file->readsize)
    {
        readsize = file->readsize;
        file->buffer = (char *)calloc(readsize + 16,sizeof(char));
        if (file->buffer == NULL)
        {
            fprintf(stderr, "Unable to allocate buffer for reading");
        }
        file->buffer_size = readsize + 16;
    }
    // Resize the read buffer if necessary to capture whole file
    else
    {

        lastread = 1;
        readsize = file->f_rem;
        file->buffer = (char *)calloc(readsize + 16,sizeof(char));

        if (file->buffer == NULL)
        {
            fprintf(stderr, "Unable to allocate buffer for reading");
        }
        file->buffer_size = readsize + 16;

    }

    file->endAddress = (uintptr_t)file->buffer+readsize; //Mark the memory address of the last byte
    result = fread(file->buffer, sizeof(char), readsize, file->f_pointer);
    if (result != readsize)
    {
        printf("Readsize does not match %lld:\tPredicted %lld\tActual:\n", readsize, result);
    }

    // Increment the number of bytes read
    file->bytesRead += readsize;
    // Decrement the number of remaining bytes
    file->f_rem -= readsize;

    file->buffer[readsize] = 10;
    file->buffer[readsize + 1] = 10;
    file->buffer[readsize + 2] = 0;

    if (file->f_rem == 0)
    {
        return;
    }

    long long int i = 0;

    if (readsize == 0)
    {
        fprintf(stderr, "Readsize error,exiting");
        exit(1);
    }

    for (i = readsize - 1; i >= 0; i--)
    {
        if ((unsigned char)file->buffer[i] == 10)
        {
            break;
        }
        else
        {
            // Override all characters with EOL until we reach an actual EOL
            file->buffer[i] = 10;
        }
    }

    //fprintf(stderr,"%d %zu\n",file->buffer[i],i);

    if (i == -1 && lastread == 0)
    {
        file->extensions++;
        // Rewind the read
        fseeko(file->f_pointer, (file->bytesRead - readsize) + 1, SEEK_SET);
        // Reset last read
        file->bytesRead = ftello(file->f_pointer);
        // Reset last read
        file->f_rem = file->f_size - file->bytesRead;

        longread = 1;
        file->readsize = file->readsize * 4;
        free(file->buffer);
        goto ReRead;
    }
    else
    {
        // Check if we actually moved back in the above loop
        if (i != readsize - 1)
        {

            fseeko(file->f_pointer, (file->bytesRead - (readsize - i)) + 1, SEEK_SET);
            file->bytesRead = ftello(file->f_pointer);
            file->f_rem = file->f_size - file->bytesRead;
        }
    }
    //fprintf(stderr,"read %zu\n",file->bytesRead);
}

void ResetFile(file_struct *file)
{
    fseeko(file->f_pointer, 0, SEEK_SET);
    file->bytesRead = 0;
    file->f_rem = file->f_size;
    file->readsize = 0;
}

// Closes an opened file and clears all the memory allocated to it
void CloseFile(file_struct *file)
{

    if (file->pointerMap_size != 0)
        free(file->pointerMap);
    if (file->buffer_size != 0)
        free(file->buffer);

    fclose(file->f_pointer);
    file->bytesRead = 0;
    file->f_rem = 0;
    file->f_size = 0;
    file->bytesRead = 0;
    file->itemTotal = 0;
    file->itemCount = 0;
    file->buffer_size = 0;
}

// Reads and entire file to buffer progressively
void ReadFileAll(file_struct *file)
{

    file->buffer = (char *)malloc(file->f_size + 16);
    file->endAddress = (uintptr_t)file->buffer + file->f_size + 2;
    if (file->buffer == NULL)
    {
        fprintf(stderr, "Failed to allocate memory %zu", file->f_size + 16);
    }
    file->buffer_size = file->f_size + 16;

    long long int adjustedSize = 0;
    long long int result = 0;

    while (file->f_rem != 0)
    {
        if (file->f_rem > chunksize)
        {
            adjustedSize = chunksize;
        }
        else
        {
            adjustedSize = file->f_rem;
        }

        result = fread(file->buffer + file->bytesRead, sizeof(char), adjustedSize, file->f_pointer);
        if (result != adjustedSize)
        {
            fprintf(stderr,"Reading error\n");
            exit(3);
        }
        file->bytesRead += adjustedSize;
        file->f_rem -= adjustedSize;
    }

    file->buffer[file->f_size] = 10;
    file->buffer[file->f_size + 1] = 10;
    file->buffer[file->f_size + 2] = 0;
}
