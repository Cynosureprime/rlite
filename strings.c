#include "strings.h"
#include <string.h>
#include <stdlib.h>

void initWriteBuffer(WBuffer *Write_Buffer)
{
    Write_Buffer->bufferSize = WriteBufferSize + 1;
    Write_Buffer->bufferUsed = 0;
    Write_Buffer->writeCount = 0;

    Write_Buffer->buffer = (char *)malloc(WriteBufferSize + 1);
    if (Write_Buffer->buffer ==NULL)
    {
        fprintf(stderr,"Error allocating write buffer");
        exit(0);
    }
}

void flushBuffer(WBuffer *Write_Buffer, FILE * Fptr, int force)
{
    if (Write_Buffer->bufferUsed > WriteBufferSize) // Write if greater than buffer size (bufferused can grow greater within buffer_string)
    {
        fwrite(Write_Buffer->buffer, 1, Write_Buffer->bufferUsed, Fptr); // Fwrite does atomic ops on windows/posix
        Write_Buffer->bufferSize = WriteBufferSize + 1;
        Write_Buffer->bufferUsed = 0;
        Write_Buffer->writeCount = 0;
    }

    if (force){
        if (Write_Buffer->bufferUsed > 0) // Flush the buffer
        {
            fwrite(Write_Buffer->buffer, 1, Write_Buffer->bufferUsed, Fptr); // Fwrite does atomic ops on windows/posix
            Write_Buffer->bufferSize = WriteBufferSize + 1;
            Write_Buffer->bufferUsed = 0;
            Write_Buffer->writeCount = 0;
        }
    }
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


void myputs(char *line)
{
    char buffer[BUFSIZ];
    fprintf(stderr,"string length is %zu\n",mystrlen2(line,100000000));
    memcpy(buffer,line,mystrlen2(line,100000000));
    buffer[mystrlen(line)] = 0;
    fprintf(stderr,"%s\n",buffer);
}


//Used for debugging
void myputs2(char *line,size_t bytes)
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
void buffer_string3(size_t * arr, size_t iLoopIdx, WBuffer *WStruct, char *string,size_t max,long count_limit)
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
