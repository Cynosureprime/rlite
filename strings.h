#ifndef STRINGS_H
#define STRINGS_H

#define WriteBufferSize 32*1024*1024

#include <stdio.h>
#include <stdint.h>

typedef struct writeBuffer
{
    size_t bufferSize;
    size_t bufferUsed;
    char *buffer;
    size_t writeCount;
} WBuffer;

void buffer_string2(WBuffer *WStruct, char *string,size_t max);
void buffer_string3(size_t * arr, size_t iLoopIdx, WBuffer *WStruct, char *string,size_t max,long count_limit);
size_t mystrlen3(char *line,size_t bytes,size_t one, size_t two);
size_t mystrlen2(char *line,size_t bytes);
size_t mystrlen(char *line);
void flushBuffer(WBuffer *Write_Buffer, FILE * Fptr, int force);
void initWriteBuffer(WBuffer *Write_Buffer);

#endif
