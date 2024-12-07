#ifndef ADVFILE_H_INCLUDED
#define ADVFILE_H_INCLUDED

typedef struct
{
    char *f_name;      // Filename
    FILE *f_pointer;   // File pointer (read)
    char **pointerMap; // Map to hold the indexes mapping to the items within buffer
    size_t pointerMap_size;
    size_t *charLen;
    size_t f_size;      // Filesize
    size_t f_rem;       // Bytes remaining
    char *buffer;       // Buffer holding the read (malloc)
    size_t buffer_size; // Allocated buffer size
    size_t bytesRead;   // Bytes read
    size_t itemTotal;   // Items counted within buffer
    size_t itemCount;   // Items read each loop
    size_t itemMax;
    size_t readsize;
    size_t readNo;
    size_t extensions;
    uintptr_t endAddress;
} file_struct;

size_t IndexChunk(file_struct *file, char ***pointerMap, size_t start, size_t end, int id);
size_t GetChunk(file_struct *file, size_t offset, size_t chunk_size);
size_t CountChunk(file_struct *file, size_t start, size_t end);

void adjustChunk(int direction);
void writeItem(char *inputstring, FILE *filepointer);
int OpenFile02(file_struct *file);
void IndexFile(file_struct *file);
void ReadFile02(file_struct *file);
void ReadFileAll(file_struct *file);
void CloseFile(file_struct *file);
void IndexAnalysis(file_struct *file, size_t **mAarray);
void ResetFile(file_struct *file);
int VirtualOpen(file_struct *file);
void CountChunkV2(file_struct *file);

size_t BytesAtLine(file_struct *file, size_t line);
size_t CountLines(file_struct *file);
#endif // ADVFILE_H_INCLUDED
