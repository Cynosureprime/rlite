#ifndef FHANDLE_H_INCLUDED
#define FHANDLE_H_INCLUDED

void FreeArray(size_t ** array, int x);
void ToSize(void * pointer);
int FileExists(char * fname);
int DirExists(char * dName);
size_t FileSize(char * fname);
size_t FileSizeLive(FILE * handle);
void Autoname(char * input, char * target, char * suffix);

#endif // FHANDLE_H_INCLUDED

