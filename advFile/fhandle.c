
#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>

void FreeArray(size_t **array, int x)
{
    int i = 0;
    for (i = 0; i < x; i++)
    {
        free(array[i]);
    }
    free(array);
}

typedef struct
{
    double filesize;
    char notation[BUFSIZ];
} filesizes;

void ToSize(void *pointer)
{
    filesizes *ptr;
    ptr = (filesizes *)pointer;

    int divs = 0;
    while (ptr->filesize > 1024)
    {
        ptr->filesize = ptr->filesize / 1024;
        divs++;
    }
    switch (divs)
    {
    case 0:
        strcpy(ptr->notation, "Bytes");
        break;
    case 1:
        strcpy(ptr->notation, "KB");
        break;
    case 2:
        strcpy(ptr->notation, "MB");
        break;
    case 3:
        strcpy(ptr->notation, "GB");
        break;
    }
}

int DirExists(char *dName)
{

    struct stat sb;

    if (stat(dName, &sb) == 0 && S_ISDIR(sb.st_mode))
    {
        return 1;
    }
    else
    {
        return 0;
    }
}
int FileExists(char *fname)
{
    if (access(fname, F_OK) != -1)
    {
        return 1;
    }
    else
    {
        return 0;
    }
}

size_t FileSize(char *fname)
{

    FILE *ret_size;
    ret_size = fopen(fname, "rb");
    size_t fsize = 0;

    fseeko(ret_size, 0, SEEK_END); // Goto EOF
    fsize = ftello(ret_size);      // Set the filesize into struct

    fclose(ret_size);

    return fsize;
}

size_t FileSizeLive(FILE *handle)
{
    return ftello(handle);
}

void Autoname(char *input, char *target, char *suffix)
{
    char temp[BUFSIZ];
    strcpy(temp, input);
    int searchChar = 47;
#ifdef _WIN
    searchChar = 92;
#endif // _WIN

    char NoExt[BUFSIZ];
    char Ext[BUFSIZ];

    int i, j;
    for (i = strlen(temp) - 1; i >= 0; i--)
    {
        if (temp[i] == searchChar)
        {
            break;
        }
    }

    for (j = strlen(temp) - 1; j > i; j--)
    {
        if (temp[j] == '.')
        {
            break;
        }
    }

    if (i != j)
    {
        memcpy(NoExt, input, j);
        NoExt[j] = '\0';
        strcpy(Ext, input + j);
    }
    else
    {
        strcpy(NoExt, input);
        Ext[0] = '\0';
    }

    sprintf(target, "%s_%s%s", NoExt, suffix, Ext);

    if (FileExists(target) == 0)
    {
        return;
    }

    for (i = 2; i < 9999; i++)
    {
        sprintf(target, "%s_%s_%d%s", NoExt, suffix, i, Ext);
        if (FileExists(target) == 0)
            return;
    }
}
