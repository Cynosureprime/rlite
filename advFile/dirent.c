#include "dirent.h"
#include <stdio.h>
#include <stdlib.h>

static void list_directory(
    const char *dirname)
{
    DIR *dir;
    struct dirent *ent;

    /* Open directory stream */
    dir = opendir(dirname);
    if (dir != NULL)
    {

        /* Print all files and directories within the directory */
        while ((ent = readdir(dir)) != NULL)
        {
            switch (ent->d_type)
            {
            case DT_REG:
                printf("%s\n", ent->d_name);
                break;

            case DT_DIR:
                printf("%s/\n", ent->d_name);
                break;

            case DT_LNK:
                printf("%s@\n", ent->d_name);
                break;

            default:
                printf("%s*\n", ent->d_name);
            }
        }

        closedir(dir);
    }
    else
    {
        /* Could not open directory */
        printf("Cannot open directory %s\n", dirname);
        exit(EXIT_FAILURE);
    }
}
