
#include "common.h"
#include "util.h"

int createDir(const char *path) __CHK_FN__
{
    struct stat st = { 0 };

    // Check if the directory already exists (i.e., ignoring the request)
    if (stat(path, &st) == -1)
    {
        char cmd[PATH_MAX];
        
        // For simplicity, we use the "mkdir" command
        sprintf(cmd, "mkdir -p %s", path);
        CHK(system(cmd));
    }
    
    return CHK_SUCCESS(CHK_EMPTY_ERROR_FN);
}

int deleteDir(const char *path) __CHK_FN__
{
    char cmd[PATH_MAX];
    
    // For simplicity, we use the "rm" command
    sprintf(cmd, "rm -rf %s", path);
    CHK(system(cmd));
    
    return CHK_SUCCESS(CHK_EMPTY_ERROR_FN);
}

int openFile(const char *filename, int flags, int8_t preallocate, size_t size,
             int *fd) __CHK_FN__
{
    struct stat st = { 0 };
    
    *fd = open(filename, flags, (S_IRUSR | S_IWUSR));
    CHKB((*fd < 0 || fstat(*fd, &st)), EIO);
    
    if (preallocate && st.st_size != size)
    {
        CHK(ftruncate(*fd, size));
    }
    
    return CHK_SUCCESS(CHK_EMPTY_ERROR_FN);
}

double getElapsed(timespec_t start, timespec_t stop, tsunit_t unit)
{
    return (double)((stop.tv_sec  - start.tv_sec) * __UINT64_C(1000000000) +
                    (stop.tv_nsec - start.tv_nsec)) / (double)unit;
}

