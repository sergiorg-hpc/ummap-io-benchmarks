#ifndef _UTIL_H
#define _UTIL_H

#ifdef __cplusplus
extern "C" {
#endif

typedef struct timespec timespec_t;

/**
 * Enumerate that defines the time unit between two different time intervals.
 */
typedef enum
{
    TSUNIT_NSEC = 1,
    TSUNIT_USEC = 1000,
    TSUNIT_MSEC = 1000000,
    TSUNIT_SEC  = 1000000000
} tsunit_t;

/**
 * Helper method that allows to create a directory given its path.
 */
int createDir(const char *path);

/**
 * Helper method that allows to delete a directory given its path.
 */
int deleteDir(const char *path);

/**
 * Helper method to open a file and preallocate its size, if needed.
 */
int openFile(const char *filename, int flags, int8_t preallocate, size_t size,
             int *fd);

/**
 * Helper method that returns the elapsed time between two time intervals.
 */
double getElapsed(timespec_t start, timespec_t stop, tsunit_t unit);

#ifdef __cplusplus
}
#endif

#endif

