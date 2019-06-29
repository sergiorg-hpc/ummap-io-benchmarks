
#include "common.h"
#include "util.h"
#include "ummap.h"
#include <sys/time.h>
#include <mpi.h>

#define PFLATENCY_PARAMS "[size] [impl] [num_alloc] [folder] [seg_size]"
#define TMP_FILE         "pflatency_test.tmp"
#define PROT_FULL        (PROT_READ   | PROT_WRITE)
#define MMAP_FLAGS_M     (MAP_PRIVATE | MAP_NORESERVE | MAP_ANONYMOUS)
#define POSIX_FLAGS      (O_CREAT     | O_RDWR)

enum ImplType
{
    IMPL_MEM = 0, // 0
    IMPL_MMAP,    // 1 << Ignored
    IMPL_UMMAP    // 2
};

int main (int argc, char *argv[]) __CHK_FN__
{
    // Input parameters for the benchmark
    size_t       alloc_size         = 0;
    int          impl_type          = IMPL_MEM;
    int          num_alloc          = 0;
    size_t       seg_size           = sysconf(_SC_PAGESIZE);
    
    // Auxiliary variables required for the test
    const size_t padding            = sysconf(_SC_PAGESIZE); // Fixed
    size_t       alloc_size_s       = 0;
    int          rank               = 0;
    int          num_procs          = 0;
    char         **baseptr          = NULL;
    int          fd                 = -1;
    char         filename[PATH_MAX] = { 0 };
    timespec_t   start              = { 0 };
    timespec_t   stop               = { 0 };
    
    // Check if the number of parameters match the expected
    if (argc < 5 || argc > 6)
    {
        fprintf(stderr, "Error: The number of parameters is incorrect!\n");
        fprintf(stderr, "Use: %s %s\n", argv[0], PFLATENCY_PARAMS);
        return -1;
    }
    
    // Initialize MPI and retrieve the rank of the process
    CHKPRINT(MPI_Init(&argc, &argv));
    CHKPRINT(MPI_Comm_rank(MPI_COMM_WORLD, &rank));
    CHKPRINT(MPI_Comm_size(MPI_COMM_WORLD, &num_procs));
    
    // Retrieve the benchmark settings
    sscanf(argv[1], "%zu", &alloc_size);
    sscanf(argv[2], "%d",  &impl_type);
    sscanf(argv[3], "%d",  &num_alloc);
    
    if (argc == 6)
    {
        sscanf(argv[5], "%zu", &seg_size);
    }
    
    // Create the temp. folder according to the settings
    if (rank == 0 && impl_type == IMPL_UMMAP)
    {
        CHKPRINT(createDir(argv[4]));
    }
    
    // Force all processes to wait before allocating
    CHKPRINT(MPI_Barrier(MPI_COMM_WORLD));
    
    // Allocate the corresponding resources
    baseptr      = (char **)malloc(sizeof(char *) * num_alloc);
    alloc_size_s = (alloc_size / (size_t)num_alloc);
    
    for (int alloc = 0; alloc < num_alloc; alloc++)
    {
        if (impl_type == IMPL_UMMAP)
        {
            sprintf(filename, "%s/%s", argv[4], TMP_FILE);
            CHKPRINT(openFile(filename, POSIX_FLAGS, FALSE, alloc_size_s, &fd));
            CHKPRINT(ummap(alloc_size_s, seg_size, PROT_FULL, fd, 0, UINT_MAX,
                           FALSE, 0, (void **)&baseptr[alloc]));
            CHKPRINT(close(fd));
        }
        else
        {
            baseptr[alloc] = mmap(NULL, alloc_size_s, PROT_FULL, MMAP_FLAGS_M,
                                  fd, 0);
            CHKBPRINT((baseptr[alloc] == MAP_FAILED), errno);
        }
    }
    
    // Force all processes to wait before starting the test
    CHKPRINT(MPI_Barrier(MPI_COMM_WORLD));
    
    // Launch the page-fault test
    clock_gettime(CLOCK_REALTIME, &start);
    for (off_t offset = 0; offset < alloc_size_s; offset += padding)
    {
        for (int alloc = 0; alloc < num_alloc; alloc++)
        {
            baseptr[alloc][offset] = 21;
        }
    }
    clock_gettime(CLOCK_REALTIME, &stop);
    
    // Print the result (in order)
    for (int drank = 0; drank < num_procs; drank++)
    {
        CHKPRINT(MPI_Barrier(MPI_COMM_WORLD));
        
        if (drank == rank)
        {
            double num_pf  = (double)(alloc_size / padding);
            double elapsed = getElapsed(start, stop, TSUNIT_NSEC) / num_pf;
            
            printf("%d;%d; %zu;%zu;%d;%d; %lf\n", rank, num_procs, alloc_size,
                   seg_size, impl_type, num_alloc, elapsed);
        }
    }
    
    // Release the resources
    for (int alloc = 0; alloc < num_alloc; alloc++)
    {
        if (impl_type == IMPL_UMMAP)
        {
            CHKPRINT(umunmap(baseptr[alloc], FALSE));
        }
        else
        {
            CHKPRINT(munmap(baseptr[alloc], alloc_size_s));
        }
    }
    
    free(baseptr);
    
    // Force all processes to wait before finalizing the MPI session
    CHKPRINT(MPI_Barrier(MPI_COMM_WORLD));
    
#if !VERIFY_OUTPUT
    // Delete the temp. folder containing the file
    if (rank == 0 && impl_type == IMPL_UMMAP)
    {
        CHKPRINT(deleteDir(argv[4]));
    }
#endif
    
    // Finalize the MPI session
    CHKPRINT(MPI_Finalize());
    
    return CHK_SUCCESS(CHK_EMPTY_ERROR_FN);
}

