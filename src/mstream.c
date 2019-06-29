
#include "common.h"
#include "util.h"
#include "ummap.h"
#include <sys/time.h>
#include <mpi.h>
#ifdef MPI_SWIN_ENABLED
// Include the MPI Storage Windows header
#include "mpi_swin_keys.h"
#endif

#define MSTREAM_PARAMS "[size] [seg_size] [csize] [bmark] [impl] [read] " \
                       "[ptype] [dynamic] [folder]"
#define TMP_FOLDER     "tmp"
#define NUM_ITER_INIT  0
#define NUM_ITER       10
#define NUM_ITER_TOTAL (NUM_ITER_INIT + NUM_ITER)
#define PROT_FULL      (PROT_READ       | PROT_WRITE)
#define MMAP_FLAGS     (MAP_SHARED      | MAP_NORESERVE)
#define MMAP_FLAGS_M   (MAP_PRIVATE     | MAP_NORESERVE | MAP_ANONYMOUS)
#define POSIX_FLAGS    (O_CREAT         | O_RDWR)
#define MPIIO_FLAGS    (MPI_MODE_CREATE | MPI_MODE_RDWR)

#define RAND_OFFSET(seed, chunk_size, size) \
    (((off_t)rand_r(&seed) * chunk_size) % size)

enum BenchmarkType
{
    BENCHMARK_SEQUENTIAL = 0, // 0
    BENCHMARK_PADDING,        // 1
    BENCHMARK_PRANDOM,        // 2
    BENCHMARK_MIXED           // 3
};

enum ImplType
{
    IMPL_MEM = 0, // 0
    IMPL_MMAP,    // 1
    IMPL_UMMAP,   // 2
    IMPL_MPI1SM,  // 3
    IMPL_MPI1SS,  // 4
    IMPL_MPIIO    // 5
};

enum PolicyType
{
    UMMAP_PTYPE_FIFO = 0, // 0
    UMMAP_PTYPE_LIFO,     // 1
    UMMAP_PTYPE_PLRU,     // 2
    UMMAP_PTYPE_PRND,     // 3
    UMMAP_PTYPE_WIRO_F,   // 4
    UMMAP_PTYPE_WIRO_L    // 5
};

#ifdef MPI_SWIN_ENABLED
/**
 * Helper method that allows to create an MPI_Info object to enable Storage
 * allocations.
 */
int setStorageInfo(char *filename, size_t seg_size, MPI_Info* info) __CHK_FN__
{
    char str[PATH_MAX];

    CHK(MPI_Info_create(info));
    CHK(MPI_Info_set(*info, MPI_SWIN_ALLOC_TYPE, "storage"));
    // CHK(MPI_Info_set(*info, MPI_SWIN_DEVICEID,   "921"));
    CHK(MPI_Info_set(*info, MPI_SWIN_FILENAME,   filename));
    CHK(MPI_Info_set(*info, MPI_SWIN_OFFSET,     "0"));
    CHK(MPI_Info_set(*info, MPI_SWIN_UNLINK,     "false"));
    CHK(MPI_Info_set(*info, MPI_SWIN_FLUSH_INT,  "921921"));
    CHK(MPI_Info_set(*info, MPI_SWIN_READ_FILE,  "false"));
    CHK(MPI_Info_set(*info, MPI_SWIN_PTYPE,      "fifo")); // ptype is ignored!
    CHK(MPI_Info_set(*info, MPI_SWIN_ORDER,      "mem_first"));
    CHK(MPI_Info_set(*info, MPI_SWIN_FACTOR,     "1.0"));
    
    // Convert the implementation type to set the hint
/*    sprintf(str, "%s", ((impl_type == IMPL_MMAP) ? "mmap" : "ummap"));*/
/*    CHK(MPI_Info_set(*info, MPI_SWIN_IMPL_TYPE, str));*/
    
    // Convert the segment size to set the hint
    sprintf(str, "%zu", seg_size);
    CHK(MPI_Info_set(*info, MPI_SWIN_SEG_SIZE, str));
    
    // CHK(MPI_Info_set(*info, MPI_IO_ACCESS_STYLE,    "write_mostly"));
    // CHK(MPI_Info_set(*info, MPI_IO_FILE_PERM,       "S_IRUSR | S_IWUSR"));
    // CHK(MPI_Info_set(*info, MPI_IO_STRIPING_FACTOR, "8"));
    // CHK(MPI_Info_set(*info, MPI_IO_STRIPING_UNIT,   "8388608"));
    
    return CHK_SUCCESS(CHK_EMPTY_ERROR_FN);
}
#endif

/**
 * Sequential / Random benchmark that stores chunks of a fixed size or
 * separated by a padding. The benchmark combines read / write operations.
 */
int launchBenchmark(int impl_type, char *baseptr, MPI_Win win, MPI_File file,
                    int is_random, int drank, size_t alloc_size, size_t size_b,
                    size_t chunk_size, size_t padding) __CHK_FN__
{
    off_t    offset       = 0;
    int      write_active = TRUE;
    void     *baseptr_tmp = malloc(chunk_size);
    uint32_t seed         = (drank + 1) * 921;
    
    if (is_random)
    {
        // Set the seed and generate the first offset
        rand_r(&seed);
        offset = RAND_OFFSET(seed, chunk_size, alloc_size);
    }
    
    for (off_t offset_b = 0; offset_b < size_b; offset_b += chunk_size)
    {
        if (write_active)
        {
#if VERIFY_OUTPUT
            memset(baseptr_tmp, ((offset_b / chunk_size) + 1), chunk_size);
#endif
            
            switch (impl_type)
            {
                case IMPL_MPI1SM:
                case IMPL_MPI1SS:
                {
                    CHK(MPI_Put(baseptr_tmp, chunk_size, MPI_BYTE, drank,
                                offset, chunk_size, MPI_BYTE, win));
                    CHK(MPI_Win_flush_local(drank, win));
                } break;
                
                case IMPL_MPIIO:
                {
                    CHK(MPI_File_write_at(file, offset, baseptr_tmp, chunk_size,
                                          MPI_BYTE, MPI_STATUS_IGNORE));
                } break;
                
                default: // IMPL_MEM + IMPL_MMAP + IMPL_UMMAP
                    memcpy(&baseptr[offset], baseptr_tmp, chunk_size);
            }
        }
        else
        {
            switch (impl_type)
            {
                case IMPL_MPI1SM:
                case IMPL_MPI1SS:
                {
                    CHK(MPI_Get(baseptr_tmp, chunk_size, MPI_BYTE, drank,
                                offset, chunk_size, MPI_BYTE, win));
                    CHK(MPI_Win_flush_local(drank, win));
                } break;
                
                case IMPL_MPIIO:
                {
                    CHK(MPI_File_read_at(file, offset, baseptr_tmp, chunk_size,
                                         MPI_BYTE, MPI_STATUS_IGNORE));
                } break;
                
                default: // IMPL_MEM + IMPL_MMAP + IMPL_UMMAP
                    memcpy(baseptr_tmp, &baseptr[offset], chunk_size);
            }
        }
        
        offset       = (is_random) ? RAND_OFFSET(seed, chunk_size, alloc_size) :
                                     (offset + padding) % alloc_size;
        write_active = !write_active;
    }
    
    free(baseptr_tmp);
    
    return CHK_SUCCESS(CHK_EMPTY_ERROR_FN);
}

int main (int argc, char *argv[]) __CHK_FN__
{
    // Input parameters for the benchmark
    size_t     alloc_size         = 0;
    size_t     seg_size           = 0;
    size_t     chunk_size         = 0;
    int        benchmark          = BENCHMARK_SEQUENTIAL;
    int        impl_type          = IMPL_MEM;
    int        read_file          = FALSE;
    int        ptype              = UMMAP_PTYPE_FIFO;
    int        is_dynamic         = FALSE;
    
    // Auxiliary variables required for each test
    int        rank               = 0;
    int        num_procs          = 0;
    MPI_Win    win                = MPI_WIN_NULL;
    MPI_Info   info               = MPI_INFO_NULL;
    char       *baseptr           = NULL;
    int        fd                 = -1;
    MPI_File   file               = MPI_FILE_NULL;
    char       filename[PATH_MAX] = { 0 };
    char       tmp_path[PATH_MAX] = { 0 };
    timespec_t start[3]           = { 0 };
    timespec_t stop[3]            = { 0 };
    
    // Check if the number of parameters match the expected
    if (argc < 9 || argc > 10)
    {
        fprintf(stderr, "Error: The number of parameters is incorrect!\n");
        fprintf(stderr, "Use: %s %s\n", argv[0], MSTREAM_PARAMS);
        return -1;
    }
    
    // Initialize MPI and retrieve the rank of the process
    CHKPRINT(MPI_Init(&argc, &argv));
    CHKPRINT(MPI_Comm_rank(MPI_COMM_WORLD, &rank));
    CHKPRINT(MPI_Comm_size(MPI_COMM_WORLD, &num_procs));
    
    // Retrieve the benchmark settings
    sscanf(argv[1], "%zu", &alloc_size);
    sscanf(argv[2], "%zu", &seg_size);
    sscanf(argv[3], "%zu", &chunk_size);
    sscanf(argv[4], "%d",  &benchmark);
    sscanf(argv[5], "%d",  &impl_type);
    sscanf(argv[6], "%d",  &read_file);
    sscanf(argv[7], "%d",  &ptype);
    sscanf(argv[8], "%d",  &is_dynamic);
    
    // Create the temp. folder according to the settings
    sprintf(tmp_path, "%s/%s/%d_%d_%d_%d_%d", ((argc > 9) ? argv[9] : "."),
                 TMP_FOLDER, num_procs, benchmark, impl_type, read_file, ptype);
    
    if (rank == 0)
    {
        CHKPRINT(createDir(tmp_path));
    }
    
    // Force all processes to wait before creating the local folders
    CHKPRINT(MPI_Barrier(MPI_COMM_WORLD));
    
    // Define the path according to the rank of the process
    sprintf(filename, "%s/p%d", tmp_path, rank);
    CHKPRINT(createDir(filename));
    sprintf(filename, "%s/p%d/mstream.tmp", tmp_path, rank);
    
    if (is_dynamic)
    {
        const size_t size = 1073741824 + (alloc_size >> rank);
        alloc_size        = (size > alloc_size) ? alloc_size : size;
    }
    
    // Allocate the corresponding resources
    switch (impl_type)
    {
        case IMPL_MPI1SM:
        case IMPL_MPI1SS:
        {
            #ifdef MPI_SWIN_ENABLED
                // Define the MPI Info object based on the allocation type
                if (impl_type == IMPL_MPI1SS)
                {
                    CHKPRINT(setStorageInfo(filename, seg_size, &info));
                }
            #endif
            
            // Allocate the window with the specified size
            CHKPRINT(MPI_Win_allocate(alloc_size, sizeof(char), info,
                                      MPI_COMM_WORLD, (void**)&baseptr, &win));
            
            // Lock the window in exclusive mode
            CHKPRINT(MPI_Win_lock(MPI_LOCK_EXCLUSIVE, rank, 0, win));
        } break;
        
        case IMPL_MPIIO:
        {
            // Open the target file using MPI I/O
            CHKPRINT(MPI_File_open(MPI_COMM_SELF, filename, MPIIO_FLAGS,
                                   MPI_INFO_NULL, &file));
            CHKPRINT(MPI_File_preallocate(file, alloc_size));
        } break;
        
        default: // IMPL_MEM + IMPL_MMAP + IMPL_UMMAP
        {
            const uint32_t flush_int  = UINT_MAX; // <<<<<<<<<<<<<<<<<
            const int32_t  mmap_flags = (impl_type == IMPL_MEM) ? MMAP_FLAGS_M :
                                                                  MMAP_FLAGS;
            
            if (impl_type != IMPL_MEM)
            {
                CHKPRINT(openFile(filename, POSIX_FLAGS, TRUE, alloc_size,
                                  &fd));
            }
            
            if (impl_type == IMPL_UMMAP)
            {
                CHKPRINT(ummap(alloc_size, seg_size, PROT_FULL, fd, 0,
                               flush_int, read_file, ptype, (void **)&baseptr));
            }
            else
            {
                baseptr = mmap(NULL, alloc_size, PROT_FULL, mmap_flags, fd, 0);
                CHKBPRINT((baseptr == MAP_FAILED), errno);
            }
            
            if (impl_type != IMPL_MEM)
            {
                CHKPRINT(close(fd));
            }
        }
    }
    
    // Force all processes to wait before starting the benchmark
    CHKPRINT(MPI_Barrier(MPI_COMM_WORLD));
    
    // Launch the benchmark
    for (uint32_t iteration = 0; iteration < NUM_ITER_TOTAL; iteration++)
    {
        // Start the timer after the initial iterations
        if (iteration == NUM_ITER_INIT)
        {
            clock_gettime(CLOCK_REALTIME, &start[0]);
            clock_gettime(CLOCK_REALTIME, &start[2]);
            
            if (is_dynamic)
            {
                usleep(rank * 921921);
            }
        }
        
        switch (benchmark)
        {
            case BENCHMARK_SEQUENTIAL:
                CHK(launchBenchmark(impl_type, baseptr, win, file, FALSE, rank,
                                    alloc_size, alloc_size, chunk_size,
                                    chunk_size)); break;
            case BENCHMARK_PADDING:
                CHK(launchBenchmark(impl_type, baseptr, win, file, FALSE, rank,
                                    alloc_size, alloc_size, chunk_size,
                                    (chunk_size << 1))); break;
            case BENCHMARK_PRANDOM:
                CHK(launchBenchmark(impl_type, baseptr, win, file, TRUE, rank,
                                    alloc_size, alloc_size, chunk_size, 0));
                                    break;
            case BENCHMARK_MIXED:
                CHK(launchBenchmark(impl_type, baseptr, win, file, TRUE, rank,
                                    alloc_size, (alloc_size >> 1), chunk_size,
                                    0));
                CHK(launchBenchmark(impl_type, baseptr, win, file, FALSE, rank,
                                    alloc_size, (alloc_size >> 1), chunk_size,
                                    (chunk_size << 1)));
        }
    }
    
    // Synchronize the resources
    clock_gettime(CLOCK_REALTIME, &start[1]);
    switch (impl_type)
    {
        case IMPL_MPI1SM:
        case IMPL_MPI1SS:
        {
            CHKPRINT(MPI_Win_sync(win));
        } break;
        
        case IMPL_MPIIO:
        {
            CHKPRINT(MPI_File_sync(file));
        } break;
        
        case IMPL_UMMAP:
        {
            CHKPRINT(umsync(baseptr, FALSE));
        } break;
        
        default: // IMPL_MEM + IMPL_MMAP
        {
            CHKPRINT(msync(baseptr, alloc_size, MS_SYNC));
        }
    }
    clock_gettime(CLOCK_REALTIME, &stop[1]);
    clock_gettime(CLOCK_REALTIME, &stop[0]);
    
    CHKPRINT(MPI_Barrier(MPI_COMM_WORLD));
    clock_gettime(CLOCK_REALTIME, &stop[2]);
    
    // Print the result (in order)
    for (int drank = 0; drank < num_procs; drank++)
    {
        CHKPRINT(MPI_Barrier(MPI_COMM_WORLD));
        
        if (drank == rank)
        {
            size_t   alloc_size_all   = alloc_size * num_procs;
            size_t   wsize            = alloc_size * NUM_ITER;
            double   elapsed          = getElapsed(start[0], stop[0], TSUNIT_SEC);
            double   elapsed_flush    = getElapsed(start[1], stop[1], TSUNIT_SEC);
            double   elapsed_all      = getElapsed(start[2], stop[2], TSUNIT_SEC);
            double   bandwidth        = wsize / elapsed;
            double   bandwidth_mb     = bandwidth / 1048576.0;
            double   bandwidth_all    = (wsize * num_procs) / elapsed_all;
            double   bandwidth_all_mb = bandwidth_all / 1048576.0;
            uint32_t num_reads        = 0;
            uint32_t num_writes       = 0;
            
            CHKPRINT(umstats(&num_reads, &num_writes));
            
            printf("%d;%d; %zu;%zu;%zu;%zu;%d;%d;%d;%d; %lf;%lf;%lf;%lf;%lf; " \
                   "%d;%d\n", rank, num_procs, alloc_size, alloc_size_all,
                   seg_size, chunk_size, benchmark, impl_type, read_file, ptype,
                   elapsed, elapsed_flush, bandwidth_mb, elapsed_all,
                   bandwidth_all_mb, num_reads, num_writes);
        }
    }
    
    // Release the resources
    switch (impl_type)
    {
        case IMPL_MPI1SM:
        case IMPL_MPI1SS:
        {
            CHKPRINT(MPI_Win_unlock(rank, win));
            CHKPRINT(MPI_Win_free(&win));
        } break;
        
        case IMPL_MPIIO:
        {
            CHKPRINT(MPI_File_close(&file));
        } break;
        
        case IMPL_UMMAP:
        {
            CHKPRINT(umunmap(baseptr, FALSE));
        } break;
        
        default: // IMPL_MEM + IMPL_MMAP
        {
            CHKPRINT(munmap(baseptr, alloc_size));
        }
    }
    
    // Force all processes to wait before finalizing the MPI session
    CHKPRINT(MPI_Barrier(MPI_COMM_WORLD));
    
#if !VERIFY_OUTPUT
    // Delete the temp. folder
    if (rank == 0)
    {
        sprintf(tmp_path, "%s/%s", ((argc > 9) ? argv[9] : "."), TMP_FOLDER);
        CHKPRINT(deleteDir(tmp_path));
    }
#endif
    
    CHKPRINT(MPI_Finalize());
    
    return CHK_SUCCESS(CHK_EMPTY_ERROR_FN);
}

