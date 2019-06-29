
BUILDDIR       = ../build
BINDIR         = $(BUILDDIR)/bin
INCDIR   	   = $(BUILDDIR)/include
OBJDIR         = $(BUILDDIR)/obj
UMMIO_BUILDDIR = ../ummap-io/build
UMMIO_SRCDIR   = ../ummap-io/src
UMMIO_INCDIR   = $(UMMIO_BUILDDIR)/include
UMMIO_LIBDIR   = $(UMMIO_BUILDDIR)/lib
UMMIO_OBJDIR   = $(UMMIO_BUILDDIR)/obj
UMMIO_LIBPATH  = $(UMMIO_LIBDIR)/libummapio.a
IOR_DIR        = ../ior
PRK_DIR        = ../prk
UMMIO_LIBPATH  = $(UMMIO_LIBDIR)/libummapio.a
CLIBS          = -lummapio
CFLAGS         = -DVERIFY_OUTPUT=$(or $(VERIFY_OUTPUT),0) -O2 \
                 -I/usr/include/mpi -I$(UMMIO_SRCDIR) -L$(UMMIO_LIBDIR) -I./util

ifeq (,$(findstring Cray,$(shell cc -V 2>&1)))
    CLIBS  += -lmpi -pthread -lrt
    CFLAGS += -Wall -Wno-unused-label -Wno-unused-function -std=c99
endif

ifndef MPICC
    ifeq (,$(shell which mpicc 2>/dev/null))
        $(error No MPI C compiler found. Use "make [...] MPICC=/path/to/mpicc" \
                to provide one)
    else
        MPICC = mpicc
        $(warning Using "mpicc" for compilation (no MPI C compiler provided).)
        $(info **           Use "make [...] MPICC=/path/to/mpicc" to replace \
                            the default.)
    endif
endif

all: setup $(UMMIO_LIBPATH) \
		   $(BINDIR)/mstream.out \
		   $(BINDIR)/pflatency.out

prk: all $(BINDIR)/prk_p2p.out \
		 $(BINDIR)/prk_transpose.out \
		 $(BINDIR)/prk_stencil.out \
		 $(BINDIR)/prk_reduce.out \
		 $(BINDIR)/prk_pic.out

$(BINDIR)/prk_p2p.out:
	@./$(PRK_DIR)/build.sh make
$(BINDIR)/prk_transpose.out:
	@./$(PRK_DIR)/build.sh make
$(BINDIR)/prk_stencil.out:
	@./$(PRK_DIR)/build.sh make
$(BINDIR)/prk_reduce.out:
	@./$(PRK_DIR)/build.sh make
$(BINDIR)/prk_pic.out:
	@./$(PRK_DIR)/build.sh make

ior: all $(BINDIR)/ior.out

$(BINDIR)/ior.out:
	@./$(IOR_DIR)/build.sh make "$(MPICC) -std=gnu99"

$(BINDIR)/mstream.out: $(OBJDIR)/util.o mstream.c
	$(MPICC) $(CFLAGS) mstream.c -o $(BINDIR)/mstream.out $(OBJDIR)/*.o $(CLIBS)

$(BINDIR)/pflatency.out: $(OBJDIR)/util.o pflatency.c
	$(MPICC) $(CFLAGS) pflatency.c -o $(BINDIR)/pflatency.out $(OBJDIR)/*.o \
			 $(CLIBS)

$(OBJDIR)/util.o: util/util.*
	$(MPICC) $(CFLAGS) -c util/util.c -o $(OBJDIR)/util.o

$(UMMIO_LIBPATH):
	@$(MAKE) -C $(UMMIO_SRCDIR)

setup:
	@mkdir -p $(BINDIR) $(INCDIR) $(OBJDIR) 2> /dev/null
	@cp util/util.h $(INCDIR) 2> /dev/null

clean:
	@rm -rf $(BUILDDIR)
clean_all: clean
	@$(MAKE) clean --no-print-directory -C $(UMMIO_SRCDIR)
	@./$(IOR_DIR)/build.sh clean 1> /dev/null
	@./$(PRK_DIR)/build.sh clean 1> /dev/null

rebuild: clean all
rebuild_ior: clean ior
rebuild_prk: clean prk
rebuild_all: clean_all all
rebuild_ior_all: clean_all ior
rebuild_prk_all: clean_all prk

