LDFLAGS     = -mkl
INCLUDES    = -I$(HOME)/CANDMC/include -I$(TAUROOTDIR)/include
LIBS		= -L$(HOME)/CANDMC/lib -L$(TAUROOTDIR)/craycnl/lib/libTAU.so
#DEFS        = -D_POSIX_C_SOURCE=200112L -D__STDC_LIMIT_MACROS -DEDISON -DCPP11 -DLAPACKHASTSQR=1 -DFTN_UNDERSCORE=1

#uncomment below to enable performance profiling
#DEFS       += -DPROFILE -DPMPI
#uncomment below to enable debugging
#DEFS       += -DDEBUG=1

#AR          = xiar

#CXX         = tau_cxx.sh
CXX			= CC
FTN			= ftn
FTNFLAGS	= -nofor_main
CXXFLAGS    = -fast -no-ipo -openmp -restrict -std=c++11 -Wall
CFLAGS	    = -fast -no-ipo -openmp -restrict -Wall
DEPS 		= nmf.h
OBJ			= nmf.o

bench_nmf: bench_nmf.o readHdf5.o nnls.o
	$(CXX) -g -o bench_nmf bench_nmf.o readHdf5.o nnls.o $(INCLUDES) $(LIBS)  -lCANQR -lCANShared -mkl
bench_nmf.o: bench_nmf.cxx bench_nmf.h 
	$(CXX) -g -c bench_nmf.cxx $(CXXFLAGS) $(INCLUDES) $(LIBS) -lCANQR -lCANShared -mkl
#%.o: %.cxx
#	$(CXX) -c $(CXXFLAGS) $(INCLUDES) -lf2c -lm $<
nnls.o: nnls.c
	cc -g -c nnls.c $(CFLAGS)
readHdf5.o: readHdf5.cxx
	$(CXX) -g -c readHdf5.cxx $(CXXFLAGS) -I$(TAUROOTDIR)/include -L$(TAUROOTDIR)/craycnl/lib/libTAU.so -mkl
#%.o: %.cxx
#	$(CXX) -c $(CXXFLAGS) $(INCLUDES) -lf2c -lm $<
clean:
	rm -f *.o bench_nmf precision.mod
	rm -f *.o readHdf5
