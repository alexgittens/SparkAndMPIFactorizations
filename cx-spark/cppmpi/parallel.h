
#ifndef _PARALLEL_H_

#define _PARALLEL_H_

#include <stdio.h>
#include <pthread.h>

#include "memory.h"

#define CORES_PER_SOCKET 6
#define MAX_THREADS 32
#define ALIGN 16
volatile static int BARRIER_SELECT[MAX_THREADS*ALIGN];
volatile static int BARRIER_1[MAX_THREADS];
volatile static int BARRIER_2[MAX_THREADS];
volatile static int ENABLE_BARRIER_1;
volatile static int ENABLE_BARRIER_2;


class Parallel_Infrastructure
{
    public:

    int nthreads;
    pthread_t *threads;



    Parallel_Infrastructure(void)
    {
        threads = NULL;
        nthreads = 1;
    }

    void Set_nthreads(int _nthreads, Memory *memory)
    {
        nthreads = _nthreads;

        if (nthreads > MAX_THREADS) ERROR_PRINT_STRING("MAX_THREADS is less than nthreads...");
        threads = (pthread_t *)memory->small_malloc( nthreads * sizeof(pthread_t));
    }

    void SetAffinity(int threadid)
    {

//#define AFFINITY

#ifdef AFFINITY
        cpu_set_t mask;
        CPU_ZERO(&mask);

        //int threadid_prime = threadid;
        int threadid_prime = (threadid%CORES_PER_SOCKET) * 2 + (threadid/CORES_PER_SOCKET);

        CPU_SET(threadid_prime, &mask);
        sched_setaffinity(0, sizeof(mask), &mask);
#endif
    }

    void barrier(int threadid)
    {
      if (BARRIER_SELECT[threadid*ALIGN] == 0) {
        BARRIER_SELECT[threadid*ALIGN] = 1;
        if (threadid == 0) {
          for (int i=1; i<nthreads; i++) {
            while(BARRIER_1[i] == 0);
            BARRIER_2[i] = 0;
          }
          ENABLE_BARRIER_2 = 0;
          ENABLE_BARRIER_1 = 1;
        }
        else
        {
          BARRIER_1[threadid] = 1;
          while(ENABLE_BARRIER_1 == 0);
        }
      }
      else {
        BARRIER_SELECT[threadid*ALIGN] = 0;
        if (threadid == 0) {
          for (int i=1; i<nthreads; i++) {
            while(BARRIER_2[i] == 0);
            BARRIER_1[i] = 0;
          }
          ENABLE_BARRIER_1 = 0;
          ENABLE_BARRIER_2 = 1;
        }
        else {
          BARRIER_2[threadid] = 1;
          while(ENABLE_BARRIER_2 == 0);
        }
      }
    }


    void PrintParams(void)
    {
        printf("nthreads = %d\n", nthreads);
    }




};

#endif
