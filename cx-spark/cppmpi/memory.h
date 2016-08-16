
#ifndef _MEMORY_H_

#define _MEMORY_H_

#include <stdio.h>
#include <stdlib.h>

class Memory
{
    public:

    size_t small_malloced_memory;
    size_t large_malloced_memory;

    Memory(void)
    {
        small_malloced_memory = 0;
        large_malloced_memory = 0;
    }

    void *small_malloc(size_t sz)
    {
        //printf("Calling Malloc\n");
        small_malloced_memory += sz;
        return malloc(sz);
    }

    void small_free(void *X, size_t sz)
    {
        small_malloced_memory -= sz;
        free(X);
    }

    void PrintParams(void)
    {
        printf("Total Small Malloced Memory = %lld bytes (%.2lf MB)\n", (long long int)(small_malloced_memory), small_malloced_memory/1000.0/1000.0);
    }

    void *aligned_small_malloc(size_t sz)
    {
        //return small_malloc(sz);
        small_malloced_memory += sz;
        size_t sz_plus_64 = (sz + 64);
        unsigned char *X  = (unsigned char *)malloc(sz_plus_64);

        unsigned long long int Y = (unsigned long long int)(X);

        while (Y % 64)
        {
            X++;
            Y = (unsigned long long int)(X);
        }

        return ((void *)(X));
    }


};

#endif
