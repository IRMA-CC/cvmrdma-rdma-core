#pragma once

#include <stdio.h>
#ifdef NDEBUG
    #define _PRINT_DEBUGS_BOOL 0
#else
    #define _PRINT_DEBUGS_BOOL 1
#endif

#define DEBUG(format, ...) \
    do { if(_PRINT_DEBUGS_BOOL) fprintf(stderr, format __VA_OPT__(,) __VA_ARGS__); } while(0)


#include <time.h>

static inline void cvmrdma_time(char const * label)
{
    struct timespec t;
    int ret = clock_gettime(CLOCK_MONOTONIC, &t);
#ifndef NDEBUG
    if(ret) fprintf(stderr, "WARN: clock_gettime returned %d\n", ret);
#endif
    printf("TIME %s,%ld%09ld\n", label, t.tv_sec, t.tv_nsec);
}

#ifdef NTIMING
    #define _PRINT_TIMINGS 0
#else
    #define _PRINT_TIMINTS 1
#endif

#define TIMING(label) \
    do { if(_PRINT_TIMINTS) cvmrdma_time(label); } while(0)
