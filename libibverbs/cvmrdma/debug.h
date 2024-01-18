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

#define NTIMING

#ifdef NTIMING
    #define _PRINT_TIMINGS 0
#else
    #define _PRINT_TIMINGS 1
#endif

#define TIMING(fmt, ...) \
    do { if(_PRINT_TIMINGS) { \
    static size_t count = 0; \
    struct timespec t; \
    clock_gettime(CLOCK_MONOTONIC, &t); \
    printf("TIME " fmt ",%zu,%ld%09ld\n", __VA_ARGS__  __VA_OPT__(,) count++, t.tv_sec, t.tv_nsec); \
    } \
  } while(0)
