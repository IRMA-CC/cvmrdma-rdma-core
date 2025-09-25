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


static inline void print_timings(struct timespec t1, struct timespec t2, const char* label) {
    long seconds = t2.tv_sec - t1.tv_sec;
    long ns = t2.tv_nsec - t1.tv_nsec;

    if (t2.tv_nsec < t1.tv_nsec) {
        --seconds;
        ns += 1000000000;
    }
    long total_ns = seconds * 1000000000L + ns;
    printf("TIMING %s: %ld nanoseconds\n", label, total_ns);
}

static inline void print_percentage(struct timespec t1, struct timespec t2, struct timespec start, struct timespec end, const char* label) {
    long total_ns = (end.tv_sec - start.tv_sec) * 1000000000L + (end.tv_nsec - start.tv_nsec);
    long part_ns = (t2.tv_sec - t1.tv_sec) * 1000000000L + (t2.tv_nsec - t1.tv_nsec);
    double percentage = (double)part_ns / (double)total_ns * 100.0;
    printf("PERCENTAGE %s: %.2f%%\n", label, percentage);
}
