#pragma once

#include <unistd.h>
#include <limits.h>

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

//#define DEBUG_TIMING
// #define DEBUG_CYCLES

#ifdef DEBUG_TIMING

#define LOG_TIME(ts) do { \
    uint64_t ns_total = (uint64_t)(ts).tv_sec * 1000000000ULL + (uint64_t)(ts).tv_nsec; \
    printf("%s = %" PRIu64 " ns\n", #ts, ns_total); \
} while (0)

#define LOG_STRING(x) printf("%s\n", x);

static inline void get_time(struct timespec *t){
	clock_gettime(CLOCK_MONOTONIC, t);
}

static inline void add_time_difference(uint64_t *sum_ns, uint64_t *counter, struct timespec t1, struct timespec t2){
	long seconds = t2.tv_sec - t1.tv_sec;
    long ns = t2.tv_nsec - t1.tv_nsec;

    if (t2.tv_nsec < t1.tv_nsec) {
        --seconds;
        ns += 1000000000;
    }
    long total_ns = seconds * 1000000000L + ns;

    *sum_ns += total_ns;
    (*counter)++;
}

static void save_results(uint64_t *values, uint64_t *counters, char **labels, int length){

    double total_mean = (double)values[0] / (double)counters[0];

    for (int i = 0; i < length; i++){
        uint64_t value = values[i];
        uint64_t counter = counters[i];
        char* label = labels[i];

        double mean = (double)value / (double)counter;

        double percentage = mean / total_mean * 100.0;

        fprintf(stderr, "%s: %f (%.2f%%)\n", label, mean, percentage);
    }
    fprintf(stderr, "\n");
}

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
#elif defined(DEBUG_CYCLES)

#define LOG_TIME(x) printf("%s = %" PRIu64 " \n", #x, x);

#define LOG_STRING(x) printf("%s\n", x);

static inline uint64_t rdtscp(void) {
    unsigned int lo, hi;
    __asm__ volatile ("RDTSCP\n\t" : "=a"(lo), "=d"(hi) : : "%rcx");
    return ((uint64_t)hi << 32) | lo;
}

static inline void get_time(uint64_t *t) {
    *t = rdtscp();
}

static inline void add_time_difference(uint64_t *sum_cycles, uint64_t *counter, uint64_t t1, uint64_t t2) {
    uint64_t diff = t2 - t1;
    *sum_cycles += diff;
    (*counter)++;
}

static void save_results(uint64_t *values, uint64_t *counters, char **labels, int length){

    double total_mean = (double)values[0] / (double)counters[0];

    for (int i = 0; i < length; i++){
        uint64_t value = values[i];
        uint64_t counter = counters[i];
        char* label = labels[i];

        double mean = (double)value / (double)counter;

        double percentage = mean / total_mean * 100.0;

        fprintf(stderr, "%s: %f (%.2f%%)\n", label, mean, percentage);
    }
    fprintf(stderr, "\n");
}

static inline void print_timings(uint64_t t1, uint64_t t2, const char* label) {
    uint64_t total_cycles = t2 - t1;
    printf("TIMING CYCLES %s: %lu cycles\n", label, total_cycles);
}

static inline void print_percentage(uint64_t t1, uint64_t t2, uint64_t start, uint64_t end, const char* label) {
    uint64_t total_cycles = end - start;
    uint64_t part_cycles = t2 - t1;
    double percentage = (double)part_cycles / (double)total_cycles * 100.0;
    printf("PERCENTAGE CYCLES %s: %.2f%%\n", label, percentage);
}

#else
#define LOG_STRING(x) ((void)0)
#define LOG_TIME(x) ((void)0)
#define get_time(x) ((void)0)
#define add_time_difference(a,b,c,d) ((void)0)
#define save_results(a,b,c,d) ((void)0)
#define print_timings(a,b,c) ((void)0)
#define print_percentage(a,b,c,d,e) ((void)0)
#endif

#define PATH_MAX 4096
static const char *lib_get_application_name(void)
{
    static char path[PATH_MAX];
    ssize_t len = readlink("/proc/self/exe", path, sizeof(path) - 1);
    if (len != -1) {
        path[len] = '\0';
        return path;
    }
    return "unknown";
}
