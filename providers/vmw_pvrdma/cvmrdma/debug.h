#pragma once

#include <stdio.h>
#ifdef NDEBUG
    #define _PRINT_DEBUGS_BOOL false
#else
    #define _PRINT_DEBUGS_BOOL true
#endif

#define DEBUG(format, ...) \
    do { if(_PRINT_DEBUGS_BOOL) fprintf(stderr, format __VA_OPT__(,) __VA_ARGS__); } while(0)
