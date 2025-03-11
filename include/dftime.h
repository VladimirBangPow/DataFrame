#ifndef TIME_H
#define TIME_H
#include <stddef.h>   // for size_t
#include <stdbool.h>  // for bool   

long long parseDatetimeToMicroseconds(const char* dateStr, const char* format);
bool microsecondsToString(long long micros, char* outBuf, size_t bufSize, const char* format);

#endif // TIME_H
