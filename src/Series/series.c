#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <inttypes.h>  // for PRId64 or similar if you want printing macros

#include "series.h"  // The new series header

static char* safeStrdup(const char* src) {
    if (!src) return NULL;
    size_t len = strlen(src) + 1;
    char* copy = (char*)malloc(len);
    if (!copy) return NULL;
    memcpy(copy, src, len);
    return copy;
}

void seriesInit(Series* s, const char* name, ColumnType type) {
    if (!s) return;

    s->name = safeStrdup(name);
    s->type = type;
    daInit(&s->data, 8); // some default capacity
}

void seriesFree(Series* s) {
    if (!s) return;

    // For DF_STRING, we do NOT individually free each string because 
    // DynamicArray code already free()s every pointer if it allocated them.
    daFree(&s->data);

    // Free the series name
    free(s->name);
    s->name = NULL;
}

size_t seriesSize(const Series* s) {
    if (!s) return 0;
    return daSize(&s->data);
}

void seriesAddInt(Series* s, int value) {
    if (!s || s->type != DF_INT) return;
    daPushBack(&s->data, &value, sizeof(int));
}

void seriesAddDouble(Series* s, double value) {
    if (!s || s->type != DF_DOUBLE) return;
    daPushBack(&s->data, &value, sizeof(double));
}

void seriesAddString(Series* s, const char* str) {
    if (!s || s->type != DF_STRING || !str) return;
    size_t len = strlen(str) + 1;
    daPushBack(&s->data, str, len); // store a copy in the dynamic array
}

/* ---------------------------------------------------------------------------
 * Add a DateTime value (64-bit, e.g. milliseconds since Unix epoch)
 * --------------------------------------------------------------------------- */
void seriesAddDateTime(Series* s, long long datetimeMillis) {
    if (!s || s->type != DF_DATETIME) return;
    daPushBack(&s->data, &datetimeMillis, sizeof(long long));
}

/* ---------------------------------------------------------------------------
 * Get a DateTime value (64-bit)
 * --------------------------------------------------------------------------- */
bool seriesGetDateTime(const Series* s, size_t index, long long* outValue) {
    if (!s || s->type != DF_DATETIME || !outValue) return false;
    if (index >= daSize(&s->data)) return false;

    const long long* valPtr = (const long long*)daGet(&s->data, index);
    if (!valPtr) return false;

    *outValue = *valPtr;
    return true;
}
/* --------------------------------------------------------------------------- */

bool seriesGetInt(const Series* s, size_t index, int* outValue) {
    if (!s || s->type != DF_INT || !outValue) return false;
    if (index >= daSize(&s->data)) return false;
    const int* valPtr = (const int*)daGet(&s->data, index);
    if (!valPtr) return false;
    *outValue = *valPtr;
    return true;
}

bool seriesGetDouble(const Series* s, size_t index, double* outValue) {
    if (!s || s->type != DF_DOUBLE || !outValue) return false;
    if (index >= daSize(&s->data)) return false;
    const double* valPtr = (const double*)daGet(&s->data, index);
    if (!valPtr) return false;
    *outValue = *valPtr;
    return true;
}

bool seriesGetString(const Series* s, size_t index, char** outStr) {
    if (!s || s->type != DF_STRING || !outStr) return false;
    if (index >= daSize(&s->data)) return false;
    const char* valPtr = (const char*)daGet(&s->data, index);
    if (!valPtr) return false;

    *outStr = safeStrdup(valPtr);
    return (*outStr != NULL);
}

void seriesPrint(const Series* s) {
    if (!s) return;
    printf("Series \"%s\" (", s->name);
    switch (s->type) {
        case DF_INT:
            printf("int");
            break;
        case DF_DOUBLE:
            printf("double");
            break;
        case DF_STRING:
            printf("string");
            break;
        case DF_DATETIME:
            printf("datetime (milliseconds)");
            break;
    }
    printf("), size = %zu\n", seriesSize(s));

    for (size_t i = 0; i < seriesSize(s); i++) {
        switch (s->type) {
            case DF_INT: {
                int val;
                if (seriesGetInt(s, i, &val)) {
                    printf("  [%zu] %d\n", i, val);
                }
            } break;
            case DF_DOUBLE: {
                double val;
                if (seriesGetDouble(s, i, &val)) {
                    printf("  [%zu] %.6f\n", i, val);
                }
            } break;
            case DF_STRING: {
                char* str = NULL;
                if (seriesGetString(s, i, &str)) {
                    printf("  [%zu] \"%s\"\n", i, str);
                    free(str);
                }
            } break;
            case DF_DATETIME: {
                long long dtVal;
                if (seriesGetDateTime(s, i, &dtVal)) {
                    // Just print the 64-bit value. Optionally, you can 
                    // convert it to a human-readable string using strftime.
                    printf("  [%zu] %lld\n", i, dtVal);
                }
            } break;
        }
    }
}
