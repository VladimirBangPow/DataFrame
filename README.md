# DataFrame::Core


# Core::void Dataframe_Create(DataFrame* df)
![Create](diagrams/Create.png "Create")

## Usage:
```c
    DataFrame df;
    DataFrame_Create(&df);
```


# Core::bool df.addSeries(DataFrame* df, const Series* s)

![AddSeries](diagrams/addSeries.png "AddSeries")
## Usage:
```c
    DataFrame df;
    DataFrame_Create(&df);

    // 1) Build some sample data
    int intVals[] = { 10, 20, 30, 40 };
    double doubleVals[] = { 1.5, 2.5, 3.25, 4.75 };
    const char* stringVals[] = { "apple", "banana", "cherry", "date" };
    long long datetimeVals[] = { 1677612345LL, 1677612400LL, 1677612500LL, 1677613000LL }; 
    // E.g., some arbitrary epoch seconds

    Series sInt = buildIntSeries("IntCol", intVals, 4);
    Series sDouble = buildDoubleSeries("DoubleCol", doubleVals, 4);
    Series sString = buildStringSeries("StrCol", stringVals, 4);
    Series sDatetime = buildDatetimeSeries("TimeCol", datetimeVals, 4);

    bool ok = df.addSeries(&df, &sInt);
    ok = df.addSeries(&df, &sDouble);
    ok = df.addSeries(&df, &sString);
    ok = df.addSeries(&df, &sDatetime);

    // We can free the local series copies now
    seriesFree(&sInt);
    seriesFree(&sDouble);
    seriesFree(&sString);
    seriesFree(&sDatetime);
```


# Core::const Series* df.getSeries(const DataFrame* df, size_t colIndex)
![GetSeries](diagrams/GetSeries.png "GetSeries")

## Usage:
```c
    DataFrame df;
    DataFrame_Create(&df);

    // 1) Build some sample data
    int intVals[] = { 10, 20, 30, 40 };

    Series sInt = buildIntSeries("IntCol", intVals, 4);

    bool ok = df.addSeries(&df, &sInt);

    // We can free the local series copies now
    seriesFree(&sInt);

    const Series* col1 = df.getSeries(&df, 0);

```

# Core::size_t df.numColumns(const DataFrame* df)
![NColumns](diagrams/NColumns.png "NColumns")

## Usage:
```c
    DataFrame df;
    DataFrame_Create(&df);

    // 1) Build some sample data
    int intVals[] = { 10, 20, 30, 40 };

    Series sInt = buildIntSeries("IntCol", intVals, 4);

    bool ok = df.addSeries(&df, &sInt);

    seriesFree(&sInt);

    assert(df.numColumns(&df) == 1);

```

# Core::bool df.addRow(DataFrame* df, const void** rowData)
![AddRow](diagrams/AddRow.png "AddRow")

## Usage:
```c
    DataFrame df;
    DataFrame_Create(&df);

    // 1) Build some sample data
    int intVals[] = { 10, 20, 30, 40 };

    Series sInt = buildIntSeries("IntCol", intVals, 4);

    bool ok = df.addSeries(&df, &sInt);

    seriesFree(&sInt);

    int newValA = 50;
    const void* rowData[] = { &newValA };

    ok = df.addRow(&df, rowData);

```

# Core::bool df.getRow(DataFrame* df, size_t rowIndex, void** outRow)
![GetRow](diagrams/GetRow.png "GetRow")
## Usage:
```c
    DataFrame df;
    DataFrame_Create(&df);

    // 1) Build some sample data
    int intVals[] = { 10, 20, 30, 40 };

    Series sInt = buildIntSeries("IntCol", intVals, 4);

    bool ok = df.addSeries(&df, &sInt);

    seriesFree(&sInt);

    void** rowData = NULL;
    ok = df.getRow(&df, 2, &rowData);
    int* pInt = (int*)rowData[0];
    assert(pInt && *pInt==30);

```

# Core::size_t df.numRows(const DataFrame *df)
![NRows](diagrams/NRows.png "NRows")
## Usage:
```c
    DataFrame df;
    DataFrame_Create(&df);

    // 1) Build some sample data
    int intVals[] = { 10, 20, 30, 40 };

    Series sInt = buildIntSeries("IntCol", intVals, 4);

    bool ok = df.addSeries(&df, &sInt);

    seriesFree(&sInt);

    assert(df.numRows(&df) == 4);

```




# DataFrame::Date

# Date::bool convertToDatetime(DataFrame* df, size_t dateColIndex, const char* formatType)

![convertToDatetime](diagrams/convertToDatetime.png "convertToDatetime")


| **Original Column Type** | **Example Cell Value**  | **`formatType`**             | **Result in DF_DATETIME (milliseconds)**                                                   |
|--------------------------|-------------------------|------------------------------|--------------------------------------------------------------------------------------------|
| **DF_STRING**            | `"2023-03-15 12:34:56"` | `"%Y-%m-%d %H:%M:%S"`        | Parse string → epoch seconds (UTC) via `strptime+timegm` ⇒ multiply by 1000. For example: <br/> `1678882496 * 1000 = 1678882496000` ms. |
| **DF_STRING**            | `"20230315"`           | `"YYYYMMDD"`                 | Interpreted as YYYY=2023, MM=03, DD=15, midnight UTC. For example: <br/> `1678838400` seconds → `1678838400000` ms. |
| **DF_STRING**            | `"1678882496000"`      | `"unix_millis"`             | Interpreted as **epoch milliseconds** => `1678882496000 / 1000 = 1678882496` seconds. <br/> Then re-multiplied by 1000 => stays `1678882496000` ms. |
| **DF_STRING**            | `"1678882496"`         | `"unix_seconds"`            | Interpreted as epoch seconds => `1678882496`. <br/> Convert to ms => `1678882496000`.       |
| **DF_INT**               | `1678882496`           | (any) e.g. `"unix_seconds"`  | The integer is converted to a string `"1678882496"`. Then parsed as above. If `"unix_seconds"`, we end up with `1678882496000` ms. |
| **DF_DOUBLE**            | `1.678882496e9`        | `"unix_seconds"`            | The double is `snprintf`’d to e.g. `"1678882496"` → parse as seconds => re-multiplied => `1678882496000` ms. |
| **DF_STRING**            | `"invalid date"`       | `"%Y-%m-%d %H:%M:%S"`        | Fails parse => final epoch = 0 ms.                                                         |
| **DF_STRING**            | `"2023-03-15 12:34:56"` | `"unrecognized_format"`      | No match => final epoch = 0 ms.                                                            |
| **DF_DATETIME**          | *Any input*            | (function does nothing)      | Actually your code overwrites with 0 ms if you attempt to parse DF_DATETIME. Typically no change. |


## Usage:
```c
    DataFrame df;
    DataFrame_Create(&df);

    // Build a DF_STRING column of date/time
    const char* dates[] = {
        "2023-03-15 12:34:56",
        "2023-03-16 00:00:00",
        "invalid date",
        "2023-03-17 23:59:59"
    };
    Series s;
    seriesInit(&s, "TestDates", DF_STRING);
    for (int i=0; i<4; i++) {
        seriesAddString(&s, dates[i]);
    }

    bool ok = df.addSeries(&df, &s);
    seriesFree(&s); // free local copy

    ok = df.convertToDatetime(&df, 0, "%Y-%m-%d %H:%M:%S");

    const Series* converted = df.getSeries(&df, 0);
    assert(converted != NULL);
    assert(converted->type == DF_DATETIME);

    // For row 2 "invalid date", we expect epoch=0
    long long val=0;
    bool got = seriesGetDateTime(converted, 2, &val);
    assert(got && val == 0);

    DataFrame_Destroy(&df);

```


# Date::bool datetimeToString(DataFrame* df, size_t dateColIndex, const char* outFormat)
![datetimeToString](diagrams/datetimeToString.png "datetimeToString")


| **DF_DATETIME Cell** (msVal)     | **Seconds** (msVal/1000) | **Remainder** (msVal % 1000) | **Output (assuming outFormat=\"%Y-%m-%d %H:%M:%S\")**                                                      | **Notes**                                                                                                        |
|----------------------------------|--------------------------|------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------|
| `0`                              | `0`                      | `0`                          | `"1970-01-01 00:00:00"`                                                                                                                           | No fractional appended since remainder = 0.                                                                     |
| `1678882496000`                  | `1678882496`            | `0`                          | `"2023-03-15 12:34:56"`                                                                                                                           | `gmtime(1678882496)` => March 15, 2023 12:34:56 UTC. No fractional.                                             |
| `1678882496123`                  | `1678882496`            | `123`                        | `"2023-03-15 12:34:56.123"`                                                                                                                       | Same date/time as above, plus `.123` appended because remainder=123.                                            |
| `9999999999999999999` (overflow)| (overflow if too large)  | (overflow)                   | `""` (empty)                                                                                                                                        | If `gmtime` fails due to out-of-range `time_t`, we store an empty string.                                        |
| *Any valid msVal but `gmtime` fails internally* | *N/A*            | *N/A*                      | `""`                                                                                                                                                | If `gmtime` returns `NULL`, we store an empty string.                                                            |

## Usage:
```c
    DataFrame df;
    DataFrame_Create(&df);

    // Make DF_DATETIME column with some known epochs (UTC)
    // e.g. 1678871696 => "2023-03-15 12:34:56" if truly UTC
    long long epochs[] = {
        1678871696LL,
        1678924800LL,
        0LL,
        1679003999LL
    };
    Series sd;
    seriesInit(&sd, "Epochs", DF_DATETIME);
    for (int i=0; i<4; i++) {
        seriesAddDateTime(&sd, epochs[i]);
    }
    bool ok = df.addSeries(&df, &sd);
    seriesFree(&sd);
    assert(ok);

    // Convert => DF_STRING
    ok = df.datetimeToString(&df, 0, "%Y-%m-%d %H:%M:%S");
    assert(ok);

    // Check results
    const Series* s2 = df.getSeries(&df, 0);
    assert(s2 && s2->type == DF_STRING);
    char* strVal = NULL;
    bool got = seriesGetString(s2, 2, &strVal);
    assert(got && strVal);
    // row 2 => "1970-01-01 00:00:00" presumably
    assert(strlen(strVal) > 0);
    free(strVal);

    DataFrame_Destroy(&df);
```



# Date:bool datetimeAdd(DataFrame* df, size_t dateColIndex, int daysToAdd)

![datetimeAdd](diagrams/datetimeAdd.png "datetimeAdd")


| **Row** | **Original Value** (`msVal`)                                               | **msToAdd** = `86400000` (1 day)        | **Final Value** = `msVal + msToAdd`                                              | **Notes**                                                                                                    |
|---------|---------------------------------------------------------------------------|-----------------------------------------|----------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------|
| Row 0   | `baseMs = 1678871696 * 1000` = `1678871696000`                            | `86400000`                              | `1678871696000 + 86400000 = 1678958096000`                                       | This represents shifting the initial date/time by exactly one day in milliseconds.                             |
| Row 1   | `baseMs + (1 * 3600000) = 1678871696000 + 3600000 = 1678875296000`        | `86400000`                              | `1678875296000 + 86400000 = 1678961696000`                                       | An hour later than row 0 originally; still ends up exactly 1 day further in total.                             |
| Row 2   | `baseMs + (2 * 3600000) = 1678871696000 + 7200000 = 1678878896000`        | `86400000`                              | `1678878896000 + 86400000 = 1678965296000`                                       | Two hours later than row 0; also gains 86400000 ms.                                                           |
| *Invalid cell* | *Fails to read msVal (e.g. out-of-range?)*                        | *N/A*                                   | *No change or possibly set to 0 if your code clamps negative.*                    | If `seriesGetDateTime` fails, your code might skip or store 0. Depends on the exact logic in your implementation. |




## Usage:

```c
    DataFrame df;
    DataFrame_Create(&df);

    // Build a DF_DATETIME column in milliseconds
    Series sdt;
    seriesInit(&sdt, "Times", DF_DATETIME);

    // For example, base epoch ~ 2023-03-15 12:34:56 UTC
    // Convert to ms by multiplying by 1000
    long long baseMs = 1678871696LL * 1000LL;

    // Add three rows, each 1 hour apart => 3600000 ms
    for (int i = 0; i < 3; i++) {
        seriesAddDateTime(&sdt, baseMs + (i * 3600000LL));
    }
    bool ok = df.addSeries(&df, &sdt);
    seriesFree(&sdt);
    assert(ok);

    // We want to add 1 day => 86,400 seconds => 86,400,000 ms
    // (Assuming your "df.datetimeAdd" now expects ms to add)
    long long oneDayMs = 86400000LL;
    ok = df.datetimeAdd(&df, 0, oneDayMs);
    assert(ok);

    // Check the first row's new value
    const Series* s2 = df.getSeries(&df, 0);
    long long val = 0;
    bool got = seriesGetDateTime(s2, 0, &val);

    // Expect baseMs + 86,400,000
    long long expected = baseMs + oneDayMs;
    assert(got && val == expected);

    DataFrame_Destroy(&df);
```

# Date::DataFrame datetimeDiff(const DataFrame* df, size_t col1Index, size_t col2Index, const char* newColName)

![datetimeDiff](diagrams/datetimeDiff.png "datetimeDiff")


| **Row** | **Column1 (msVal)** | **Column2 (msVal)** | **Operation**               | **Result in DF_INT** (ms) | **Notes**                                                                                     |
|---------|---------------------|---------------------|-----------------------------|---------------------------|-----------------------------------------------------------------------------------------------|
| 0       | 1,000,000          | 2,000,000          | (2,000,000 - 1,000,000)     | 1,000,000                | If both valid, returns the millisecond difference.                                           |
| 1       | 5,000,000          | 6,000,000          | (6,000,000 - 5,000,000)     | 1,000,000                | Also valid => 1 million ms difference.                                                       |
| 2       | 0                  | 0                  | (0 - 0)                     | 0                         | Same timestamps => difference = 0 ms.                                                        |
| 3       | 10,000,000         | 5,000,000          | (5,000,000 - 10,000,000)    | -5,000,000               | If col2 < col1, result can be negative.                                                      |
| 4       | *Invalid or missing*| 2,000,000          | *cannot read first value*   | 0                         | If reading ms fails for a row, the function stores 0 by default.                             |
| 5       | 99999999999999     | 99999999999999 + 1 | (~1×10^14 ms difference)     | Might overflow as DF_INT  | If difference > 2^31-1 or < -2^31, the stored int may overflow. Consider using 64-bit field. |
| 6       | 1678882496000      | 1678882896000       | (1678882896000 - 1678882496000)| 400,000                | e.g. 400 sec difference in ms if your timestamps are ~2023-03-15 plus some offset.            |

## Usage:
```c
    DataFrame df;
    DataFrame_Create(&df);

    // We'll have 2 DF_DATETIME columns: Start, End
    // Each storing epoch in milliseconds.
    Series sStart, sEnd;
    seriesInit(&sStart, "Start", DF_DATETIME);
    seriesInit(&sEnd, "End", DF_DATETIME);

    // row 0 => start=1,000,000 ms, end=2,000,000 ms => diff=1,000,000 ms
    // row 1 => start=5,000,000 ms, end=6,000,000 ms => diff=1,000,000 ms
    // row 2 => start=0, end=0 => diff=0 ms
    long long starts[] = {1000000LL, 5000000LL, 0LL};
    long long ends[]   = {2000000LL, 6000000LL, 0LL};

    for (int i = 0; i < 3; i++) {
        seriesAddDateTime(&sStart, starts[i]);
        seriesAddDateTime(&sEnd,   ends[i]);
    }
    bool ok = df.addSeries(&df, &sStart);
    assert(ok);
    ok = df.addSeries(&df, &sEnd);
    assert(ok);

    seriesFree(&sStart);
    seriesFree(&sEnd);

    // Diff => new DF with one column named "Diff"
    // Now returns difference in ms
    DataFrame diffDF = df.datetimeDiff(&df, 0, 1, "Diff");
    assert(diffDF.numColumns(&diffDF) == 1);

    const Series* diffS = diffDF.getSeries(&diffDF, 0);
    assert(diffS && diffS->type == DF_INT);

    // Check the results
    int check = 0;
    bool gotVal = seriesGetInt(diffS, 0, &check);
    // row0 => 2,000,000 - 1,000,000 => 1,000,000 ms
    assert(gotVal && check == 1000000);

    seriesGetInt(diffS, 1, &check);
    // row1 => 6,000,000 - 5,000,000 => 1,000,000 ms
    assert(check == 1000000);

    seriesGetInt(diffS, 2, &check);
    // row2 => 0 - 0 => 0
    assert(check == 0);

    DataFrame_Destroy(&diffDF);
    DataFrame_Destroy(&df);

```
# Date::DataFrame datetimeFilter(const DataFrame* df, size_t dateColIndex, long long startMs, long long endMs)

![datetimeFilter](diagrams/datetimeFilter.png "datetimeFilter")

| **Column Type** | **Row Value** (`msVal`) | **`startMs`**         | **`endMs`**           | **Filter Condition**                  | **Included in Result?**                         |
|-----------------|-------------------------|------------------------|-----------------------|---------------------------------------|-------------------------------------------------|
| `DF_DATETIME`   | `0`                    | `0`                    | `9999999999`         | `0 >= 0 && 0 <= 9999999999`           | **Yes** (0 is in [0..9999999999])              |
| `DF_DATETIME`   | `2000`                 | `1000`                 | `3000`               | `2000 >= 1000 && 2000 <= 3000`        | **Yes** (2000 is in [1000..3000])              |
| `DF_DATETIME`   | `4000`                 | `1000`                 | `3000`               | `4000 >= 1000 && 4000 <= 3000`        | **No** (4000 > 3000)                           |
| `DF_DATETIME`   | `1678882496000`        | `1678880000000`       | `1679000000000`      | `1678882496000 >= 1678880000000 && 1678882496000 <= 1679000000000` | **Yes** (it's within the provided ms range)    |
| `DF_DATETIME`   | `9999999999999999`     | `0`                    | `9999999999999999`   | `9999999999999999 >= 0 && 9999999999999999 <= 9999999999999999`    | **Yes** (it’s exactly the upper boundary)       |
| `DF_DATETIME`   | *Invalid or missing*    | *Any*                 | *Any*                | *Cannot read msVal => default false*  | **Excluded** (if `seriesGetDateTime` fails)    |

## Usage:
```c
    DataFrame df;
    DataFrame_Create(&df);

    // DF_DATETIME col => 1000,2000,3000,4000
    Series sdt;
    seriesInit(&sdt, "Times", DF_DATETIME);
    for (int i=1; i<=4; i++) {
        seriesAddDateTime(&sdt, i*1000LL);
    }
    bool ok = df.addSeries(&df, &sdt);
    seriesFree(&sdt);
    assert(ok);

    // Filter => keep [2000..3000]
    DataFrame filtered = df.datetimeFilter(&df, 0, 2000LL, 3000LL);
    assert(filtered.numRows(&filtered)==2);

    const Series* fcol = filtered.getSeries(&filtered, 0);
    long long val=0;
    bool got = seriesGetDateTime(fcol, 0, &val);
    assert(got && val==2000);
    seriesGetDateTime(fcol, 1, &val);
    assert(val==3000);

    DataFrame_Destroy(&filtered);
    DataFrame_Destroy(&df);
```

# Date:: bool datetimeTruncate(DataFrame* df, size_t colIndex, const char* unit)

![datetimeTruncate](diagrams/datetimeTruncate.png "datetimeTruncate")


| **DF_DATETIME Cell** (msVal)   | **Unit**     | **Initial UTC Date/Time**       | **Zeroed-Out Fields**                          | **New UTC Date/Time**                          | **Final Stored Value (ms)**                                  | **Notes**                                                                                                           |
|--------------------------------|-------------|---------------------------------|------------------------------------------------|------------------------------------------------|----------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------|
| `1678871696000` (e.g. ~2023-03-15 12:34:56) | `"day"`     | 2023-03-15 12:34:56 UTC        | Hour=0, Min=0, Sec=0                           | 2023-03-15 00:00:00 UTC                        | `1678838400000`                                              | Eliminates partial hours, minutes, seconds => start of that day in UTC.                                             |
| `1678871696000`               | `"hour"`    | 2023-03-15 12:34:56 UTC         | Min=0, Sec=0                                   | 2023-03-15 12:00:00 UTC                        | `1678872000000`                                              | Truncates to start of hour => 12:00:00.                                                                             |
| `1678871696000`               | `"month"`   | 2023-03-15 12:34:56 UTC         | Day=1, Hour=0, Min=0, Sec=0                    | 2023-03-01 00:00:00 UTC                        | `1677628800000`                                              | Moves to first day of that month => March 1, 2023, midnight UTC.                                                    |
| `1678871696000`               | `"year"`    | 2023-03-15 12:34:56 UTC         | Month=0 (Jan), Day=1, Hour=0, Min=0, Sec=0      | 2023-01-01 00:00:00 UTC                        | `1672531200000`                                              | Zero out month=January, day=1 => start of the year.                                                                 |
| `0`                            | `"day"`     | 1970-01-01 00:00:00 UTC         | (Already 0:00:00)                              | 1970-01-01 00:00:00 UTC                        | `0`                                                          | If it’s already midnight epoch, no change.                                                                          |
| `9999999999999999999`         | `"month"`   | Very large => `gmtime` might fail| If `gmtime` fails => skip or fallback to 0      | Possibly 0 if out of range                     | `0`                                                          | If date is out-of-range for `timegm`, set to 0.                                                                     |
| *Invalid row*                  | *any*       | *Cannot read msVal*            | *No operation performed*                        | *No change or fallback*                        | Possibly unchanged or 0                                      | If `seriesGetDateTime` fails, we skip that row.                                                                     |

## Usage:
```c
    DataFrame df;
    DataFrame_Create(&df);

    // We'll store an epoch for "2023-03-15 12:34:56"
    long long e = 1678871696; // 12:34:56 UTC (approx!)
    long long eMs = 1678838400LL * 1000; // 1678838400000
    Series sdt;
    seriesInit(&sdt, "TruncTest", DF_DATETIME);
    seriesAddDateTime(&sdt, eMs);
    bool ok = df.addSeries(&df, &sdt);
    seriesFree(&sdt);
    assert(ok);

    // Truncate => "day"
    ok = df.datetimeTruncate(&df, 0, "day");
    assert(ok);

    // Now should be ~1678838400 => "2023-03-15 00:00:00"
    const Series* sc = df.getSeries(&df, 0);
    long long msVal = 0;
    bool got = seriesGetDateTime(sc, 0, &msVal);

    long long secVal = msVal / 1000LL;  // because this library stores ms

    assert(secVal == 1678838400LL);

    DataFrame_Destroy(&df);

```



# Date::DataFrame datetimeExtract(const DataFrame* df, size_t dateColIndex, const char* const* fields, size_t numFields)

![datetimeExtract](diagrams/datetimeExtract.png "datetimeExtract")

| **DF_DATETIME Cell (msVal)** | **Converted UTC** (`msVal/1000` → `gmtime`) | **Requested Field** | **Extracted Value**                        | **Stored in DF_INT** | **Notes**                                                                                                  |
|------------------------------|---------------------------------------------|---------------------|--------------------------------------------|-----------------------|------------------------------------------------------------------------------------------------------------|
| `1678882496000`             | 2023-03-15 12:34:56 UTC                     | `"year"`            | `2023` (tm_year + 1900)                    | `2023`                | For that timestamp, year=123 in `struct tm`, plus 1900 => 2023.                                            |
| `1678882496000`             | 2023-03-15 12:34:56 UTC                     | `"month"`           | `3` (tm_mon + 1)                           | `3`                   | If tm_mon=2 => +1 => 3 => “March.”                                                                         |
| `1678882496000`             | 2023-03-15 12:34:56 UTC                     | `"day"`             | `15` (tm_mday)                             | `15`                  |                                                                                                            |
| `1678882496000`             | 2023-03-15 12:34:56 UTC                     | `"hour"`            | `12` (tm_hour)                             | `12`                  | 24-hour clock in UTC.                                                                                      |
| `1678882496000`             | 2023-03-15 12:34:56 UTC                     | `"minute"`          | `34` (tm_min)                              | `34`                  |                                                                                                            |
| `1678882496000`             | 2023-03-15 12:34:56 UTC                     | `"second"`          | `56` (tm_sec)                              | `56`                  |                                                                                                            |
| `0`                          | 1970-01-01 00:00:00 UTC                     | `"year","month",...`| e.g. year=1970, month=1, day=1, hour=0, etc.| e.g. 1970,1,1,0,...   | If msVal=0 => epoch => 1970-01-01.                                                                          |
| `9999999999999999`          | May overflow `time_t` => `gmtime` fails     | (any field)         | `0` (since it can’t parse)                 | `0`                   | If `gmtime` returns NULL, the code stores 0.                                                               |
| *Invalid cell*               | *No data read*                              | (any field)         | `0`                                        | `0`                   | If `seriesGetDateTime` fails for that row, store 0.                                                        |
| Unrecognized field (not in `year,month,day,hour,minute,second`) | Still code only checks known fields | => `outVal=0` fallback  | `0`                                        | `0`                   | The snippet only sets those 6 fields. If a user passes `"millis"`, code results in 0.                      |

## Usage:
```c
    DataFrame df;
    DataFrame_Create(&df);

    // DF_DATETIME => 1 row => aiming for 2023-03-15 12:14:56
    long long e = 1678882496L * 1000; // approximately => "2023-03-15 12:14:56" UTC
    Series sdt;
    seriesInit(&sdt, "DTExtract", DF_DATETIME);
    seriesAddDateTime(&sdt, e);
    bool ok = df.addSeries(&df, &sdt);
    seriesFree(&sdt);
    assert(ok);

    // Extract => year, month, day, hour, minute, second
    const char* fields[] = {"year","month","day","hour","minute","second"};
    DataFrame extracted = df.datetimeExtract(&df, 0, fields, 6);
    assert(extracted.numColumns(&extracted)==6);

    // row0 => year=2023, month=3, day=15, hour=12, minute=14, second=56 (for this epoch)
    const Series* sy = extracted.getSeries(&extracted, 0);
    int val=0;
    bool gotVal = seriesGetInt(sy, 0, &val);
    assert(gotVal && val==2023);

    const Series* sm = extracted.getSeries(&extracted, 1);
    seriesGetInt(sm, 0, &val);
    assert(val==3);

    const Series* sd = extracted.getSeries(&extracted, 2);
    seriesGetInt(sd, 0, &val);
    assert(val==15);

    const Series* sh = extracted.getSeries(&extracted, 3);
    seriesGetInt(sh, 0, &val);
    // should be 12 if that epoch is correct
    assert(val==12);

    const Series* smin = extracted.getSeries(&extracted, 4);
    seriesGetInt(smin, 0, &val);
    // we expect 14 from that epoch
    assert(val==14);

    const Series* ssec = extracted.getSeries(&extracted, 5);
    seriesGetInt(ssec, 0, &val);
    assert(val==56);

    DataFrame_Destroy(&extracted);
    DataFrame_Destroy(&df);
```


# Date::DataFrame datetimeGroupBy(const DataFrame* df, size_t dateColIndex, const char* truncateUnit)
![datetimeGroupBy](diagrams/datetimeGroupBy.png "datetimeGroupBy")

| **Original DataFrame**                           | **`dateColIndex`** | **`truncateUnit`** | **Steps**                                                                                                                             | **Final Returned DataFrame**                                    | **Notes**                                                                                                                                                  |
|--------------------------------------------------|--------------------|--------------------|----------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------|
| A DF_DATETIME column in ms (e.g., storing 2023-03-15 12:34:56, 2023-03-15 14:10:00, 2023-03-16 00:00:00, etc.). Other columns as well. | e.g. `1`          | `"day"`            | 1) **Slice** all rows → `copyAll` <br/> 2) **Truncate** that DF’s `dateColIndex` to `"day"` → zero out hour/min/sec => e.g. 2023-03-15 00:00:00, 2023-03-16 00:00:00, etc. <br/> 3) **GroupBy** that truncated column. | A new grouped DataFrame, typically with columns like `["group", "count"]` or more depending on your `groupBy` design. | Often yields fewer rows if multiple datetimes collapse to the same truncated day (or hour/month/year).                                                      |
| A DF_DATETIME column in ms, *but no rows*        | e.g. `0`          | `"month"`          | 1) Slice => an empty DataFrame (since numRows=0) <br/> 2) Truncation + GroupBy on an empty set => results in an empty DF as well.      | **Empty** DataFrame with 0 rows                                   | If `copyAll.numRows(...)` is 0, we just return that empty DataFrame.                                                                                        |
| DF_DATETIME w/ partial times => `truncateUnit="year"` | `2`              | `"year"`           | 1) Slice => copyAll <br/> 2) `dfDatetimeTruncate_impl(...,"year")` => sets month=0, day=1, etc. <br/> 3) groupBy that year-level date  | Possibly columns like `[TruncatedDate, otherAggregations?]` depending on groupBy output | All times in the same year now become the *same* group if they share the same truncated year (like 2023-01-01 00:00:00).                                  |
| DF_DATETIME w/ massive out-of-range or invalid rows | any index        | any unit ("hour") | 1) Slicing includes them <br/> 2) Truncation might set them to 0 if `gmtime` fails <br/> 3) groupBy lumps all invalid => 1970-01-01  | A grouped DF, possibly including a `1970-01-01 00:00:00` group for those out-of-range.   | If `timegm` fails, the truncated ms => 0 => they appear in the group for “1970-01-01 00:00:00.”                                                           |

## Usage:

```c
    DataFrame df;
    DataFrame_Create(&df);

    // times => same day => 2023-03-15, but different hours
    // plus another day => 2023-03-16
    long long day1_0 = 1678838400LL * 1000LL; // "2023-03-15 00:00:00"
    long long day1_1 = 1678842000LL * 1000LL; // "2023-03-15 01:00:00"
    long long day2_0 = 1678924800LL * 1000LL; // "2023-03-16 00:00:00"
    Series sdt;
    seriesInit(&sdt, "GroupDT", DF_DATETIME);
    seriesAddDateTime(&sdt, day1_0);
    seriesAddDateTime(&sdt, day1_1);
    seriesAddDateTime(&sdt, day2_0);
    bool ok = df.addSeries(&df, &sdt);
    seriesFree(&sdt);
    assert(ok);

    // group by day
    DataFrame grouped = df.datetimeGroupBy(&df, 0, "day");
    // We'll do a minimal check => at least 2 distinct days => 2 rows
    assert(grouped.numRows(&grouped)==2);
    df.print(&grouped);

    DataFrame_Destroy(&grouped);
    DataFrame_Destroy(&df);


```


# Date::bool datetimeRound(const DataFrame* df, size_t colIndex, const char* unit)
![datetimeRound](diagrams/datetimeRound.png "datetimeRound")

| Original `msVal`      | Rounding Unit | New (Rounded) `msVal` | Explanation                                                                                     |
|-----------------------|---------------|------------------------|-------------------------------------------------------------------------------------------------|
| **1678871696789**     | `"minute"`    | **1678871700000**      | - Original ≈ 2023-03-15 12:34:56.789 UTC.<br/>- remainder = 789 ms ≥ 500 ⇒ round up to 12:34:57.<br/>- Now rounding to minute: 57 ≥ 30 ⇒ minute++ ⇒ 12:35:00.<br/>- Final epoch ms = 1678871700000. |
| **1679003999123**     | `"day"`       | **1679001600000**      | - Original ≈ 2023-03-16 23:59:59.123 UTC.<br/>- remainder = 123 ms < 500 ⇒ remains 23:59:59.<br/>- Rounding to day: hour=23 ≥ 12 ⇒ next day ⇒ 2023-03-17 00:00:00.<br/>- Final epoch ms = 1679001600000. |
| **1678838400650**     | `"second"`    | **1678838401000**      | - Original ≈ 2023-03-15 00:00:00.650 UTC.<br/>- remainder = 650 ms ≥ 500 ⇒ increment second ⇒ 00:00:01.<br/>- Rounding to second does nothing more ⇒ final epoch ms = 1678838401000. |
| **1677871204000**     | `"hour"`      | **1677871200000**      | - Original ≈ 2023-03-03 11:00:04.000 UTC.<br/>- remainder=0, no change to seconds.<br/>- Rounding to hour: minute=0 but sec=4≥30? No, so hour stays 11 ⇒ zero out minutes & seconds ⇒ 2023-03-03 11:00:00.<br/>- Final epoch ms = 1677871200000. |

## Usage:
```c
    DataFrame df;
    DataFrame_Create(&df);

    // Create a DF_DATETIME column with some known epoch-millis:
    // Let's pick a base time: 2023-03-15 12:34:56.789 => epoch = 1678871696, leftover .789 ms
    // Multiply by 1000 for ms => 1678871696789
    long long baseMs = 1678871696789LL;
    long long times[] = {
        baseMs,                // ~ 12:34:56.789
        baseMs + 501,         // ~ 12:34:57.290 (should round up to 12:34:58 if rounding second)
        baseMs + 45*1000,     // ~ 12:35:41.789 (should test rounding minute)
        baseMs + 3600*1000,   // ~ 13:34:56.789 (test hour rounding)
        baseMs - 200LL        // negative remainder check near the boundary
    };

    Series sdt;
    seriesInit(&sdt, "RoundTimes", DF_DATETIME);
    for (int i = 0; i < 5; i++) {
        seriesAddDateTime(&sdt, times[i]);
    }
    bool ok = df.addSeries(&df, &sdt);
    seriesFree(&sdt);
    assert(ok);

    // We'll test a single rounding unit first: "second"
    ok = df.datetimeRound(&df, 0, "second");
    assert(ok);

    // Validate row 0 => original remainder .789 => >= 500 => +1 sec
    // row0 was 1678871696789 => break that into (seconds=1678871696, remainder=789).
    // => final => 1678871697 in seconds => *1000 => 1678871697000
    const Series* col = df.getSeries(&df, 0);
    long long val = 0;
    bool gotVal = seriesGetDateTime(col, 0, &val);
    assert(gotVal);
    assert(val == 1678871697000LL);

    // Validate row 1 => was baseMs+501 => remainder ~ 501 => round up => +1 sec from base
    // So we expect second = baseSec+1 => 1678871697 in seconds => 1678871697000 ms
    seriesGetDateTime(col, 1, &val);
    assert(val == 1678871697000LL);

    // We won’t check all rows in detail here, but you can. Let’s at least confirm row 4 works.
    seriesGetDateTime(col, 4, &val);
    // row4 was baseMs - 200 => 1678871696589 => remainder=589 => round up => second=1678871697
    assert(val == 1678871697000LL);

    // Now let’s do "minute" rounding on row0 to see if it changes to 12:35:00
    // We can re-round the entire column or re-add times. For simplicity, re-insert them:
    DataFrame_Destroy(&df);
    DataFrame_Create(&df);
    seriesInit(&sdt, "RoundTimes", DF_DATETIME);
    for (int i = 0; i < 5; i++) {
        seriesAddDateTime(&sdt, times[i]);
    }
    df.addSeries(&df, &sdt);
    seriesFree(&sdt);

    // Round to minute
    df.datetimeRound(&df, 0, "minute");

    col = df.getSeries(&df, 0);
    seriesGetDateTime(col, 0, &val);
    // base => "12:34:56.789" => second=56 => >=30 => round up => minute=35 => new time=12:35:00
    // Let's check the resulting epoch in UTC
    // 12:35:00 on 2023-03-15 => epoch=1678871700 => in ms => 1678871700000
    assert(val == 1678871700000LL);

    DataFrame_Destroy(&df);

```

# Date::DataFrame datetimeBetween(const DataFrame* df, size_t dateColIndex, const char* startStr, const char* endStr, const char* formatType)
![datetimeBetween](diagrams/datetimeBetween.png "datetimeBetween")

| **Input**                                                      | **Parsed Range**                                      | **Output**                                                                                                          | **Explanation**                                                                                                                                                                                               |
|----------------------------------------------------------------|--------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `startStr = "2023-03-15 00:00:00", endStr = "2023-03-16 00:00:00"`<br>`formatType = "%Y-%m-%d %H:%M:%S"` | - `parseEpochSec("2023-03-15 00:00:00")` => `1678838400` (seconds)<br>- `parseEpochSec("2023-03-16 00:00:00")` => `1678924800`<br>- Converted to ms => `[1678838400000..1678924800000]` | A new `DataFrame` containing **only rows** whose timestamp in `colIndex` is within **[1678838400000..1678924800000]** (inclusive). | - The function multiplies each parsed epoch-second by 1000 to get milliseconds.<br>- It then calls `df->datetimeFilter(...)`, filtering rows where `DF_DATETIME` ∈ [1678838400000..1678924800000].                                                    |
| `startStr = "2023-03-20", endStr = "2023-03-15"`<br>`formatType = "%Y-%m-%d"`                            | - Suppose `"2023-03-20"` => `1679270400` (sec)<br>- `"2023-03-15"` => `1678838400` (sec)<br>- Ms => `[1679270400000..1678838400000]` but swapped ⇒ `[1678838400000..1679270400000]` | Similar `DataFrame` subset, but the range is **[1678838400000..1679270400000]** after swap.                                    | - If `startMs > endMs`, the code swaps them, ensuring the final filter range is always ascending.<br>- Only rows within that millisecond window remain in the returned `DataFrame`.                                                                  |
| `startStr = "invalid date", endStr = "2023-03-15 12:00:00"`<br>`formatType = "%Y-%m-%d %H:%M:%S"`        | - `parseEpochSec("invalid date", ...)` => 0 (failure)<br>- `parseEpochSec("2023-03-15 12:00:00", ...)` => `1678872000` (sec) => `1678872000000` (ms)<br>- Final range => `[0..1678872000000]` | Any row with a timestamp ≤ 1678872000000 ms is kept.                                                                            | - An invalid date string returns `0`, so `startMs = 0`.<br>- `endMs` is ~ `1678872000000`.<br>- The final filter is `[0..1678872000000]`, meaning rows at or after the Unix epoch but before 2023-03-15 12:00:00 remain.                              |



## Usage:
```c
    DataFrame df;
    DataFrame_Create(&df);

    
    long long times[] = {
        1678838400LL * 1000, // "2023-03-15 00:00:00" in MILLISECONDS
        1678871696LL * 1000, // "2023-03-15 9:14:56"
        1678924800LL * 1000, // "2023-03-16 00:00:00"
        1679000000LL * 1000  // "2023-03-16 20:53:20"
    };
    

    Series sdt;
    seriesInit(&sdt, "BetweenTest", DF_DATETIME);
    for (int i = 0; i < 4; i++) {
        // Storing raw seconds. If your code expects ms in DF_DATETIME,
        // multiply by 1000. But we'll store seconds for clarity here.
        seriesAddDateTime(&sdt, times[i]);
    }
    bool ok = df.addSeries(&df, &sdt);
    seriesFree(&sdt);
    assert(ok);

    // We'll keep rows between "2023-03-15 12:00:00" and "2023-03-16 00:00:00" inclusive
    // => start=1678862400, end=1678924800
    DataFrame result = df.datetimeBetween(
        &df,              // inDF
        0,                // dateColIndex
        "2023-03-15 9:13:00",  // start
        "2023-03-16 00:00:00",  // end
        "%Y-%m-%d %H:%M:%S"     // format
    );

    // The only rows in that range:
    //   times[1] = 1678871696000 => ~ 2023-03-15 12:34:56
    //   times[2] = 1678924800000 => 2023-03-16 00:00:00 (inclusive)
    assert(result.numRows(&result) == 2);
    result.print(&result);
    const Series* sres = result.getSeries(&result, 0);
    long long val=0;
    // row0 => 1678871696
    bool gotVal = seriesGetDateTime(sres, 0, &val);
    assert(gotVal && val == 1678871696000LL);
    // row1 => 1678924800
    seriesGetDateTime(sres, 1, &val);
    assert(val == 1678924800000LL);

    DataFrame_Destroy(&result);
    DataFrame_Destroy(&df);

```



# Date::bool datetimeRebase(const DataFrame* df, size_t colIndex, long long anchorMs)
![datetimeRebase](diagrams/datetimeRebase.png "datetimeRebase")


| **Original `msVal`** | **`anchorMs`** | **Computation**                  | **New (Rebased) `msVal`** | **Explanation**                                                         |
|----------------------|---------------:|----------------------------------|---------------------------:|-------------------------------------------------------------------------|
| **10,000**          |         5,000  | `newMs = (10000 - 5000) = 5000`  | **5000**                   | - Original value = 10,000 ms.<br>- Subtract anchor=5,000 ms => 5,000 ms.<br>- 5,000 ≥ 0, so no clamp needed.                                       |
| **2,000**           |         3,000  | `newMs = (2000 - 3000) = -1000`  | **0**                      | - Original = 2,000 ms.<br>- Subtract anchor=3,000 => -1,000.<br>- Negative => clamp to 0.                                                         |
| **123,456,789**     |    100,000,000 | `newMs = (123,456,789 - 100,000,000) = 23,456,789` | **23,456,789**           | - Original = 123,456,789 ms (~1.43 days from epoch).<br>- Anchor=100,000,000 => result=23,456,789 ms.                                             |
| **1,000**           |         1,000  | `newMs = (1000 - 1000) = 0`      | **0**                      | - Perfect offset => exactly zero after rebase.<br>- No clamp needed.                                        |
| **500**             |         500    | `newMs = (500 - 500) = 0`        | **0**                      | - Another example => results in 0.                                                                          |
| **2,500**           |         500    | `newMs = (2500 - 500) = 2000`    | **2,000**                  | - Subtract anchor => 2,000 ms.                                                                              |


## Usage:

```c
    DataFrame df;
    DataFrame_Create(&df);

    Series sdt;
    seriesInit(&sdt, "RebaseTest", DF_DATETIME);
    // We'll store times: 1000, 2000, 3000, 500
    long long times[] = {1000LL, 2000LL, 3000LL, 500LL};
    for (int i=0; i<4; i++) {
        seriesAddDateTime(&sdt, times[i]);
    }
    bool ok = df.addSeries(&df, &sdt);
    seriesFree(&sdt);
    assert(ok);

    // rebase with anchor=1500 => newVal = oldVal -1500, clamp >=0
    // so => row0=1000 => -500 => clamp=0
    //        row1=2000 => 500
    //        row2=3000 => 1500
    //        row3=500  => -1000 => clamp=0
    ok = df.datetimeRebase(&df, 0, 1500LL);
    assert(ok);

    const Series* col = df.getSeries(&df, 0);
    long long val=0;
    seriesGetDateTime(col, 0, &val);
    assert(val == 0LL);
    seriesGetDateTime(col, 1, &val);
    assert(val == 500LL);
    seriesGetDateTime(col, 2, &val);
    assert(val == 1500LL);
    seriesGetDateTime(col, 3, &val);
    assert(val == 0LL);

    DataFrame_Destroy(&df);

```

# Date::bool datetimeClamp(const DataFrame* df, size_t colIndex, long long minMs, long long maxMs)
![datetimeClamp](diagrams/datetimeClamp.png "datetimeClamp")

| **Original `msVal`** | **`minMs`** | **`maxMs`** | **Computed `msVal`**                     | **Explanation**                                                                                               |
|----------------------|------------:|------------:|-------------------------------------------|---------------------------------------------------------------------------------------------------------------|
| **1,000**           |       2,000 |      10,000 | **2,000**                                 | - `1,000` < `minMs` => clamped up to `2,000`.                                                                 |
| **5,000**           |       2,000 |      10,000 | **5,000**                                 | - Already within `[2,000..10,000]` => remains `5,000`.                                                        |
| **15,000**          |       2,000 |      10,000 | **10,000**                                | - `15,000` > `maxMs` => clamped down to `10,000`.                                                             |
| **1,999**           |       2,000 |      10,000 | **2,000**                                 | - Just below `minMs` => clamped up to `2,000`.                                                                |
| **9,999**           |       2,000 |      10,000 | **9,999**                                 | - Falls within the range => unchanged.                                                                        |
| **-500**            |       2,000 |      10,000 | **2,000**                                 | - Negative value => also clamped up to `2,000`.                                                               |


## Usage:

```c
    DataFrame df;
    DataFrame_Create(&df);

    // Create a DF_DATETIME col => 10, 50, 100, 9999
    Series sdt;
    seriesInit(&sdt, "ClampTest", DF_DATETIME);
    long long vals[] = {10LL, 50LL, 100LL, 9999LL};
    for (int i=0; i<4; i++) {
        seriesAddDateTime(&sdt, vals[i]);
    }
    bool ok = df.addSeries(&df, &sdt);
    seriesFree(&sdt);
    assert(ok);

    // clamp => min=20, max=9000
    // => 10 => 20
    // => 50 => 50
    // => 100 => 100
    // => 9999 => 9000
    ok = df.datetimeClamp(&df, 0, 20LL, 9000LL);
    assert(ok);

    const Series* col = df.getSeries(&df, 0);
    long long val=0;

    seriesGetDateTime(col, 0, &val);
    assert(val == 20LL);
    seriesGetDateTime(col, 1, &val);
    assert(val == 50LL);
    seriesGetDateTime(col, 2, &val);
    assert(val == 100LL);
    seriesGetDateTime(col, 3, &val);
    assert(val == 9000LL);

    DataFrame_Destroy(&df);
```

# DataFrame::Aggregate

# Aggregate::double sum(const DataFrame* df, size_t colIndex)

Given a DataFrame `df` and a column index `colIndex`, the function computes the sum of that column’s values by iterating through each row and **accumulating** the entries that successfully read. Formally, if the column has \(n\) rows and we denote the value in row \(r\) as \(x_r\), then:

# $\sum_{r=0}^{n-1} x_r$

## Usage:
```c
    DataFrame df;
    DataFrame_Create(&df);

    // Build a DF_INT column => [1, 2, 3, 4]
    Series s;
    seriesInit(&s, "Numbers", DF_INT);
    for (int i = 1; i <= 4; i++) {
        seriesAddInt(&s, i);
    }
    bool ok = df.addSeries(&df, &s);
    assert(ok);

    double sumRes = df.sum(&df, 0);
    assertAlmostEqual(sumRes, 1+2+3+4, 1e-9);

    seriesFree(&s);
    DataFrame_Destroy(&df);
```



# Aggregate::double mean(const DataFrame* df, size_t colIndex)

Given a DataFrame `df` and a column index `colIndex`, the function computes the **mean** of that column’s values by summing all valid entries and dividing by the total number of rows. Formally, if the column has \(n\) rows and we denote the value in row \(r\) as \(x_r\), then:

# $\displaystyle \frac{1}{n} \sum_{r=0}^{n-1} x_r$

## Usage:
```c
    DataFrame df;
    DataFrame_Create(&df);

    Series s;
    seriesInit(&s, "MeanTest", DF_DOUBLE);

    // [1.0, 2.0, 3.0, 4.0]
    double arr[] = {1.0, 2.0, 3.0, 4.0};
    for (int i=0; i<4; i++){
        seriesAddDouble(&s, arr[i]);
    }
    bool ok = df.addSeries(&df, &s);
    assert(ok);

    double m = df.mean(&df, 0);
    // average = (1+2+3+4)/4 = 2.5
    assertAlmostEqual(m, 2.5, 1e-9);

    seriesFree(&s);
    DataFrame_Destroy(&df);
```


# Aggregate::double min(const DataFrame* df, size_t colIndex)

Given a DataFrame `df` and a column index `colIndex`, the function finds the **minimum** of that column’s values by iterating through each row and keeping track of the smallest entry encountered. Formally, if the column has \(n\) rows and we denote the value in row \(r\) as \(x_r\), then:

# $\displaystyle \min_{0 \le r < n} \ x_r$

## Usage:
```c
    DataFrame df;
    DataFrame_Create(&df);

    // DF_DOUBLE col => [10.5, 2.2, 7.7]
    Series s;
    seriesInit(&s, "MinTest", DF_DOUBLE);
    seriesAddDouble(&s, 10.5);
    seriesAddDouble(&s, 2.2);
    seriesAddDouble(&s, 7.7);
    df.addSeries(&df, &s);
    seriesFree(&s);

    double mn = df.min(&df, 0);
    assertAlmostEqual(mn, 2.2, 1e-9);

    DataFrame_Destroy(&df);
```


# Aggregate::double max(const DataFrame* df, size_t colIndex)

Given a DataFrame `df` and a column index `colIndex`, the function finds the **maximum** of that column’s values by iterating through each row and keeping track of the largest entry encountered. Formally, if the column has \(n\) rows and we denote the value in row \(r\) as \(x_r\), then:

# $\displaystyle \max_{0 \le r < n} \ x_r$

## Usage:
```c
    DataFrame df;
    DataFrame_Create(&df);

    // DF_INT col => [3,9,1]
    Series s;
    seriesInit(&s, "MaxTest", DF_INT);
    seriesAddInt(&s, 3);
    seriesAddInt(&s, 9);
    seriesAddInt(&s, 1);
    df.addSeries(&df, &s);
    seriesFree(&s);

    double mx = df.max(&df, 0);
    assertAlmostEqual(mx, 9, 1e-9);

    DataFrame_Destroy(&df);
```


# Aggregate::double count(const DataFrame* df, size_t colIndex)

Given a DataFrame `df` and a column index `colIndex`, the function returns the **count** of valid (non-null) entries in that column by iterating over each row and incrementing for every successfully read value.

# $\displaystyle \sum_{r=0}^{n-1} \mathbf{I}(x_r\ \text{is not null})$

## Usage:
```c
    DataFrame df;
    DataFrame_Create(&df);

    // We'll do DF_STRING with 4 valid rows
    Series s;
    seriesInit(&s, "CountTest", DF_STRING);
    seriesAddString(&s, "apple");
    seriesAddString(&s, "banana");
    seriesAddString(&s, "orange");
    seriesAddString(&s, "kiwi");
    df.addSeries(&df, &s);
    seriesFree(&s);

    double c = df.count(&df, 0);
    // 4 non-null strings => count=4
    assertAlmostEqual(c, 4.0, 1e-9);

    DataFrame_Destroy(&df);
```


# Aggregate::double median(const DataFrame* df, size_t colIndex)

Given a DataFrame `df` and a column index `colIndex`, the function returns the **median** of the column’s numeric values by sorting them. 

![image](https://github.com/user-attachments/assets/6683f5b1-a608-48da-9752-ec248aaf5f0d)

The median of a set of numbers is the value separating the higher half from the lower half of a data sample, a population, or a probability distribution. For a data set, it may be thought of as the “middle" value.
## Usage:
```c
    DataFrame df;
    DataFrame_Create(&df);

    // DF_DOUBLE => [2, 4, 6, 8] => median = (4+6)/2=5
    Series s;
    seriesInit(&s, "MedianTest", DF_DOUBLE);
    seriesAddDouble(&s, 2.0);
    seriesAddDouble(&s, 4.0);
    seriesAddDouble(&s, 6.0);
    seriesAddDouble(&s, 8.0);
    df.addSeries(&df, &s);
    seriesFree(&s);

    double med = df.median(&df, 0);
    assertAlmostEqual(med, 5.0, 1e-9);

    DataFrame_Destroy(&df);
```


# Aggregate::double mode(const DataFrame* df, size_t colIndex)

Given a DataFrame `df` and a column index `colIndex`, the function returns the **mode** of the column’s numeric values by sorting them. 
![image](https://github.com/user-attachments/assets/697f6b7b-863f-4016-a5b7-efbc3dea9083)

In statistics, the mode is the value that appears most often in a set of data values.[1] If X is a discrete random variable, the mode is the value x at which the probability mass function takes its maximum value (i.e., x=argmaxxi P(X = xi)). In other words, it is the value that is most likely to be sampled.

## Usage:
```c
    DataFrame df;
    DataFrame_Create(&df);

    // DF_INT => [2,2,5,2,5] => mode=2 since freq(2)=3 freq(5)=2
    Series s;
    seriesInit(&s, "ModeTest", DF_INT);
    seriesAddInt(&s, 2);
    seriesAddInt(&s, 2);
    seriesAddInt(&s, 5);
    seriesAddInt(&s, 2);
    seriesAddInt(&s, 5);
    df.addSeries(&df, &s);
    seriesFree(&s);

    double modeVal = df.mode(&df, 0);
    assertAlmostEqual(modeVal, 2.0, 1e-9);

    DataFrame_Destroy(&df);
```


# Aggregate::double std(const DataFrame* df, size_t colIndex)
Given a DataFrame `df` and a column index `colIndex`, the function returns the **standard deviation** of the column’s numeric values. The **sample** standard deviation is:
![image](https://github.com/user-attachments/assets/5a3c7690-c894-467c-96f3-6ffd11390391)

# $\sigma={\sqrt {\frac {\sum(x_{i}-{\mu})^{2}}{N}}}$
- $\sigma$	=	population standard deviation
- $N$	=	the size of the population
- $x_i$	=	each value from the population
- $\mu$	=	the population mean
## Usage:
```c
    DataFrame df;
    DataFrame_Create(&df);

    // DF_DOUBLE => [1,2,3,4]
    // sample standard deviation => sqrt(1.6666667) ~ 1.290994
    Series s;
    seriesInit(&s, "StdTest", DF_DOUBLE);
    seriesAddDouble(&s, 1.0);
    seriesAddDouble(&s, 2.0);
    seriesAddDouble(&s, 3.0);
    seriesAddDouble(&s, 4.0);
    df.addSeries(&df, &s);
    seriesFree(&s);

    double stdev = df.std(&df, 0);
    // Expect ~1.290994 (since sample var=1.6667)
    assert(fabs(stdev - 1.290994) < 1e-5);

    DataFrame_Destroy(&df);
```


# Aggregate:: double var(const DataFrame* df, size_t colIndex)
Given a DataFrame `df` and a column index `colIndex`, the function returns the **variance** of the column’s numeric values.

![image](https://github.com/user-attachments/assets/777f8349-3dc9-4a2f-bb35-7538439d0e9e)


# $S^2 = \frac{\sum (x_i - \bar{x})^2}{n - 1}$
- $S^2$	=	sample variance
- $x_i$	=	the value of the one observation
- $\bar{x}$	=	the mean value of all observations
- $n$	=	the number of observations
## Usage:
```c
    DataFrame df;
    DataFrame_Create(&df);

    // DF_DOUBLE => [1, 2, 3, 4]
    // sample variance => 1.66666666...
    Series s;
    seriesInit(&s, "VarTest", DF_DOUBLE);
    for (int i=1; i<=4; i++){
        seriesAddDouble(&s, i);
    }
    df.addSeries(&df, &s);
    seriesFree(&s);

    double v = df.var(&df, 0);
    // population var=1.25, sample var= 1.6666667 (2 decimal=1.67)
    // 1->1,2->4,3->9,4->16 => mean=2.5 => squares ~ (1.5^2 +0.5^2+0.5^2+1.5^2)=1.5^2=2.25 => sum=5 => /3=1.6667
    assert(fabs(v - 1.6666667) < 1e-5);

    DataFrame_Destroy(&df);
```

# Aggregate:: double range(const DataFrame* df, size_t colIndex)

Given a DataFrame `df` and a column index `colIndex`, the function returns the **range** of the column’s numeric values by computing the difference between the maximum and the minimum. If $\(n\)$ is the number of valid rows, and $\(x_r\)$ is the value in row $\(r\)$, then:

# $\displaystyle \text{range} = \max_{0 \le r < n} x_r - \min_{0 \le r < n} x_r$
## Usage:
```c
    DataFrame df;
    DataFrame_Create(&df);

    // DF_INT => [3,7,1,9] => min=1, max=9 => range=8
    Series s;
    seriesInit(&s, "RangeTest", DF_INT);
    seriesAddInt(&s, 3);
    seriesAddInt(&s, 7);
    seriesAddInt(&s, 1);
    seriesAddInt(&s, 9);
    df.addSeries(&df, &s);
    seriesFree(&s);

    double r = df.range(&df, 0);
    assertAlmostEqual(r, 8.0, 1e-9);

    DataFrame_Destroy(&df);
```


# Aggregate:: double quantile(const DataFrame* df, size_t colIndex, double q)
Given a DataFrame `df` and a column index `colIndex`, the function computes the **$\(\alpha\)$-quantile** of the column’s numeric values. 

![image](https://github.com/user-attachments/assets/6a83d415-c6f3-4893-b85a-80746afdb26c)

The area below the red curve is the same in the intervals (−∞,Q1), (Q1,Q2), (Q2,Q3), and (Q3,+∞).

In statistics and probability, quantiles are cut points dividing the range of a probability distribution into continuous intervals with equal probabilities, or dividing the observations in a sample in the same way. There is one fewer quantile than the number of groups created. Common quantiles have special names, such as quartiles (four groups), deciles (ten groups), and percentiles (100 groups). The groups created are termed halves, thirds, quarters, etc., though sometimes the terms for the quantile are used for the groups created, rather than for the cut points.

## Usage:
```c
    DataFrame df;
    DataFrame_Create(&df);

    // DF_DOUBLE => [10,20,30,40]
    Series s;
    seriesInit(&s, "QuantTest", DF_DOUBLE);
    seriesAddDouble(&s, 10);
    seriesAddDouble(&s, 20);
    seriesAddDouble(&s, 30);
    seriesAddDouble(&s, 40);
    df.addSeries(&df, &s);
    seriesFree(&s);

    double q25 = df.quantile(&df, 0, 0.25); 
    // sorted => [10,20,30,40], 0.25*(4-1)=0.75 => idxBelow=0, idxAbove=1 => interpol
    // => 10 + 0.75*(20-10)= 10+7.5=17.5
    assertAlmostEqual(q25, 17.5, 1e-9);

    double q75 = df.quantile(&df, 0, 0.75); 
    // pos=0.75*(3)=2.25 => idxBelow=2 => 30 => fraction=0.25 => next=40 => val=30+0.25*(40-30)=32.5
    assertAlmostEqual(q75, 32.5, 1e-9);

    DataFrame_Destroy(&df);
```




# Aggregate:: double iqr(const DataFrame* df, size_t colIndex)

Given a DataFrame `df` and a column index `colIndex`, the function computes the **interquartile range (IQR)** of the column’s numeric values. It uses the 25th percentile $(\(Q_1\))$ and the 75th percentile $(\(Q_3\))$:

![image](https://github.com/user-attachments/assets/ea2dc610-80ea-401b-9357-595ff8371d27)

In descriptive statistics, the interquartile range (IQR) is a measure of statistical dispersion, which is the spread of the data.[1] The IQR may also be called the midspread, middle 50%, fourth spread, or H‑spread. It is defined as the difference between the 75th and 25th percentiles of the data.[2][3][4] To calculate the IQR, the data set is divided into quartiles, or four rank-ordered even parts via linear interpolation.[1] These quartiles are denoted by Q1 (also called the lower quartile), Q2 (the median), and Q3 (also called the upper quartile). The lower quartile corresponds with the 25th percentile and the upper quartile corresponds with the 75th percentile, so IQR = Q3 −  Q1[1].

The IQR is an example of a trimmed estimator, defined as the 25% trimmed range, which enhances the accuracy of dataset statistics by dropping lower contribution, outlying points.[5] It is also used as a robust measure of scale[5] It can be clearly visualized by the box on a box plot.[1]


# $\displaystyle \text{IQR} = Q_{3} - Q_{1}$

where $\(Q_{1} = \text{quantile}(0.25)\)$ and $\(Q_{3} = \text{quantile}(0.75)\)$. The values are typically found via interpolation after sorting.


## Usage:
```c
    DataFrame df;
    DataFrame_Create(&df);

    // DF_INT => [2,4,6,8]
    Series s;
    seriesInit(&s, "IQRTest", DF_INT);
    seriesAddInt(&s, 2);
    seriesAddInt(&s, 4);
    seriesAddInt(&s, 6);
    seriesAddInt(&s, 8);
    df.addSeries(&df, &s);
    seriesFree(&s);

    double iqrVal = df.iqr(&df, 0);
    // 25% ~ 3, 75% ~ 7 => iqr=3
    // Let's see precisely:
    // sorted => [2,4,6,8], q1 => 0.25*(3)=0.75 => interpol => 2 +0.75*(4-2)= 3.5? Actually let's do carefully
    // Actually let's do quick: if q1=3, q3=7 => iqr=3 => We'll accept approximate
    // This might differ a bit if your quantile logic is continuous. We'll assert ~3
    assertAlmostEqual(iqrVal, 3.0, 0.1);

    DataFrame_Destroy(&df);
```




# Aggregate:: double nullCount(const DataFrame* df, size_t colIndex)
If a column has $\(n\)$ rows, we define an “is-null” indicator $\(\mathbf{I}(\cdot)\)$ that is 1 if reading row $\(r\)$ fails, and 0 otherwise. The **null count** is:

# $\[\sum_{r=0}^{n-1} \mathbf{I}\bigl(\text{read fails at row } r\bigr).\]$

Thus, each time seriesGetXxx(...) returns false, we interpret that row as null and increment by 1.

## Usage:
```c
    DataFrame df;
    DataFrame_Create(&df);

    // 2) Build a DF_STRING column with a single valid entry ("hi")
    Series s;
    seriesInit(&s, "NullTest", DF_STRING);
    seriesAddString(&s, "hi");   // 1 row => "hi"

    bool ok = df.addSeries(&df, &s);
    seriesFree(&s);
    assert(ok);

    // 3) Prepare a "null" pointer for the second row => row2
    //    dfAddRow_impl for DF_STRING checks if (strPtr == NULL) => returns false,
    //    so the row won't actually be added.
    const char* row2 = NULL;
    const void* rowData[1];
    rowData[0] = (const void*)row2;

    // Attempt to add a second row. This will fail silently and not increment nrows.
    if (df.numColumns(&df) == 1) {
        bool added = df.addRow(&df, rowData);
        // This is expected to be 'false' because strPtr == NULL
        assert(!added);
    }

    // 4) Check nullCount. We still only have 1 row => "hi", no actual "null" rows
    double nCount = df.nullCount(&df, 0);
    // Because the second row never got added, aggregator sees only "hi".
    // => no null => assert nCount==0
    assert(nCount == 0.0);

    DataFrame_Destroy(&df);
```



# Aggregate:: double uniqueCount(const DataFrame* df, size_t colIndex)
The unique count aggregator's goal is to count the number of distinct values in a specified column

Why the O(n²) Approach?
This naive check is simple to implement, iterating pairs of elements. For each new element, we see if it already exists among previously encountered elements.
In a production environment, we might use a hash set or sort the array and do a single pass to find distinct elements in O(n log n). But here, the naive approach is straightforward.

# $\displaystyle \sum_{r=0}^{n-1} \mathbf{I}\!\Bigl( x_r \not\in \{x_0,\dots,x_{r-1}\}\Bigr)$

## Usage:
```c
    DataFrame df;
    DataFrame_Create(&df);

    // DF_INT => [1,2,2,3]
    Series s;
    seriesInit(&s, "UniqueCountTest", DF_INT);
    seriesAddInt(&s,1);
    seriesAddInt(&s,2);
    seriesAddInt(&s,2);
    seriesAddInt(&s,3);
    df.addSeries(&df, &s);
    seriesFree(&s);

    double uniq = df.uniqueCount(&df, 0);
    // distinct= {1,2,3} => 3
    assert(uniq==3.0);

    DataFrame_Destroy(&df);
```



# Aggregate::double product(const DataFrame* df, size_t colIndex)
The product aggregator multiplies all valid numeric values in a specified column, returning the cumulative product as a double

# $\displaystyle \prod_{r=0}^{n-1} x_r$

## Usage:
```c
    DataFrame df;
    DataFrame_Create(&df);

    // DF_INT => [2,3,4] => product=2*3*4=24
    Series s;
    seriesInit(&s, "ProdTest", DF_INT);
    seriesAddInt(&s,2);
    seriesAddInt(&s,3);
    seriesAddInt(&s,4);
    df.addSeries(&df, &s);
    seriesFree(&s);

    double prod = df.product(&df, 0);
    assert(prod==24.0);

    DataFrame_Destroy(&df);
```




# Aggregate::double nthLargest(const DataFrame* df, size_t colIndex, size_t n)
Given a DataFrame `df`, a column index `colIndex`, and an integer `n`, the function returns the **n-th largest** value in the column’s numeric data. If the column has $\(N\)$ valid entries $\(x_0, x_1, \dots, x_{N-1}\)$, we first **sort** them in **descending** order:

# $y_0 \ge y_1 \ge\dots\ge y_{N-1}$

where each $\(y_i\)$ is one of the $\(x_r\)$. Then the **n-th largest** is:

# $\displaystyle y_{\,(n-1)}$

i.e., **the $\((n-1)\)$-th index** in the sorted (descending) array. If $\(n\)$ exceeds $\(N\)$ or no values are valid, a fallback value (e.g., 0.0) is returned.

## Usage:
```c
    DataFrame df;
    DataFrame_Create(&df);

    // DF_DOUBLE => [5, 10, 1, 9, 20]
    Series s;
    seriesInit(&s, "NthLarge", DF_DOUBLE);
    seriesAddDouble(&s,5);
    seriesAddDouble(&s,10);
    seriesAddDouble(&s,1);
    seriesAddDouble(&s,9);
    seriesAddDouble(&s,20);
    df.addSeries(&df, &s);
    seriesFree(&s);

    // sorted desc => [20,10,9,5,1]
    // nth largest(1) => 20
    // nth largest(3) => 9
    double l1 = df.nthLargest(&df,0,1);
    double l3 = df.nthLargest(&df,0,3);
    assert(l1==20.0);
    assert(l3==9.0);

    DataFrame_Destroy(&df);
```



# Aggregate::double nthSmallest(const DataFrame* df, size_t colIndex, size_t n)

Given a DataFrame `df`, a column index `colIndex`, and an integer `n`, the function returns the **n-th smallest** value in the column’s numeric data. If the column has $\(N\)$ valid entries $\(x_0, x_1, \dots, x_{N-1}\)$, we first **sort** them in **ascending** order:

# $y_0 \le y_1 \le\dots\le y_{N-1}$

where each $\(y_i\)$ is one of the $\(x_r\)$. Then the **n-th smallest** is:

# $\displaystyle y_{\,(n-1)}$

i.e., **the $\((n-1)\)$-th index** in the sorted (ascending) array. If $\(n\)$ exceeds $\(N\)$ or no values are valid, a fallback value (e.g., 0.0) is returned.


## Usage:
```c
    DataFrame df;
    DataFrame_Create(&df);

    // DF_INT => [10,3,5,7]
    Series s;
    seriesInit(&s, "NthSmall", DF_INT);
    seriesAddInt(&s,10);
    seriesAddInt(&s,3);
    seriesAddInt(&s,5);
    seriesAddInt(&s,7);
    df.addSeries(&df, &s);
    seriesFree(&s);

    // sorted ascending => [3,5,7,10]
    // 1st => 3, 2nd => 5
    double s1 = df.nthSmallest(&df,0,1);
    double s2 = df.nthSmallest(&df,0,2);
    assert(s1==3.0);
    assert(s2==5.0);

    DataFrame_Destroy(&df);
```



# Aggregate::double skewness(const DataFrame* df, size_t colIndex)

Given a DataFrame `df` and a column index `colIndex`, the function computes the **sample skewness** of the column’s numeric values. 

Consider the two distributions in the figure just below. Within each graph, the values on the right side of the distribution taper differently from the values on the left side. These tapering sides are called tails, and they provide a visual means to determine which of the two kinds of skewness a distribution has:

- negative skew: The left tail is longer; the mass of the distribution is concentrated on the right of the figure. The distribution is said to be left-skewed, left-tailed, or skewed to the left, despite the fact that the curve itself appears to be skewed or leaning to the right; left instead refers to the left tail being drawn out and, often, the mean being skewed to the left of a typical center of the data. A left-skewed distribution usually appears as a right-leaning curve.
- positive skew: The right tail is longer; the mass of the distribution is concentrated on the left of the figure. The distribution is said to be right-skewed, right-tailed, or skewed to the right, despite the fact that the curve itself appears to be skewed or leaning to the left; right instead refers to the right tail being drawn out and, often, the mean being skewed to the right of a typical center of the data. A right-skewed distribution usually appears as a left-leaning curve. https://en.wikipedia.org/wiki/Skewness
  
![image](https://github.com/user-attachments/assets/41d110a0-5465-4e47-b229-0bab92605fcb)

![image](https://github.com/user-attachments/assets/e700653c-b96a-487b-b2a5-6b2e7daf78b1)

where μ is the mean, σ is the standard deviation, E is the expectation operator, μ3 is the third central moment, and κt are the t-th cumulants. It is sometimes referred to as Pearson's moment coefficient of skewness,[5] or simply the moment coefficient of skewness,[4] but should not be confused with Pearson's other skewness statistics

## Usage:
```c
    DataFrame df;
    DataFrame_Create(&df);

    // simple DF_DOUBLE => [1,2,3,4,100] => known to have positive skew
    Series s;
    seriesInit(&s, "SkewTest", DF_DOUBLE);
    double arr[] = {1,2,3,4,100};
    for (int i=0; i<5; i++){
        seriesAddDouble(&s, arr[i]);
    }
    df.addSeries(&df, &s);
    seriesFree(&s);

    double sk = df.skewness(&df, 0);
    // We'll just check it's >0
    assert(sk>0.0);

    DataFrame_Destroy(&df);
```




# Aggregate::double kurtosis(const DataFrame* df, size_t colIndex)

Given a DataFrame `df` and a column index `colIndex`, the function computes the **kurtosis** of the columns numeric values.

![image](https://github.com/user-attachments/assets/8c094786-c2ab-45fa-afad-9ea59f5c66cc)

In probability theory and statistics, kurtosis (from Greek: κυρτός, kyrtos or kurtos, meaning "curved, arching") refers to the degree of “tailedness” in the probability distribution of a real-valued random variable. Similar to skewness, kurtosis provides insight into specific characteristics of a distribution. Various methods exist for quantifying kurtosis in theoretical distributions, and corresponding techniques allow estimation based on sample data from a population. It’s important to note that different measures of kurtosis can yield varying interpretations.

The standard measure of a distribution's kurtosis, originating with Karl Pearson,[1] is a scaled version of the fourth moment of the distribution. This number is related to the tails of the distribution, not its peak;[2] hence, the sometimes-seen characterization of kurtosis as "peakedness" is incorrect. For this measure, higher kurtosis corresponds to greater extremity of deviations (or outliers), and not the configuration of data near the mean.

The kurtosis is the fourth standardized moment, defined as:
![image](https://github.com/user-attachments/assets/c01d9ad4-bc70-49e8-a577-5992d4f96fc9)


## Usage:
```c
    DataFrame df;
    DataFrame_Create(&df);

    // DF_DOUBLE => [1,2,3,4,100] => typically has high kurtosis
    Series s;
    seriesInit(&s, "KurtTest", DF_DOUBLE);
    double arr[] = {1,2,3,4,100};
    for (int i=0; i<5; i++){
        seriesAddDouble(&s, arr[i]);
    }
    df.addSeries(&df, &s);
    seriesFree(&s);

    double kurt = df.kurtosis(&df, 0);
    // Check it's > 0. Typically big outlier => large positive kurt
    assert(kurt>0.0);

    DataFrame_Destroy(&df);
```



# Aggregate::double covariance(const DataFrame* df, size_t colIndex1, size_t colIndex2)

Given a DataFrame `df` and two column indices (`colIndex1` and `colIndex2`), the function computes the **sample covariance** between the numeric values in these two columns.

![image](https://github.com/user-attachments/assets/4992c491-dd4c-4aaa-93af-2da86909cd5d)

![image](https://github.com/user-attachments/assets/90f43f70-7681-4974-bf28-627e6db90644)


- $cov_{x,y}$	=	covariance between variable x and y
- $x_{i}$	=	data value of x
- $y_{i}$	=	data value of y
- $\bar{x}$	=	mean of x
- $\bar{y}$	=	mean of y
- $N$	=	number of data values

## Usage:
```c
    DataFrame df;
    DataFrame_Create(&df);

    // 2 columns => X=[1,2,3], Y=[2,4,6] => correlation=1 => covariance>0
    Series sx, sy;
    seriesInit(&sx, "CovX", DF_INT);
    seriesInit(&sy, "CovY", DF_INT);
    seriesAddInt(&sx,1);
    seriesAddInt(&sx,2);
    seriesAddInt(&sx,3);
    seriesAddInt(&sy,2);
    seriesAddInt(&sy,4);
    seriesAddInt(&sy,6);

    df.addSeries(&df, &sx);
    df.addSeries(&df, &sy);

    seriesFree(&sx);
    seriesFree(&sy);

    double cov = df.covariance(&df, 0,1);
    // Because Y=2X => perfect correlation => sample cov won't be 0 => let's just check >0
    assert(cov>0.0);

    DataFrame_Destroy(&df);
```



# Aggregate::double correlation(const DataFrame* df, size_t colIndexX, size_t colIndexY)

Given a DataFrame `df` and two column indices (`colIndexX`, `colIndexY`), the function computes the **Pearson correlation** between those columns’ numeric values.

![image](https://github.com/user-attachments/assets/e5134239-299c-4e6e-8931-abf2736eb224)

In statistics, the Pearson correlation coefficient (PCC)[a] is a correlation coefficient that measures linear correlation between two sets of data. It is the ratio between the covariance of two variables and the product of their standard deviations; thus, it is essentially a normalized measurement of the covariance, such that the result always has a value between −1 and 1. As with covariance itself, the measure can only reflect a linear correlation of variables, and ignores many other types of relationships or correlations.

![image](https://github.com/user-attachments/assets/d8a5b131-6929-4d92-88ab-9e3589b255f7)


- $r$	=	correlation coefficient
- $x_{i}$	=	values of the x-variable in a sample
- $\bar{x}$	=	mean of the values of the x-variable
- $y_{i}$	=	values of the y-variable in a sample
- $\bar{y}$	=	mean of the values of the y-variable



## Usage:
```c
    DataFrame df;
    DataFrame_Create(&df);

    // X=[10,20,30], Y=[20,40,60] => perfect correlation => correlation ~1
    Series sx, sy;
    seriesInit(&sx, "CorrX", DF_INT);
    seriesInit(&sy, "CorrY", DF_INT);

    seriesAddInt(&sx,10);
    seriesAddInt(&sx,20);
    seriesAddInt(&sx,30);

    seriesAddInt(&sy,20);
    seriesAddInt(&sy,40);
    seriesAddInt(&sy,60);

    df.addSeries(&df, &sx);
    df.addSeries(&df, &sy);
    seriesFree(&sx);
    seriesFree(&sy);

    double corr = df.correlation(&df, 0,1);
    // should be near 1
    assertAlmostEqual(corr,1.0,1e-5);

    DataFrame_Destroy(&df);
```



# Aggregate::DataFrame uniqueValues(const DataFrame* df, size_t colIndex)

Given a DataFrame `df` and a column index `colIndex`, the function **creates a new DataFrame** containing only the **distinct values** from that column.
![uniqueValues](diagrams/uniqueValues.png "uniqueValues")

## Usage:
```c
    DataFrame df;
    DataFrame_Create(&df);

    // DF_INT => [2,2,5,7,5]
    Series s;
    seriesInit(&s, "UniqueValTest", DF_INT);
    seriesAddInt(&s,2);
    seriesAddInt(&s,2);
    seriesAddInt(&s,5);
    seriesAddInt(&s,7);
    seriesAddInt(&s,5);
    df.addSeries(&df,&s);
    seriesFree(&s);

    DataFrame uniqueDF = df.uniqueValues(&df, 0);
    // distinct => {2,5,7} => we expect 3 rows in uniqueDF
    size_t rowCount = uniqueDF.numRows(&uniqueDF);
    assert(rowCount==3);

    // We won't check the exact order. Just check the total.
    DataFrame_Destroy(&uniqueDF);
    DataFrame_Destroy(&df);
```



# Aggregate::DataFrame valueCounts(const DataFrame* df, size_t colIndex)

Given a DataFrame `df` and a column index `colIndex`, the **valueCounts** function returns a new DataFrame listing each **distinct value** in that column along with its **frequency**.

![valueCounts](diagrams/valueCounts.png "valueCounts")



## Usage:
```c
    DataFrame df;
    DataFrame_Create(&df);

    // DF_STRING => ["apple", "apple", "banana"]
    Series s;
    seriesInit(&s, "VCtest", DF_STRING);
    seriesAddString(&s, "apple");
    seriesAddString(&s, "apple");
    seriesAddString(&s, "banana");
    df.addSeries(&df, &s);
    seriesFree(&s);

    DataFrame vc = df.valueCounts(&df, 0);
    // Expect 2 distinct => "apple" (2), "banana"(1)
    // We'll just check numRows=2
    size_t rowCount = vc.numRows(&vc);
    assert(rowCount==2);

    DataFrame_Destroy(&vc);
    DataFrame_Destroy(&df);
```



# Aggregate::DataFrame cumulativeSum(const DataFrame* df, size_t colIndex)

It creates a new column that, for each row, holds the running total (sum) of all previous rows (including the current row).

![cumSum](diagrams/cumSum.png "cumSum")


## Usage:
```c
    DataFrame df;
    DataFrame_Create(&df);

    // DF_DOUBLE => [1.0, 2.0, 3.0]
    Series s;
    seriesInit(&s, "CumSumTest", DF_DOUBLE);
    seriesAddDouble(&s,1.0);
    seriesAddDouble(&s,2.0);
    seriesAddDouble(&s,3.0);
    df.addSeries(&df,&s);
    seriesFree(&s);

    DataFrame cs = df.cumulativeSum(&df, 0);
    // col "cumsum" => [1.0, 3.0, 6.0]
    const Series* csumCol = cs.getSeries(&cs, 0);
    assert(csumCol && csumCol->type==DF_DOUBLE);

    double v0,v1,v2;
    seriesGetDouble(csumCol, 0, &v0);
    seriesGetDouble(csumCol, 1, &v1);
    seriesGetDouble(csumCol, 2, &v2);
    assertAlmostEqual(v0,1.0,1e-9);
    assertAlmostEqual(v1,3.0,1e-9);
    assertAlmostEqual(v2,6.0,1e-9);

    DataFrame_Destroy(&cs);
    DataFrame_Destroy(&df);
```



# Aggregate::DataFrame cumulativeProduct(const DataFrame* df, size_t colIndex)
For each row in a numeric column, cumulative product stores the running product of all previous values (including the current one)
![cumProduct](diagrams/cumProduct.png "cumProduct")


## Usage:
```c
    DataFrame df;
    DataFrame_Create(&df);

    // DF_INT => [2,2,3]
    Series s;
    seriesInit(&s, "CumProdTest", DF_INT);
    seriesAddInt(&s,2);
    seriesAddInt(&s,2);
    seriesAddInt(&s,3);
    df.addSeries(&df,&s);
    seriesFree(&s);

    DataFrame cp = df.cumulativeProduct(&df,0);
    // expect => [2,4,12]
    const Series* cprodCol = cp.getSeries(&cp, 0);
    double v0,v1,v2;
    seriesGetDouble(cprodCol,0,&v0);
    seriesGetDouble(cprodCol,1,&v1);
    seriesGetDouble(cprodCol,2,&v2);
    assertAlmostEqual(v0,2.0,1e-9);
    assertAlmostEqual(v1,4.0,1e-9);
    assertAlmostEqual(v2,12.0,1e-9);

    DataFrame_Destroy(&cp);
    DataFrame_Destroy(&df);
```


# Aggregate::DataFrame cumulativeMax(const DataFrame* df, size_t colIndex)

The cumulative max at row 𝑖 is the largest value seen so far (from row 0 up to row 𝑖)

![cumMax](diagrams/cumMax.png "cumMax")


## Usage:
```c
    DataFrame df;
    DataFrame_Create(&df);

    // DF_INT => [1,3,2,5]
    Series s;
    seriesInit(&s, "CumMaxTest", DF_INT);
    seriesAddInt(&s,1);
    seriesAddInt(&s,3);
    seriesAddInt(&s,2);
    seriesAddInt(&s,5);
    df.addSeries(&df,&s);
    seriesFree(&s);

    DataFrame cm = df.cumulativeMax(&df,0);
    // row0 =>1, row1=>3, row2=>3, row3=>5
    const Series* cmaxCol = cm.getSeries(&cm,0);
    double v0,v1,v2,v3;
    seriesGetDouble(cmaxCol,0,&v0);
    seriesGetDouble(cmaxCol,1,&v1);
    seriesGetDouble(cmaxCol,2,&v2);
    seriesGetDouble(cmaxCol,3,&v3);
    assertAlmostEqual(v0,1.0,1e-9);
    assertAlmostEqual(v1,3.0,1e-9);
    assertAlmostEqual(v2,3.0,1e-9);
    assertAlmostEqual(v3,5.0,1e-9);

    DataFrame_Destroy(&cm);
    DataFrame_Destroy(&df);
```


# Aggregate::DataFrame cumulativeMin(const DataFrame* df, size_t colIndex)

The cumulative max at row 𝑖 is the smallest value seen so far (from row 0 up to row 𝑖)

![cumMin](diagrams/cumMin.png "cumMin")

## Usage:
```c
    DataFrame df;
    DataFrame_Create(&df);

    // DF_INT => [3,2,5,1]
    Series s;
    seriesInit(&s, "CumMinTest", DF_INT);
    seriesAddInt(&s,3);
    seriesAddInt(&s,2);
    seriesAddInt(&s,5);
    seriesAddInt(&s,1);
    df.addSeries(&df,&s);
    seriesFree(&s);

    DataFrame cmi = df.cumulativeMin(&df,0);
    // row0=>3, row1=>2, row2=>2, row3=>1
    const Series* cminCol = cmi.getSeries(&cmi,0);
    double v0,v1,v2,v3;
    seriesGetDouble(cminCol,0,&v0);
    seriesGetDouble(cminCol,1,&v1);
    seriesGetDouble(cminCol,2,&v2);
    seriesGetDouble(cminCol,3,&v3);
    assertAlmostEqual(v0,3.0,1e-9);
    assertAlmostEqual(v1,2.0,1e-9);
    assertAlmostEqual(v2,2.0,1e-9);
    assertAlmostEqual(v3,1.0,1e-9);

    DataFrame_Destroy(&cmi);
    DataFrame_Destroy(&df);
```


# Aggregate::DataFrame groupBy(const DataFrame* df, size_t groupColIndex)

Given a DataFrame `df` and a column index `colIndex`, the **groupBy** function returns a new DataFrame listing each **distinct value** in that column along with its **frequency**.

![groupBy](diagrams/groupBy.png "groupBy")

## Usage:
```c
    DataFrame df;
    DataFrame_Create(&df);

    // We'll store DF_STRING => ["apple","banana","banana","apple"]
    // so group => "apple"(2), "banana"(2)
    Series s;
    seriesInit(&s, "Fruit", DF_STRING);
    seriesAddString(&s,"apple");
    seriesAddString(&s,"banana");
    seriesAddString(&s,"banana");
    seriesAddString(&s,"apple");
    df.addSeries(&df,&s);
    seriesFree(&s);

    DataFrame g = df.groupBy(&df,0);
    // We expect 2 rows => group => "apple", "banana"
    size_t r = g.numRows(&g);
    assert(r==2);

    // Also might check the "count" column => each should be 2
    // We'll do minimal here
    DataFrame_Destroy(&g);
    DataFrame_Destroy(&df);
```













