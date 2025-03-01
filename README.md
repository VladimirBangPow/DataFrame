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

# Date::bool convertToDatetime(DataFrame* df, size_t dateColIndex, const char* formatType, bool toMillis)
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


# Date::bool datetimeToString(DataFrame* df, size_t dateColIndex, const char* outFormat)
| **DF_DATETIME Cell** (msVal)     | **Seconds** (msVal/1000) | **Remainder** (msVal % 1000) | **Output (assuming outFormat=\"%Y-%m-%d %H:%M:%S\")**                                                      | **Notes**                                                                                                        |
|----------------------------------|--------------------------|------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------|
| `0`                              | `0`                      | `0`                          | `"1970-01-01 00:00:00"`                                                                                                                           | No fractional appended since remainder = 0.                                                                     |
| `1678882496000`                  | `1678882496`            | `0`                          | `"2023-03-15 12:34:56"`                                                                                                                           | `gmtime(1678882496)` => March 15, 2023 12:34:56 UTC. No fractional.                                             |
| `1678882496123`                  | `1678882496`            | `123`                        | `"2023-03-15 12:34:56.123"`                                                                                                                       | Same date/time as above, plus `.123` appended because remainder=123.                                            |
| `9999999999999999999` (overflow)| (overflow if too large)  | (overflow)                   | `""` (empty)                                                                                                                                        | If `gmtime` fails due to out-of-range `time_t`, we store an empty string.                                        |
| *Any valid msVal but `gmtime` fails internally* | *N/A*            | *N/A*                      | `""`                                                                                                                                                | If `gmtime` returns `NULL`, we store an empty string.                                                            |

# Date:bool datetimeAdd(DataFrame* df, size_t dateColIndex, int daysToAdd)
| **DF_DATETIME Cell** (msVal)     | **daysToAdd** | **Seconds** (msVal/1000)   | **After Adding Days**                | **New Stored Value (ms)**                                                                                          | **Notes**                                                                                                                                           |
|----------------------------------|--------------|----------------------------|--------------------------------------|--------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| `0`                              | `1`          | `0`                        | `1970-01-02 00:00:00 UTC` → `86400s` | `86400000`                                                                                                          | If we start at 1970-01-01 00:00:00 and add 1 day, we get epoch 86400 sec → stored as 86400000 ms.                                                   |
| `1678871696000`                 | `1`          | `1678871696`              | Adds 1 day → `1678958096s`          | `1678958096000`                                                                                                    | Example: ~2023-03-15 12:34:56 UTC + 1 day => ~2023-03-16 12:34:56 → epoch 1678958096 sec → stored as 1678958096000 ms.                               |
| `1678838400000`                 | `-2`         | `1678838400`              | Subtract 2 days → `1678665600s`      | `1678665600000`                                                                                                    | If original was “2023-03-15 00:00:00 UTC,” then subtracting 2 days => `2023-03-13 00:00:00 UTC` → 1678665600 sec → 1678665600000 ms.                |
| `9999999999999999999` (very big)| `10`         | Possibly overflow if > `time_t` | If `timegm` fails => 0 fallback  | `0`                                                                                                                | If the new date is beyond representable range, we set newSec=0. Final stored ms => 0.                                                               |
| *Any msVal but `gmtime` fails*   | *any*        | *N/A*                      | *N/A*                                | `0`                                                                                                                | If `gmtime` returns NULL, we do `newSec=0; newMs=0`.                                                                                                |



# Date::DataFrame datetimeDiff(const DataFrame* df, size_t col1Index, size_t col2Index, const char* newColName)

| **col1 (ms)**       | **col2 (ms)**       | **Raw Difference** (`ms2 - ms1`) | **Result (seconds)** `(ms2 - ms1)/1000` | **Stored in DF_INT** | **Notes**                                                                                                 |
|---------------------|---------------------|----------------------------------|------------------------------------------|-----------------------|-----------------------------------------------------------------------------------------------------------|
| `0`                 | `0`                 | `0`                              | `0`                                      | `0`                   | If both columns have epoch=0 ms, difference is 0 sec.                                                     |
| `1000`              | `2000`              | `1000`                           | `1`                                      | `1`                   | 1000 ms => 1 second difference.                                                                           |
| `1678882496000`     | `1678882497000`     | `1000`                           | `1`                                      | `1`                   | If col1= "2023-03-15 12:34:56.000" and col2= "2023-03-15 12:34:57.000", difference=1s.                    |
| `5000`              | `2000`              | `-3000`                          | `-3`                                     | `-3`                  | Negative difference if col2 < col1. The DF_INT cell will store -3.                                        |
| `9999999999999`     | `10000000000000`    | `1` (×10³ difference in ms)      | `1` (×10³ / 1000 = 1)                    | `1`                   | Large but valid. If the difference fits an `int`, we store that.                                          |
| *missing or invalid*| *missing or invalid*| *not read => fallback*           | `0`                                      | `0`                   | If either cell can’t be read via `seriesGetDateTime`, we store 0.                                         |

# Date::DataFrame datetimeFilter(const DataFrame* df, size_t dateColIndex, long long startMs, long long endMs)
| **Column Type** | **Row Value** (`msVal`) | **`startMs`**         | **`endMs`**           | **Filter Condition**                  | **Included in Result?**                         |
|-----------------|-------------------------|------------------------|-----------------------|---------------------------------------|-------------------------------------------------|
| `DF_DATETIME`   | `0`                    | `0`                    | `9999999999`         | `0 >= 0 && 0 <= 9999999999`           | **Yes** (0 is in [0..9999999999])              |
| `DF_DATETIME`   | `2000`                 | `1000`                 | `3000`               | `2000 >= 1000 && 2000 <= 3000`        | **Yes** (2000 is in [1000..3000])              |
| `DF_DATETIME`   | `4000`                 | `1000`                 | `3000`               | `4000 >= 1000 && 4000 <= 3000`        | **No** (4000 > 3000)                           |
| `DF_DATETIME`   | `1678882496000`        | `1678880000000`       | `1679000000000`      | `1678882496000 >= 1678880000000 && 1678882496000 <= 1679000000000` | **Yes** (it's within the provided ms range)    |
| `DF_DATETIME`   | `9999999999999999`     | `0`                    | `9999999999999999`   | `9999999999999999 >= 0 && 9999999999999999 <= 9999999999999999`    | **Yes** (it’s exactly the upper boundary)       |
| `DF_DATETIME`   | *Invalid or missing*    | *Any*                 | *Any*                | *Cannot read msVal => default false*  | **Excluded** (if `seriesGetDateTime` fails)    |

# Date:: bool datetimeTruncate(DataFrame* df, size_t colIndex, const char* unit)
| **DF_DATETIME Cell** (msVal)   | **Unit**     | **Initial UTC Date/Time**       | **Zeroed-Out Fields**                          | **New UTC Date/Time**                          | **Final Stored Value (ms)**                                  | **Notes**                                                                                                           |
|--------------------------------|-------------|---------------------------------|------------------------------------------------|------------------------------------------------|----------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------|
| `1678871696000` (e.g. ~2023-03-15 12:34:56) | `"day"`     | 2023-03-15 12:34:56 UTC        | Hour=0, Min=0, Sec=0                           | 2023-03-15 00:00:00 UTC                        | `1678838400000`                                              | Eliminates partial hours, minutes, seconds => start of that day in UTC.                                             |
| `1678871696000`               | `"hour"`    | 2023-03-15 12:34:56 UTC         | Min=0, Sec=0                                   | 2023-03-15 12:00:00 UTC                        | `1678872000000`                                              | Truncates to start of hour => 12:00:00.                                                                             |
| `1678871696000`               | `"month"`   | 2023-03-15 12:34:56 UTC         | Day=1, Hour=0, Min=0, Sec=0                    | 2023-03-01 00:00:00 UTC                        | `1677628800000`                                              | Moves to first day of that month => March 1, 2023, midnight UTC.                                                    |
| `1678871696000`               | `"year"`    | 2023-03-15 12:34:56 UTC         | Month=0 (Jan), Day=1, Hour=0, Min=0, Sec=0      | 2023-01-01 00:00:00 UTC                        | `1672531200000`                                              | Zero out month=January, day=1 => start of the year.                                                                 |
| `0`                            | `"day"`     | 1970-01-01 00:00:00 UTC         | (Already 0:00:00)                              | 1970-01-01 00:00:00 UTC                        | `0`                                                          | If it’s already midnight epoch, no change.                                                                          |
| `9999999999999999999`         | `"month"`   | Very large => `gmtime` might fail| If `gmtime` fails => skip or fallback to 0      | Possibly 0 if out of range                     | `0`                                                          | If date is out-of-range for `timegm`, set to 0.                                                                     |
| *Invalid row*                  | *any*       | *Cannot read msVal*            | *No operation performed*                        | *No change or fallback*                        | Possibly unchanged or 0                                      | If `seriesGetDateTime` fails, we skip that row.                                                                     |

# Date::DataFrame datetimeExtract(const DataFrame* df, size_t dateColIndex, const char* const* fields, size_t numFields)
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


# Date::DataFrame datetimeGroupBy(const DataFrame* df, size_t dateColIndex, const char* truncateUnit)
| **Original DataFrame**                           | **`dateColIndex`** | **`truncateUnit`** | **Steps**                                                                                                                             | **Final Returned DataFrame**                                    | **Notes**                                                                                                                                                  |
|--------------------------------------------------|--------------------|--------------------|----------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------|
| A DF_DATETIME column in ms (e.g., storing 2023-03-15 12:34:56, 2023-03-15 14:10:00, 2023-03-16 00:00:00, etc.). Other columns as well. | e.g. `1`          | `"day"`            | 1) **Slice** all rows → `copyAll` <br/> 2) **Truncate** that DF’s `dateColIndex` to `"day"` → zero out hour/min/sec => e.g. 2023-03-15 00:00:00, 2023-03-16 00:00:00, etc. <br/> 3) **GroupBy** that truncated column. | A new grouped DataFrame, typically with columns like `["group", "count"]` or more depending on your `groupBy` design. | Often yields fewer rows if multiple datetimes collapse to the same truncated day (or hour/month/year).                                                      |
| A DF_DATETIME column in ms, *but no rows*        | e.g. `0`          | `"month"`          | 1) Slice => an empty DataFrame (since numRows=0) <br/> 2) Truncation + GroupBy on an empty set => results in an empty DF as well.      | **Empty** DataFrame with 0 rows                                   | If `copyAll.numRows(...)` is 0, we just return that empty DataFrame.                                                                                        |
| DF_DATETIME w/ partial times => `truncateUnit="year"` | `2`              | `"year"`           | 1) Slice => copyAll <br/> 2) `dfDatetimeTruncate_impl(...,"year")` => sets month=0, day=1, etc. <br/> 3) groupBy that year-level date  | Possibly columns like `[TruncatedDate, otherAggregations?]` depending on groupBy output | All times in the same year now become the *same* group if they share the same truncated year (like 2023-01-01 00:00:00).                                  |
| DF_DATETIME w/ massive out-of-range or invalid rows | any index        | any unit ("hour") | 1) Slicing includes them <br/> 2) Truncation might set them to 0 if `gmtime` fails <br/> 3) groupBy lumps all invalid => 1970-01-01  | A grouped DF, possibly including a `1970-01-01 00:00:00` group for those out-of-range.   | If `timegm` fails, the truncated ms => 0 => they appear in the group for “1970-01-01 00:00:00.”                                                           |


# Date::DataFrame datetimeValidate(const DataFrame* df, size_t colIndex, long long minMs, long long maxMs)
| **DF_DATETIME Cell** (`msVal`) | **`minMs`**               | **`maxMs`**               | **Condition**                                               | **Included in Result?**                            | **Notes**                                                                                                                                   |
|--------------------------------|---------------------------|---------------------------|-------------------------------------------------------------|-----------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------|
| `0`                            | `0`                       | `999999999999`           | `0 >= 0 && 0 <= 999999999999`                              | **Yes**                                              | A row at epoch 1970-01-01 00:00:00 is within `[0..999999999999]`.                                                                             |
| `2000`                         | `1000`                    | `3000`                    | `2000 >= 1000 && 2000 <= 3000`                             | **Yes**                                              | If 2000 ms (i.e., 2 seconds) is in `[1 second..3 seconds]`.                                                                                   |
| `4000`                         | `1000`                    | `3000`                    | `4000 >= 1000 && 4000 <= 3000` → **false** (4000 > 3000)    | **No**                                               | A row with ms=4000 is outside `[1000..3000]`, so it’s excluded.                                                                              |
| `1678882496000`               | `1678838400000`           | `1679000000000`          | `1678882496000 >= 1678838400000 && 1678882496000 <= 1679000000000` | **Yes**                                              | Roughly 2023-03-15 12:34:56 UTC in ms is within that bounding range.                                                                          |
| `9999999999999`               | `0`                       | `9999999999999`          | `9999999999999 >= 0 && 9999999999999 <= 9999999999999` → true | **Yes**                                              | Exactly at upper boundary.                                                                                                                   |
| `10000000000000`              | `0`                       | `9999999999999`          | `10000000000000 >= 0 && 10000000000000 <= 9999999999999` → false | **No**                                               | Exceeds `maxMs`, excluded.                                                                                                                   |
| *Invalid cell*                 | *any*                     | *any*                     | *cannot read msVal => filter defaults false*                | **No**                                               | If `seriesGetDateTime` fails, the row is excluded.                                                                                           |
