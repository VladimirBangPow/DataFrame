# DataFrame::Core


# Core::void Dataframe_Create(DataFrame* df)
![Create](diagrams/Create.png "Create")


# Core::bool df.addSeries(DataFrame* df, const Series* s)

![AddSeries](diagrams/addSeries.png "AddSeries")



# Core::const Series* df.getSeries(const DataFrame* df, size_t colIndex)
![GetSeries](diagrams/GetSeries.png "GetSeries")


# Core::size_t df.numColumns(const DataFrame* df)
![NColumns](diagrams/NColumns.png "NColumns")



# Core::bool df.addRow(DataFrame* df, const void** rowData)
![AddRow](diagrams/AddRow.png "AddRow")



# Core::bool df.getRow(DataFrame* df, size_t rowIndex, void** outRow)
![GetRow](diagrams/GetRow.png "GetRow")


# Core::size_t df.numRows(const DataFrame *df)
![NRows](diagrams/NRows.png "NRows")




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
