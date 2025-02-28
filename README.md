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

# Date::convertDatesToEpoch(DataFrame* df, size_t dateColIndex, const char* formatType, bool toMillis)
| **Original Column Type** | **Example Cell Value (input)** | **formatType**                 | **toMillis?** | **Action**                                                                                    | **Final Column Type**        | **Example Output Value**                                               |
|--------------------------|--------------------------------|--------------------------------|--------------|------------------------------------------------------------------------------------------------|------------------------------|---------------------------------------------------------------------------|
| **DF_INT**              | `20230301` (integer)           | `"YYYYMMDD"`                   | `false`      | Interprets `20230301` as “YYYY=2023, MM=03, DD=01” → parse → epoch seconds                     | **DF_INT** (overwritten)     | e.g. `1677628800` (seconds)                                            |
| **DF_INT**              | `20230301` (integer)           | `"YYYYMMDD"`                   | `true`       | Same parse, but multiply final by 1000 for ms                                                  | **DF_INT** (overwritten)     | e.g. `1677628800000` (milliseconds; watch for potential overflow)      |
| **DF_DOUBLE**           | `1.677628456e9`                | `"unix_seconds"`               | `false`      | Interprets numeric as `1677628456.0` → store as epoch seconds in the same column               | **DF_DOUBLE** (overwritten)  | e.g. `1677628456.0`                                                    |
| **DF_DOUBLE**           | `1.677628456e12`               | `"unix_millis"`                | `false`      | Interprets numeric as ms → divides by 1000 ⇒ epoch in seconds                                  | **DF_DOUBLE** (overwritten)  | e.g. `1677628456.0`                                                    |
| **DF_STRING**           | `"20230301"`                    | `"YYYYMMDD"`                   | `false`      | Convert string → numeric (20230301.0) → parse → epoch                                          | **DF_DATETIME** (64-bit int) | e.g. `1677628800` (epoch in seconds)                                   |
| **DF_STRING**           | `"20230301"`                    | `"YYYYMMDD"`                   | `true`       | Same parse, but multiply final by 1000                                                         | **DF_DATETIME** (64-bit int) | e.g. `1677628800000`                                                   |
| **DF_STRING**           | `"1677628456"`                  | `"unix_seconds"`               | `false`      | Interpret as seconds → store as epoch                                                          | **DF_DATETIME** (64-bit int) | `1677628456`                                                           |
| **DF_STRING**           | `"1677628456123"`               | `"unix_millis"`                | `false`      | Divide by 1000 ⇒ `1677628456` seconds → store                                                 | **DF_DATETIME** (64-bit int) | `1677628456`                                                           |
| **DF_STRING**           | `"2023-03-01 12:34:56"`         | `"%Y-%m-%d %H:%M:%S"`          | `false`      | `strptime` parse → epoch in seconds (UTC or local)                                             | **DF_DATETIME** (64-bit int) | e.g. `1677673696`                                                      |
| **DF_STRING**           | `"2023-03-01 12:34:56"`         | `"%Y-%m-%d %H:%M:%S"`          | `true`       | Same parse, then multiply final epoch by 1000 for ms                                           | **DF_DATETIME** (64-bit int) | e.g. `1677673696000`                                                   |
| **Not Numeric or String**| (any value)                    | (any)                          | (any)        | Logs error (“not numeric or string”) & returns false                                           | *N/A*                         | *N/A*                                                                   |
