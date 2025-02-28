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

# Date::convertToDatetime(DataFrame* df, size_t dateColIndex, const char* formatType, bool toMillis)
| **Initial Column Type** | **Example Cell Value**                       | **`formatType`**               | **`toMillis`** | **Action**                                                                                               | **Final Column Type** | **Final Stored Value (DF_DATETIME)**                                       |
|-------------------------|----------------------------------------------|--------------------------------|---------------|-----------------------------------------------------------------------------------------------------------|------------------------|--------------------------------------------------------------------------------|
| **DF_INT**             | `20230301` (integer)                         | `YYYYMMDD`                     | `false`       | - Convert `20230301` → string: `"20230301"`<br/>- parse as March 1, 2023 → epoch (e.g. `1677628800`)      | **DF_DATETIME**       | `1677628800` (seconds since epoch)                                         |
| **DF_INT**             | `20230301` (integer)                         | `YYYYMMDD`                     | `true`        | Same parse => `1677628800` then multiply by 1000 → `1677628800000`                                       | **DF_DATETIME**       | `1677628800000` (milliseconds)                                             |
| **DF_INT**             | `1677628800` (integer)                       | `unix_seconds`                 | `false`       | - Convert `1677628800` → `"1677628800"`<br/>- parse as raw epoch → same integer                           | **DF_DATETIME**       | `1677628800`                                                                |
| **DF_DOUBLE**          | `1.6776288e9` (approx)                       | `unix_seconds`                 | `true`        | - Convert double to string `"1677628800"` → parse epoch => `1677628800` → multiply 1000 = `1677628800000` | **DF_DATETIME**       | `1677628800000`                                                              |
| **DF_DOUBLE**          | `1.6776288e12` (approx)                      | `unix_millis`                  | `false`       | - Convert `"1.6776288e12"` → parse → `1.6776288e12 / 1000` = ~`1677628800` seconds                        | **DF_DATETIME**       | `1677628800`                                                                |
| **DF_STRING**          | `"20230301"`                                 | `YYYYMMDD`                     | `false`       | - Already string => parse `"20230301"` → March 1, 2023 => epoch ~ `1677628800`                            | **DF_DATETIME**       | `1677628800`                                                                |
| **DF_STRING**          | `"1677628800"`                               | `unix_seconds`                 | `false`       | - parse as raw epoch => `1677628800`                                                                       | **DF_DATETIME**       | `1677628800`                                                                |
| **DF_STRING**          | `"1677628800000"`                            | `unix_millis`                  | `false`       | - parse => `1677628800000 / 1000` = `1677628800`                                                          | **DF_DATETIME**       | `1677628800`                                                                |
| **DF_STRING**          | `"2023-03-01 12:34:56"`                      | `"%Y-%m-%d %H:%M:%S"`          | `false`       | - `strptime("2023-03-01 12:34:56", ...)` → epoch (UTC or local) e.g. `1677673696`                          | **DF_DATETIME**       | `1677673696`                                                                |
| **DF_STRING**          | `"2023-03-01 12:34:56"`                      | `"%Y-%m-%d %H:%M:%S"`          | `true`        | - Same parse → `1677673696` → multiply by 1000 => `1677673696000`                                         | **DF_DATETIME**       | `1677673696000`                                                             |
| **Any**                | *(invalid or empty value)*                   | *(any)*                        | (any)         | - parseEpochSec returns `0` => store `0`                                                                  | **DF_DATETIME**       | `0` (1970-01-01 00:00:00)                                                  |
| **Unsupported**        | *(any)*                                     | *(unrecognized format)*        | (any)         | - parseEpochSec => `0` => final => `0`                                                                    | **DF_DATETIME**       | `0`                                                                          |
