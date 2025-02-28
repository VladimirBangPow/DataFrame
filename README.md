# DataFrame

## Core:

### void Dataframe_Create(DataFrame* df)
![Create](diagrams/Create.png "Create")


### bool df.addSeries(DataFrame* df, const Series* s)

![AddSeries](diagrams/addSeries.png "AddSeries")



### const Series* df.getSeries(const DataFrame* df, size_t colIndex)
![GetSeries](diagrams/GetSeries.png "GetSeries")


### size_t df.numColumns(const DataFrame* df)
![NColumns](diagrams/NColumns.png "NColumns")



### bool df.addRow(DataFrame* df, const void** rowData)
![AddRow](diagrams/AddRow.png "AddRow")



### bool df.getRow(DataFrame* df, size_t rowIndex, void** outRow)
![GetRow](diagrams/GetRow.png "GetRow")


### size_t df.numRows(const DataFrame *df)
![NRows](diagrams/NRows.png "NRows")
