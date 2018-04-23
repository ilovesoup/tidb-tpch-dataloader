## CSV data transferer

Convert your CSV-like file to mydumper SQL files fast.

### Build
`mvn clean package`

### Usage
```bash
usage: java -jar
            tidb-tpch-dataloader-1.0-SNAPSHOT-jar-with-dependencies.jar
            [option]<arg>
TPCH data transformer - CSV format to mydumper SQL files.
 -chunkFileSize <arg>     Split tables into chunks of this output file
                          size. This value is in MB.
 -dbName <arg>            Database name:tpch/tpch_idx
 -format <arg>            Your csv format(like 'csv', 'tbl', etc.)
 -help                    Print this help
 -literalNullCols <arg>   Columns to insert literal null instead of quoted
                          "null"(filename1:[col1, col2, ...]#filename2...)
                          eg. "lineitem.csv:[1,2]#user.csv:[3,5]"
 -outputDir <arg>         Directory where the transformed sql files will
                          be placed in.
 -readers <arg>           Reader thread count.(one thread per file)
 -rowCount <arg>          How many rows per `INSERT INTO` statement.
 -separator <arg>         Defines how csv file is separated: 0 for '|' 1
                          for ',' 2 for '
                          '(tab)
 -tpchDir <arg>           Directory where you place your data file.
 -writers <arg>           Writer thread count.(one thread per file)

```

Example:
```bash
java -jar tidb-tpch-dataloader-1.0-SNAPSHOT-jar-with-dependencies.jar -outputDir "/home/novemser/testLoader" -tpchDir "/home/novemser/Documents/Code/Java/tidb-tpch-dataloader/tpch-dbgen/" -separator "0" -format "tbl" -literalNullCols "lineitem:[1,2,3]#orders:[3,5]"
```