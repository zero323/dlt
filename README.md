<img alt="dlt logo" src="man/figures/dlt.png" width="15%" align="right" />

# dlt ‒ Delta Lake interface for SparkR

## Installation

This package can be installed from the main git repository

```r
remotes::install_gitlab("zero323/dlt")
```

or its GitHub mirror

```r
remotes::install_github("zero323/dlt")
```

and requires following R packages:

- `SparkR (>= 3.1.0)`
- `magrittr`


Additionally, you'll have to ensure that a compatible Delta Lake jar is available,
for example by adding `delta-core` to `spark.jars.packages`:

```
spark.jars.packages 		io.delta:delta-core_2.12:1.0.0
```

## Usage

This package provides:

- Readers and writers for Delta format.
- DeltaTable merge API.
- Delta table builder API. 

### Batch reads and writes

`dlt_read` and `dlt_write` can be used to read and write data in Delta format.

```r
library(dlt)

target %>% 
  printSchema()

# root
#  |-- id: integer (nullable = true)
#  |-- key: string (nullable = true)
#  |-- val: integer (nullable = true)
#  |-- ind: integer (nullable = true)
#  |-- category: string (nullable = true)
#  |-- lat: double (nullable = true)
#  |-- long: double (nullable = true)

target %>% 
  dlt_write("/tmp/target")

dlt_read("/tmp/target") %>% 
  showDF(5)

# +---+---+---+---+--------+-------------------+-------------------+
# | id|key|val|ind|category|                lat|               long|
# +---+---+---+---+--------+-------------------+-------------------+
# |  1|  a|  4| -1|     KBQ| -56.28354165237397|-108.74080670066178|
# |  2|  a| 10|  1|     ROB| 50.546925463713706|-104.60825988091528|
# |  3|  a|  7| -1|     SLX|-13.985343240201473|-114.89280310459435|
# |  4|  a|  5|  1|     ACP| -47.15050248429179|-168.96175763569772|
# |  5|  b|  3| -1|     EEK|-49.020595396868885|-105.57821027934551|
# +---+---+---+---+--------+-------------------+-------------------+
# only showing top 5 rows
```

These also come with aliases following SparkR conventions - `read.delta` and `write.delta`.

```r
source %>%
  printSchema()

# root
#  |-- id: integer (nullable = true)
#  |-- key: string (nullable = true)
#  |-- val: integer (nullable = true)
#  |-- ind: integer (nullable = true)
#  |-- category: string (nullable = true)
#  |-- lat: double (nullable = true)
#  |-- long: double (nullable = true)

source %>%
  write.delta("/tmp/source")

read.delta("/tmp/source") %>% 
  showDF(5)

# +---+---+---+---+--------+------------------+-------------------+
# | id|key|val|ind|category|               lat|               long|
# +---+---+---+---+--------+------------------+-------------------+
# |  1|  a|  1|  1|     NTD| 72.72564971353859|  5.116242365911603|
# |  3|  b|  5|  1|     RSL|-65.03216980956495| -39.52675184234977|
# |  5|  b|  1| -1|     SYG| 88.00051120575517| 146.06572712771595|
# | 14|  c|  9| -1|     MYZ| 80.40028186049312|-19.090933883562684|
# | 16|  d| 10| -1|     DMO|-75.16123954206705| 120.96153359860182|
# +---+---+---+---+--------+------------------+-------------------+
```

### Loading DeltaTable objects

`DataTable` objects can be created for file system path:


```r
dlt_for_path("/tmp/target/") %>%
  dlt_to_df()

# SparkDataFrame[id:int, key:string, val:int, ind:int, category:string, lat:double, long:double]
```

or for the table name:

```r
source %>% 
  saveAsTable("source", source="delta")

dlt_for_name("source")  %>%
  dlt_to_df()

# SparkDataFrame[id:int, key:string, val:int, ind:int, category:string, lat:double, long:double]
```

### Streaming reads and writes

`dlt_read_stream` and `dlt_read_stream` can be used for streaming reads and writes respectively.

```r
query <- dlt_read_stream("/tmp/target") %>%
  dlt_write_stream(
    path = "/tmp/target-stream", queryName = "test", trigger.once = TRUE,
    checkpointLocation = "/tmp/target-stream/_checkpoints/test"
  )

awaitTermination(query, 10000)
# [1] TRUE
```

## Notes

Examples use `source` and `target` datasets as described in `tests/testthat/data/README.md`.

## Acknowledgments 

Logo based on [Yellow wasp, m, left, Kruger National Park, South Africa](https://flickr.com/photos/54563451@N08/45531028154) 
by [USGS Bee Inventory and Monitoring Lab ](https://www.flickr.com/photos/usgsbiml/). 

## Disclaimer

Delta is a trademark of the LF Projects LLC. This project is not owned, endorsed or sponsored by the LF Projects LLC.
