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


### Updates

Updates without

```r
dlt_for_path("/tmp/target") %>%
  dlt_update(list(ind = "-ind")) %>%
  dlt_show(5)

# +---+---+---+---+--------+-------------------+-------------------+
# | id|key|val|ind|category|                lat|               long|
# +---+---+---+---+--------+-------------------+-------------------+
# |  1|  a|  4|  1|     KBQ| -56.28354165237397|-108.74080670066178|
# |  2|  a| 10| -1|     ROB| 50.546925463713706|-104.60825988091528|
# |  3|  a|  7|  1|     SLX|-13.985343240201473|-114.89280310459435|
# |  4|  a|  5| -1|     ACP| -47.15050248429179|-168.96175763569772|
# |  5|  b|  3|  1|     EEK|-49.020595396868885|-105.57821027934551|
# +---+---+---+---+--------+-------------------+-------------------+
# only showing top 5 rows
```

and with

```r
dlt_for_path("/tmp/target") %>%
  dlt_update(list(
    lat = lit(39.08067389467202),
    long = lit(-89.73335678516888)
  ), column("id") %in% c(2, 4)) %>%
  dlt_show(5)

# +---+---+---+---+--------+-------------------+-------------------+
# | id|key|val|ind|category|                lat|               long|
# +---+---+---+---+--------+-------------------+-------------------+
# |  1|  a|  4|  1|     KBQ| -56.28354165237397|-108.74080670066178|
# |  2|  a| 10| -1|     ROB|  39.08067389467202| -89.73335678516888|
# |  3|  a|  7|  1|     SLX|-13.985343240201473|-114.89280310459435|
# |  4|  a|  5| -1|     ACP|  39.08067389467202| -89.73335678516888|
# |  5|  b|  3|  1|     EEK|-49.020595396868885|-105.57821027934551|
# +---+---+---+---+--------+-------------------+-------------------+
# only showing top 5 rows
```

condition are supported.

### Deletes

Deletes with

```r
dlt_for_path("/tmp/target") %>%
  dlt_delete(column("category") %in% c("ROB", "ACP")) %>%
  dlt_show(5)

# +---+---+---+---+--------+-------------------+-------------------+
# | id|key|val|ind|category|                lat|               long|
# +---+---+---+---+--------+-------------------+-------------------+
# |  1|  a|  4|  1|     KBQ| -56.28354165237397|-108.74080670066178|
# |  3|  a|  7|  1|     SLX|-13.985343240201473|-114.89280310459435|
# |  5|  b|  3|  1|     EEK|-49.020595396868885|-105.57821027934551|
# |  6|  b|  2| -1|     SMT|  80.79935231711715| -46.80488987825811|
# |  7|  b|  9|  1|     LBC|  51.65884342510253|  97.16074059717357|
# +---+---+---+---+--------+-------------------+-------------------+
# only showing top 5 rows
```

and without

```r
dlt_for_path("/tmp/target") %>%
  dlt_delete() %>%
  dlt_to_df() %>%
  count()

# [1] 0
```

condition are supported.

### Merging SparkDataFrame with DeltaTable

`dlt` supports a complete set of `DeltaMergeBuilder` methods (`dlt_when_matched_delete`, `dlt_when_matched_update`, `dlt_when_matched_update_all`, `dlt_when_not_matched_insert`, `dlt_when_not_matched_insert_all`).

```r
target %>%
  dlt_write("/tmp/target", mode="overwrite")

dlt_for_path("/tmp/target") %>%
  dlt_alias("target") %>%
  dlt_merge(alias(source, "source"), expr("source.id = target.id")) %>%
  dlt_when_matched_update_all() %>%
  dlt_when_not_matched_insert_all() %>%
  dlt_execute() %>%
  dlt_show(10)

# +---+---+---+---+--------+------------------+-------------------+
# | id|key|val|ind|category|               lat|               long|
# +---+---+---+---+--------+------------------+-------------------+
# |  6|  b|  2|  1|     SMT| 80.79935231711715| -46.80488987825811|
# | 16|  d| 10| -1|     DMO|-75.16123954206705| 120.96153359860182|
# |  7|  b|  9| -1|     LBC| 51.65884342510253|  97.16074059717357|
# | 14|  c|  9| -1|     MYZ| 80.40028186049312|-19.090933883562684|
# |  9|  c| 10| -1|     LCN| 76.90145746339113| -138.4841349441558|
# | 12|  c| 10|  1|     TBL| 6.229456798173487|  55.28501939959824|
# | 11|  c|  5| -1|     ZSH| 89.73377700895071|  61.67111137881875|
# |  3|  b|  5|  1|     RSL|-65.03216980956495| -39.52675184234977|
# |  4|  a|  5|  1|     ACP|-47.15050248429179|-168.96175763569772|
# |  1|  a|  1|  1|     NTD| 72.72564971353859|  5.116242365911603|
# +---+---+---+---+--------+------------------+-------------------+
# only showing top 10 rows
```

## Notes

Examples use `source` and `target` datasets as described in `tests/testthat/data/README.md`.

## Acknowledgments 

Logo based on [Yellow wasp, m, left, Kruger National Park, South Africa](https://flickr.com/photos/54563451@N08/45531028154) 
by [USGS Bee Inventory and Monitoring Lab](https://www.flickr.com/photos/usgsbiml/).

## Disclaimer

Delta is a trademark of the LF Projects LLC. This project is not owned, endorsed or sponsored by the LF Projects LLC.
