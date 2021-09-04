# Example Data Description

## `target.parquet`

`target.parquet` contains a test dataset of the following form

| id|key | val| ind|category |        lat|       long|
|--:|:---|---:|---:|:--------|----------:|----------:|
|  1|a   |   4|  -1|KBQ      | -56.283542| -108.74081|
|  2|a   |  10|   1|ROB      |  50.546926| -104.60826|
|  3|a   |   7|  -1|SLX      | -13.985343| -114.89280|
|  4|a   |   5|   1|ACP      | -47.150503| -168.96176|
|  5|b   |   3|  -1|EEK      | -49.020595| -105.57821|
|  6|b   |   2|   1|SMT      |  80.799352|  -46.80489|
|  7|b   |   9|  -1|LBC      |  51.658843|   97.16074|
|  8|b   |   8|   1|BOB      |  47.120745|  -91.43877|
|  9|c   |  10|  -1|LCN      |  76.901458| -138.48413|
| 10|c   |   1|   1|GKP      |  58.438535|  100.64807|
| 11|c   |   5|  -1|ZSH      |  89.733777|   61.67111|
| 12|c   |  10|   1|TBL      |   6.229457|   55.28502|

and the schema as shown below:

```
root
 |-- id: integer (nullable = true)
 |-- key: string (nullable = true)
 |-- val: integer (nullable = true)
 |-- ind: integer (nullable = true)
 |-- category: string (nullable = true)
 |-- lat: double (nullable = true)
 |-- long: double (nullable = true)
```


It can be reproduced locally, if needed, using the following snippet

```r
set.seed(323)

df <- tibble::tibble(
  id = 1:12,
  key = rep(c("a", "b", "c"), each=4),
  val = as.integer(round(sample(1:10, 12, TRUE))),
  ind = as.integer(rep(c(-1, 1), 6)),
  category = stringi::stri_rand_strings(12, 3, "[A-Z]"),
  lat = runif(12, -90, 90),
  long = runif(12, -180, 180)
)
```

## `source.parquet`

`source.parquet` contains a test dataset of the following form


| id|key | val| ind|category |       lat|       long|
|--:|:---|---:|---:|:--------|---------:|----------:|
|  1|a   |   1|   1|NTD      |  72.72565|   5.116242|
|  3|b   |   5|   1|RSL      | -65.03217| -39.526752|
|  5|b   |   1|  -1|SYG      |  88.00051| 146.065727|
| 14|c   |   9|  -1|MYZ      |  80.40028| -19.090934|
| 16|d   |  10|  -1|DMO      | -75.16124| 120.961534|


with schema the same as `example.parquet`.

It can be reproduced locally, if needed, using the following snippet

```r
set.seed(42)

df <- tibble::tibble(
  id = as.integer(c(1, 3, 5, 14, 16)),
  key = c("a", "b", "b", "c", "d"),
  val = as.integer(round(sample(1:10, 5, TRUE))),
  ind = as.integer(c(1, 1, -1, -1, -1)),
  category = stringi::stri_rand_strings(5, 3, "[A-Z]"),
  lat = runif(5, -90, 90),
  long = runif(5, -180, 180)
)
```
