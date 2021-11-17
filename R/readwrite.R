#
# Copyright 2021 zero323
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#' @include generics.R package.R
NULL


#' Write and read SparkDataFrame in delta format
#'
#' @name dlt_readwrite
#' @rdname dlt_readwrite
#'
#' @examples \dontrun{
#' set.seed(323)
#' path <- tempfile()
#'
#' df <- data.frame(
#'   id = 1:12,
#'   key = rep(c("a", "b", "c"), each = 4),
#'   value = rnorm(12)
#' ) %>%
#'   createDataFrame()
#'
#' createDataFrame(df) %>%
#'   dlt_write(path)
#'
#' dlt_read(path) %>%
#'   schema()
#'
#' createDataFrame(df) %>%
#'   write.delta(path, mode = "overwrite")
#'
#' read.delta(path) %>%
#'   schema()
#' }
NULL



#' @param path path of file to read.
#' @param ... additional data source specific named properties.
#' @describeIn dlt_readwrite Load data stored as delta into `SparkDataFrame`
#'
#' @export
#' @note dlt_read since 1.0.0
dlt_read <- function(path, ...) {
  SparkR::read.df(path, source = "delta", ...)
}


#' @param path path of file to read.
#' @param ... additional data source specific named properties.
#' @describeIn dlt_readwrite Load data stored as delta into `SparkDataFrame` (alias of `dlt_read`)
#'
#' @export
#' @note read.delta since 1.0.0
read.delta <- function(path, ...) { # nolint
  dlt_read(path, ...)
}


#' @param df SparkDataFrame
#' @param path character path to write the data.
#' @param ... additional arguments passed to writer
#'
#' @describeIn dlt_readwrite Write SparkDataFrame` in delta format
#' @aliases dlt_write,SparkDataFrame,character-method
#'
#' @export
#' @note dlt_write since 1.0.0
setMethod(
  "dlt_write",
  signature(df = "SparkDataFrame", path = "character"),
  function(df, path, ...) {
    SparkR::write.df(df, path = path, source = "delta", ...)
  }
)


#' @param df SparkDataFrame
#' @param path character path to write the data.
#' @param ... additional arguments passed to writer
#'
#' @describeIn dlt_readwrite Write `SparkDataFrame` in delta format (alias of `dlt_write`)
#' @aliases write.delta,SparkDataFrame,character-method
#'
#' @export
#' @note write.delta since 1.0.0
setMethod(
  "write.delta",
  signature(df = "SparkDataFrame", path = "character"),
  function(df, path, ...) {
    dlt_write(df, path, ...)
  }
)
