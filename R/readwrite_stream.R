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

#' Write and read streaming SparkDataFrame in delta format
#'
#' @name dlt_readwrite_stream
#' @rdname dlt_readwrite_stream
#'
#' @examples \dontrun{
#' set.seed(323)
#' input_path <- tempfile()
#' output_path <- tempfile()
#'
#' # Dummy input
#' data.frame(
#'   id = 1:12,
#'   key = rep(c("a", "b", "c"), each = 4),
#'   value = rnorm(12)
#' ) %>%
#'   createDataFrame() %>%
#'   dlt_write(input_path)
#'
#' # Read data as stream and write back to output location
#' query <- dlt_read_stream(path = input_path) %>%
#'   dlt_write_stream(
#'     path = output_path, queryName = "test", trigger.once = TRUE,
#'     checkpointLocation = file.path(output_path, "_checkpoints", "test")
#'   )
#'
#' awaitTermination(query)
#' }
#'
NULL


#' @param path path of file to read.
#' @param ... additional data source specific named properties.
#' @describeIn dlt_readwrite_stream  Loads data stored in delta format, returning the result as a streaming SparkDataFrame
#' @export
#' @note dlt_read_stream since 1.0.0
dlt_read_stream <- function(path, ...) {
  SparkR::read.stream(path = path, source = "delta", ...)
}


#' @param df streaming SparkDataFrame
#' @param path character path to write the data.
#' @param ... additional arguments passed to writer
#' @describeIn dlt_readwrite_stream Writes streaming SparkDataFrame using delta format
#' @aliases dlt_write_stream,SparkDataFrame,character-method
#' @export
#' @note dlt_write_stream since 1.0.0
setMethod(
  "dlt_write_stream",
  signature(df = "SparkDataFrame", path = "character"),
  function(df, path, ...) {
    SparkR::write.stream(df, path = path, source = "delta", ...)
  }
)
