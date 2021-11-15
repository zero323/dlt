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


#' Create a streaming SparkDataFrame from a Delta directory
#'
#' Loads a DeltaTable, returning the result as a streaming SparkDataFrame.
#'
#' @param path path of file to read.
#' @param ... additional data source specific named properties.
#' @rdname dlt_read_stream
#' @name dlt_read_stream
#' @examples
#' \dontrun{
#' dlt_read_stream("/tmp/iris-delta")
#' }
#' @export
#' @note dlt_read_stream since 1.0.0
dlt_read_stream <- function(path, ...) {
  SparkR::read.stream(path = path, source = "delta", ...)
}


#' Write streaming SparkDataFrame to Delta
#'
#' Writes streaming SparkDataFrame to Delta.
#'
#' @param df streaming SparkDataFrame
#' @param path character path to write the data.
#' @param ... additional arguments passed to writer
#' @rdname dlt_write_stream
#' @name dlt_write_stream
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
