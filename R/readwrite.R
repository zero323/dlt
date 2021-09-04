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

#' @include generics.R
NULL


#' Create a SparkDataFrame from a Delta Table
#'
#' Loads a DeltaTable, returning the result as a SparkDataFrame.
#'
#' @param path path of file to read. A vector of multiple paths is allowed.
#' @param ... additional data source specific named properties.
#' @rdname dlt_read
#' @name dlt_read
#' @examples
#' \dontrun{
#' dlt_read("/tmp/iris-delta")
#' }
#' @export
#' @note dlt_read since 1.0.0
dlt_read <- function(path, ...) {
  SparkR::read.df(path, source = "delta", ...)
}


#' Create a SparkDataFrame from a Delta Table
#'
#' Loads a DeltaTable, returning the result as a SparkDataFrame.
#'
#' @param path path of file to read. A vector of multiple paths is allowed.
#' @param ... additional data source specific named properties.
#' @rdname read.delta
#' @name read.delta
#' @note read.delta since 1.0.0
#' @examples
#' \dontrun{
#' read.delta("/tmp/iris-delta")
#' }
#' @export
#' @seealso dlt_read
read.delta <- function(path, ...) {
  dlt_read(path, ...)
}


#' Write SparkDataFrame to Delta
#'
#' Writes SparkDataFrame to Delta.
#'
#' @param df SparkDataFrame
#' @param path character path to write the data.
#' @param ... additional arguments passed to writer
#' @rdname dlt_write
#' @name dlt_write
#' @aliases dlt_write,SparkDataFrame,character-method
#' @examples
#' \dontrun{
#' SparkR::createDataFrame(iris) %>%
#'   dlt_write("/tmp/iris-delta")
#' }
#' @export
#' @note dlt_write since 1.0.0
setMethod(
  "dlt_write",
  signature(df = "SparkDataFrame", path = "character"),
  function(df, path, ...) {
    SparkR::write.df(df, path = path, source = "delta", ...)
  }
)


#' Write SparkDataFrame to Delta
#'
#' Writes SparkDataFrame to Delta.
#'
#' @param df SparkDataFrame
#' @param path character path to write the data.
#' @param ... additional arguments passed to writer
#' @rdname write.delta
#' @name write.delta
#' @aliases write.delta,SparkDataFrame,character-method
#' @examples
#' \dontrun{
#' SparkR::createDataFrame(iris) %>%
#'   write.delta("/tmp/iris-delta")
#' }
#' @export
#' @note write.delta since 1.0.0
#' @seealso dlt_write
setMethod(
  "write.delta",
  signature(df = "SparkDataFrame", path = "character"),
  function(df, path, ...) {
    dlt_write(df, path, ...)
  }
)
