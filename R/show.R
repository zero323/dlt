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


#' Print rows of the corresponding SparkDataFrame
#'
#' Convenience method equivalent to
#'
#' ```r
#' dt %>%
#'  dlt_to_df() %>%
#'  showDF()
#' ```
#'
#' @param dt DeltaTable
#' @param ... Additional arguments passed to `SparkR::showDF`
#' @returns this DeltaTable, invisibly
#'
#' @name dlt_show
#' @rdname dlt_show
#' @aliases dlt_show,DeltaTable-method
#'
#' @export
#' @note dlt_show, since 1.0.0
setMethod(
  "dlt_show",
  signature(dt = "DeltaTable"),
  function(dt, ...) {
    dt %>%
      dlt_to_df() %>%
      SparkR::showDF(...)
    invisible(dt)
  }
)
