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

#' invoke DeltaTable static methods
#'
#' @noRd
invoke_delta_table_static <- function(method, ...) {
  sparkR.callJStatic(
    "io.delta.tables.DeltaTable",
    method,
    ...
  )
}


#' Prepare list of Column objects, given named character vector or Column list
#'
#' @noRd
to_column_list <- function(x) {
  stopifnot(is.character(x) || is.list(x))
  stopifnot(!is.null(names(x)) || length(x) == 0)
  stopifnot(all(!is.na(names(x))))


  lapply(x, function(x) {
    if (is.character(x)) {
      SparkR::expr(x)
    } else if ("Column" %in% class(x)) {
      x
    } else {
      stop(paste(
        "elements of x have to be character or Column, got",
        paste(class(x), collapse = ",")
      ))
    }
  })
}


#' Prepare environment of Java Columns, given list of Column
#'
#' @noRd
to_expression_env <- function(x) {
  to_column_list(x) %>%
    lapply(function(x) x@jc) %>%
    as.environment()
}