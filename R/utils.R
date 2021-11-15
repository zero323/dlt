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


#' return the SparkSession
#'
#' @noRd
active_session <- function() {
  SparkR::sparkR.session()
}


#' Check if argument is a scalar-like character
#'
#' @noRd
is_scalar_like_character <- function(x) {
  is.character(x) && !is.na(x) && length(x) == 1
}


#' Validate if argument is a scalar-like character
#'
#' @noRd
validate_is_scalar_like_character <- function(x, nullable = FALSE) {
  stopifnot(
    (nullable && is.null(x)) || is_scalar_like_character(x)
  )
}


#' prepare ... cols
#'
#' @noRd
prepare_and_validate_cols <- function(...) {
  cols <- list(...)

  sapply(cols, is_scalar_like_character) %>%
    all() %>%
    stopifnot()

  cols
}


#' Invoke method on jobj if arg not null
#'
#' @noRd
invoke_if_arg_not_null <- function(jobj, method, arg) {
  if (is.null(arg)) {
    jobj
  } else {
    sparkR.callJMethod(jobj, method, arg)
  }
}


#' Prepare list that can be used for delayed execution
#'
#' @noRd
prepare_delayed_arg_list <- function(.method, ...) {
  list(.method = .method, args = list(...))
}


#' Call static initializer method for DataTable builder
#'
#' @noRd
initializeDeltaTableBuilder <- function(initializer) { # nolint
  new(
    "DeltaTableBuilder",
    jtb = invoke_delta_table_static(
      initializer,
      active_session()
    )
  )
}
