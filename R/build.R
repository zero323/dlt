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

#' @include generics.R table.R utils.R
NULL


#' S4 class that represents pending table build operation
#'
#' @family DeltaTableBuilder functions
#' @rdname DeltaTableBuilder
#' @docType class
#'
#' @slot jtb A Java object reference to the backing DeltaTableBuilder
#'
#' @note DeltaTableBuilder, since 1.0.0
setClass("DeltaTableBuilder", slots = c(jtb = "jobj"))


initializeDeltaTableBuilder <- function(initializer) {
  new(
    "DeltaTableBuilder",
    jtb = invoke_delta_table_static(
      initializer,
      active_session()
    )
  )
}


#' Build DeltaTable
#'
#' @returns DeltaTableBuilder
#'
#' @export
#' @note dlt_create, since 1.0.0
dlt_create <- function() {
  initializeDeltaTableBuilder("create")
}


#' Build DeltaTable if doesn't exits
#'
#' @returns DeltaTableBuilder
#'
#' @export
#' @note dlt_create_if_not_exists, since 1.0.0
dlt_create_if_not_exists <- function() {
  initializeDeltaTableBuilder("createIfNotExists")
}


#' Replace DeltaTable
#'
#' @returns DeltaTableBuilder
#'
#' @export
#' @note dlt_replace, since 1.0.0
dlt_replace <- function() {
  initializeDeltaTableBuilder("replace")
}


#' Create or replace DeltaTable
#'
#' @returns DeltaTableBuilder
#'
#' @export
#' @note dlt_create_or_replace, since 1.0.0
dlt_create_or_replace <- function() {
  initializeDeltaTableBuilder("createOrReplace")
}


#' Execute merge operation on this builder
#'
#' @param bldr Delta table builder
#' @returns DeltaTable
#'
#' @name dlt_execute
#' @rdname dlt_execute
#' @aliases dlt_execute,DeltaTableBuilder-method
#'
#' @export
#' @note dlt_execute, since 1.0.0
setMethod(
  "dlt_execute",
  signature(bldr = "DeltaTableBuilder"),
  function(bldr) {
    new(
      "DeltaTable",
      jdt = sparkR.callJMethod(bldr@jtb, "execute")
    )
  }
)


#' Specify data storage location for the build table.
#'
#' @param dtb Delta table builder
#' @param location character, path
#' @return this DeltaTableBuilder
#'
#' @name dlt_location
#' @rdname dlt_location
#' @aliases dlt_location,DeltaTableBuilder,character-method
#'
#' @export
#' @note dlt_location, since 1.0.0
setMethod(
  "dlt_location",
  signature(dtb = "DeltaTableBuilder", location = "character"),
  function(dtb, location) {
    validate_is_scalar_like_character(location)

    sparkR.callJMethod(dtb@jtb, "location", location)
    dtb
  }
)


#' Specify name of the build table.
#'
#' @param dtb Delta table builder
#' @param identifier character
#' @return this DeltaTableBuilder
#'
#' @name dlt_table_name
#' @rdname dlt_table_name
#' @aliases dlt_table_name,DeltaTableBuilder,character-method
#'
#' @export
#' @note dlt_table_name, since 1.0.0
setMethod(
  "dlt_table_name",
  signature(dtb = "DeltaTableBuilder", identifier = "character"),
  function(dtb, identifier) {
    validate_is_scalar_like_character(identifier)

    sparkR.callJMethod(dtb@jtb, "tableName", identifier)
    dtb
  }
)


#' Add column to the build table
#'
#'
#' @param dtb Delta table builder
#' @param col_name character
#' @param data_type character
#' @param nullable optional, logical
#' @param generated_always_as optional, character
#' @param comment optional, character
#' @param ... other arguments, not used
#' @return this DeltaTableBuilder
#'
#' @name dlt_add_column
#' @rdname dlt_add_column
#' @aliases dlt_add_column,DeltaTableBuilder,character,character-method
#'
#' @export
#' @note dlt_add_column, since 1.0.0
setMethod(
  "dlt_add_column",
  signature(dtb = "DeltaTableBuilder", col_name = "character", data_type = "character"),
  function(dtb, col_name, data_type, nullable = TRUE, generated_always_as = NULL, comment = NULL) {
    validate_is_scalar_like_character(generated_always_as, nullable = TRUE)
    validate_is_scalar_like_character(comment, nullable = TRUE)

    invoke_delta_table_static(
      "columnBuilder",
      active_session(),
      col_name
    ) %>%
      sparkR.callJMethod("dataType", data_type) %>%
      sparkR.callJMethod("nullable", nullable) %>%
      invoke_if_arg_not_null("generatedAlwaysAs", generated_always_as) %>%
      invoke_if_arg_not_null("comment", comment) %>%
      sparkR.callJMethod("build") %>%
      sparkR.callJMethod(dtb@jtb, "addColumn", .)

    dtb
  }
)