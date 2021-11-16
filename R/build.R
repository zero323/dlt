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

#' @include generics.R utils.R build_internal.R
NULL


#' S4 class that represents pending table build operation
#'
#' @family DeltaTableBuilder functions
#' @rdname DeltaTableBuilder
#' @docType class
#'
#' @slot method character name of the initializer method
#' @slot ops list list of operations in form of list(.method = ..., args = list(...))
#'
#' @examples \dontrun{
#'   path <- tempfile()
#'
#'   dlt_create() %>%
#'     dlt_location(path) %>%
#'     dlt_add_column("id", "integer", nullable = FALSE) %>%
#'     dlt_add_columns(structType("key string, value double")) %>%
#'     dlt_partitioned_by("key") %>%
#'     dlt_comment("Key-value table") %>%
#'     dlt_property("creation-time", as.character(Sys.time())) %>%
#'     dlt_execute()
#' }
#'
#' @note DeltaTableBuilder, since 1.0.0
setClass("DeltaTableBuilder", slots = c(method = "character", ops = "list"))


newDeltaTableBuilder <- function(method, collected_ops, ops) { # nolint
  new(
    "DeltaTableBuilder",
    method = method,
    ops = c(collected_ops, list(ops))
  )
}


initializeDeltaTableBuilder <- function(initializer) { # nolint
  new(
    "DeltaTableBuilder",
    method = initializer,
    ops = list()
  )
}


#' Build DeltaTable
#'
#' @returns InternalDeltaTableBuilder
#'
#' @export
#' @note dlt_create, since 1.0.0
dlt_create <- function() {
  initializeDeltaTableBuilder("create")
}


#' Build DeltaTable if doesn't exits
#'
#' @returns InternalDeltaTableBuilder
#'
#' @export
#' @note dlt_create_if_not_exists, since 1.0.0
dlt_create_if_not_exists <- function() {
  initializeDeltaTableBuilder("createIfNotExists")
}


#' Replace DeltaTable
#'
#' @returns InternalDeltaTableBuilder
#'
#' @export
#' @note dlt_replace, since 1.0.0
dlt_replace <- function() {
  initializeDeltaTableBuilder("replace")
}


#' Create or replace DeltaTable
#'
#' @returns InternalDeltaTableBuilder
#'
#' @export
#' @note dlt_create_or_replace, since 1.0.0
dlt_create_or_replace <- function() {
  initializeDeltaTableBuilder("createOrReplace")
}


#' Execute build operation on this builder
#'
#' @param bldr DeltaTableBuilder
#' @returns DeltaTable
#'
#' @describeIn dlt-execute-table-builder Execute build operation
#' @aliases dlt_execute,DeltaTableBuilder-method
#'
#' @export
#' @note dlt_execute, since 1.0.0
setMethod(
  "dlt_execute",
  signature(bldr = "DeltaTableBuilder"),
  function(bldr) {
    mut_bldr <- initializeInternalDeltaTableBuilder(bldr@method)
    for (op in bldr@ops) {
      do.call(op$.method, c(mut_bldr, op$args))
    }
    dlt_execute(mut_bldr)
  }
)


#' Specify data storage location for the build table.
#'
#' @param dtb DeltaTableBuilder
#' @param location character, path
#' @return DeltaTableBuilder
#'
#' @describeIn dlt-location-table-builder Specify data storage location
#' @aliases dlt_location,DeltaTableBuilder,character-method
#'
#' @export
#' @note dlt_location, since 1.0.0
setMethod(
  "dlt_location",
  signature(dtb = "DeltaTableBuilder", location = "character"),
  function(dtb, location) {
    validate_is_scalar_like_character(location)
    newDeltaTableBuilder(
      dtb@method,
      dtb@ops,
      prepare_delayed_arg_list(.method = "dlt_location", location = location)
    )
  }
)


#' Specify name of the build table.
#'
#' @param dtb DeltaTableBuilder
#' @param identifier character
#' @return DeltaTableBuilder
#'
#' @describeIn dlt-table-name-table-builder Specify name of the table
#' @aliases dlt_table_name,DeltaTableBuilder,character-method
#'
#' @export
#' @note dlt_table_name, since 1.0.0
setMethod(
  "dlt_table_name",
  signature(dtb = "DeltaTableBuilder", identifier = "character"),
  function(dtb, identifier) {
    validate_is_scalar_like_character(identifier)

    newDeltaTableBuilder(
      dtb@method,
      dtb@ops,
      prepare_delayed_arg_list(.method = "dlt_table_name", identifier = identifier)
    )
  }
)


#' Add column to the build table
#'
#'
#' @param dtb DeltaTableBuilder
#' @param col_name character
#' @param data_type character
#' @param nullable optional, logical
#' @param generated_always_as optional, character
#' @param comment optional, character
#' @return DeltaTableBuilder
#'
#' @describeIn dlt-add-column-table-builder Add column to the table
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

    newDeltaTableBuilder(
      dtb@method,
      dtb@ops,
      prepare_delayed_arg_list(
        .method = "dlt_add_column",
        col_name = col_name, data_type = data_type, nullable = nullable,
        generated_always_as = generated_always_as, comment = comment
      )
    )
  }
)


#' Add columns to the build table
#'
#'
#' @param dtb DeltaTableBuilder
#' @param schema character (DDL string) or structType
#' @return DeltaTableBuilder
#'
#' @describeIn dlt-add-columns-table-builder Add columns to the table
#' @aliases dlt_add_columns,DeltaTableBuilder,ANY-method
#'
#' @export
#' @note dlt_add_columns, since 1.0.0
setMethod(
  "dlt_add_columns",
  signature(dtb = "DeltaTableBuilder", schema = "ANY"),
  function(dtb, schema) {
    # Validate here, instead of having union of types to avoid unnecessary warnings
    stopifnot(is.character(schema) || "structType" %in% class(schema))

    newDeltaTableBuilder(
      dtb@method,
      dtb@ops,
      prepare_delayed_arg_list(.method = "dlt_add_columns", schema = schema)
    )
  }
)


#' Add comment describing the build table.
#'
#' @param dtb DeltaTableBuilder
#' @param comment character, path
#' @return DeltaTableBuilder
#'
#' @describeIn dlt-add-comment-table-builder Add comment
#' @aliases dlt_comment,DeltaTableBuilder,character-method
#'
#' @export
#' @note dlt_comment, since 1.0.0
setMethod(
  "dlt_comment",
  signature(dtb = "DeltaTableBuilder", comment = "character"),
  function(dtb, comment) {
    validate_is_scalar_like_character(comment)

    newDeltaTableBuilder(
      dtb@method,
      dtb@ops,
      prepare_delayed_arg_list(.method = "dlt_comment", comment = comment)
    )
  }
)


#' Specify partitioning columns
#'
#' @param dtb DeltaTableBuilder
#' @param ... character columns
#'
#' @describeIn dlt-partitioned-by-table-builder Specify partitioning
#' @aliases dlt_partitioned_by,DeltaTableBuilder-method
#'
#' @export
#' @note dlt_partitioned_by, since 1.0.0
setMethod(
  "dlt_partitioned_by",
  signature(dtb = "DeltaTableBuilder"),
  function(dtb, ...) {
    # Validate early
    prepare_and_validate_cols(...)

    newDeltaTableBuilder(
      dtb@method,
      dtb@ops,
      prepare_delayed_arg_list(.method = "dlt_partitioned_by", ...)
    )
  }
)


#' Set property for the build table
#'
#' @param dtb DeltaTableBuilder
#' @param key character
#' @param value character
#'
#' @describeIn dlt-dlt-property-table-builder Set property
#' @aliases dlt_property,DeltaTableBuilder,character,character-method
#'
#' @export
#' @note dlt_property, since 1.0.0
setMethod(
  "dlt_property",
  signature(dtb = "DeltaTableBuilder", key = "character", value = "character"),
  function(dtb, key, value) {
    validate_is_scalar_like_character(key)
    validate_is_scalar_like_character(value)

    newDeltaTableBuilder(
      dtb@method,
      dtb@ops,
      prepare_delayed_arg_list(.method = "dlt_property", key = key, value = value)
    )
  }
)
