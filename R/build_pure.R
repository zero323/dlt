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

#' @include generics.R utils.R build.R
NULL


#' S4 class that represents pending table build operation
#'
#' @family PureDeltaTableBuilder functions
#' @rdname PureDeltaTableBuilder
#' @docType class
#'
#' @slot method character name of the initializer method
#' @slot ops list list of operations in form of list(.method = ..., args = list(...))
#'
#' @note DeltaTableBuilder, since 1.0.0
setClass("PureDeltaTableBuilder", slots = c(method = "character", ops = "list"))


newPureDeltaTableBuilder <- function(method, collected_ops, ops) { # nolint
  new(
    "PureDeltaTableBuilder",
    method = method,
    ops = c(collected_ops, list(ops))
  )
}


initializePureDeltaTableBuilder <- function(initializer) { # nolint
  new(
    "PureDeltaTableBuilder",
    method = initializer,
    ops = list()
  )
}


#' Execute merge operation on this builder
#'
#' @param bldr PureDeltaTableBuilder
#' @returns DeltaTable
#'
#' @name dlt_execute
#' @rdname dlt_execute
#' @aliases dlt_execute,PureDeltaTableBuilder-method
#'
#' @export
#' @note dlt_execute, since 1.0.0
setMethod(
  "dlt_execute",
  signature(bldr = "PureDeltaTableBuilder"),
  function(bldr) {
    mut_bldr <- initializeDeltaTableBuilder(bldr@method)
    for (op in bldr@ops) {
      do.call(op$.method, c(mut_bldr, op$args))
    }
    dlt_execute(mut_bldr)
  }
)


#' Specify data storage location for the build table.
#'
#' @param dtb PureDeltaTableBuilder
#' @param location character, path
#' @return PureDeltaTableBuilder
#'
#' @name dlt_location
#' @rdname dlt_location
#' @aliases dlt_location,PureDeltaTableBuilder,character-method
#'
#' @export
#' @note dlt_location, since 1.0.0
setMethod(
  "dlt_location",
  signature(dtb = "PureDeltaTableBuilder", location = "character"),
  function(dtb, location) {
    validate_is_scalar_like_character(location)
    newPureDeltaTableBuilder(
      dtb@method,
      dtb@ops,
      prepare_delayed_arg_list(.method = "dlt_location", location = location)
    )
  }
)


#' Specify name of the build table.
#'
#' @param dtb PureDeltaTableBuilder
#' @param identifier character
#' @return PureDeltaTableBuilder
#'
#' @name dlt_table_name
#' @rdname dlt_table_name
#' @aliases dlt_table_name,PureDeltaTableBuilder,character-method
#'
#' @export
#' @note dlt_table_name, since 1.0.0
setMethod(
  "dlt_table_name",
  signature(dtb = "PureDeltaTableBuilder", identifier = "character"),
  function(dtb, identifier) {
    validate_is_scalar_like_character(identifier)

    newPureDeltaTableBuilder(
      dtb@method,
      dtb@ops,
      prepare_delayed_arg_list(.method = "dlt_table_name", identifier = identifier)
    )
  }
)


#' Add column to the build table
#'
#'
#' @param dtb PureDeltaTableBuilder
#' @param col_name character
#' @param data_type character
#' @param nullable optional, logical
#' @param generated_always_as optional, character
#' @param comment optional, character
#' @param ... other arguments, not used
#' @return PureDeltaTableBuilder
#'
#' @name dlt_add_column
#' @rdname dlt_add_column
#' @aliases dlt_add_column,PureDeltaTableBuilder,character,character-method
#'
#' @export
#' @note dlt_add_column, since 1.0.0
setMethod(
  "dlt_add_column",
  signature(dtb = "PureDeltaTableBuilder", col_name = "character", data_type = "character"),
  function(dtb, col_name, data_type, nullable = TRUE, generated_always_as = NULL, comment = NULL) {
    validate_is_scalar_like_character(generated_always_as, nullable = TRUE)
    validate_is_scalar_like_character(comment, nullable = TRUE)

    newPureDeltaTableBuilder(
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


#' Add column to the build table
#'
#'
#' @param dtb PureDeltaTableBuilder
#' @param schema character (DDL string) or structType
#' @return PureDeltaTableBuilder
#'
#' @name dlt_add_columns
#' @rdname dlt_add_columns
#' @aliases dlt_add_columns,PureDeltaTableBuilder,ANY-method
#'
#' @export
#' @note dlt_add_columns, since 1.0.0
setMethod(
  "dlt_add_columns",
  signature(dtb = "PureDeltaTableBuilder", schema = "ANY"),
  function(dtb, schema) {
    # Validate here, instead of having union of types to avoid unnecessary warnings
    stopifnot(is.character(schema) || "structType" %in% class(schema))

    newPureDeltaTableBuilder(
      dtb@method,
      dtb@ops,
      prepare_delayed_arg_list(.method = "dlt_add_columns", schema = schema)
    )
  }
)


#' Add comment describing the build table.
#'
#' @param dtb PureDeltaTableBuilder
#' @param comment character, path
#' @return PureDeltaTableBuilder
#'
#' @name dlt_comment
#' @rdname dlt_comment
#' @aliases dlt_comment,PureDeltaTableBuilder,character-method
#'
#' @export
#' @note dlt_comment, since 1.0.0
setMethod(
  "dlt_comment",
  signature(dtb = "PureDeltaTableBuilder", comment = "character"),
  function(dtb, comment) {
    validate_is_scalar_like_character(comment)

    newPureDeltaTableBuilder(
      dtb@method,
      dtb@ops,
      prepare_delayed_arg_list(.method = "dlt_comment", comment = comment)
    )
  }
)


#' Specify partitioning columns
#'
#' @param dtb PureDeltaTableBuilder
#' @param ... character columns
#'
#' @name dlt_partitioned_by
#' @rdname dlt_partitioned_by
#' @aliases dlt_partitioned_by,PureDeltaTableBuilder-method
#'
#' @export
#' @note dlt_partitioned_by, since 1.0.0
setMethod(
  "dlt_partitioned_by",
  signature(dtb = "PureDeltaTableBuilder"),
  function(dtb, ...) {
    # Validate early
    prepare_and_validate_cols(...)

    newPureDeltaTableBuilder(
      dtb@method,
      dtb@ops,
      prepare_delayed_arg_list(.method = "dlt_partitioned_by", ...)
    )
  }
)


#' Set property for the build table
#'
#' @param dtb PureDeltaTableBuilder
#' @param key character
#' @param value character
#'
#' @name dlt_property
#' @rdname dlt_property
#' @aliases dlt_property,PureDeltaTableBuilder,character,character-method
#'
#' @export
#' @note dlt_property, since 1.0.0
setMethod(
  "dlt_property",
  signature(dtb = "PureDeltaTableBuilder", key = "character", value = "character"),
  function(dtb, key, value) {
    validate_is_scalar_like_character(key)
    validate_is_scalar_like_character(value)

    newPureDeltaTableBuilder(
      dtb@method,
      dtb@ops,
      prepare_delayed_arg_list(.method = "dlt_property", key = key, value = value)
    )
  }
)
