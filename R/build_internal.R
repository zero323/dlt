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

#' @include generics.R package.R utils.R
NULL


#' S4 class that represents pending table build operation
#'
#' This class is used primarily to support `DeltaTableBuilder`.
#' Internally, it holds reference to a mutable Java `DeltaTableBuilder`
#' and all implemented methods mutate it in place.
#' While it is possible to use it directly, `DeltaTableBuilder`
#' is preferred and provides the same API.
#'
#' @family InternalDeltaTableBuilder functions
#' @rdname InternalDeltaTableBuilder
#' @docType class
#'
#' @slot jtb A Java object reference to the backing InternalDeltaTableBuilder
#'
#' @note InternalDeltaTableBuilder, since 1.0.0
#' @seealso DeltaTableBuilder
setClass("InternalDeltaTableBuilder", slots = c(jtb = "jobj"))


initializeInternalDeltaTableBuilder <- function(initializer) { # nolint
  new(
    "InternalDeltaTableBuilder",
    jtb = invoke_delta_table_static(
      initializer,
      active_session()
    )
  )
}


#' Execute merge operation on this builder
#'
#' @param bldr InternalDeltaTableBuilder
#' @returns DeltaTable
#'
#' @name dlt_execute
#' @rdname dlt_execute
#' @aliases dlt_execute,InternalDeltaTableBuilder-method
#'
#' @export
#' @note dlt_execute, since 1.0.0
setMethod(
  "dlt_execute",
  signature(bldr = "InternalDeltaTableBuilder"),
  function(bldr) {
    new(
      "DeltaTable",
      jdt = sparkR.callJMethod(bldr@jtb, "execute")
    )
  }
)


#' Specify data storage location for the build table.
#'
#' @param dtb InternalDeltaTableBuilder
#' @param location character, path
#' @return this InternalDeltaTableBuilder
#'
#' @name dlt_location
#' @rdname dlt_location
#' @aliases dlt_location,InternalDeltaTableBuilder,character-method
#'
#' @export
#' @note dlt_location, since 1.0.0
setMethod(
  "dlt_location",
  signature(dtb = "InternalDeltaTableBuilder", location = "character"),
  function(dtb, location) {
    validate_is_scalar_like_character(location)

    sparkR.callJMethod(dtb@jtb, "location", location)
    dtb
  }
)


#' Specify name of the build table.
#'
#' @param dtb InternalDeltaTableBuilder
#' @param identifier character
#' @return this InternalDeltaTableBuilder
#'
#' @name dlt_table_name
#' @rdname dlt_table_name
#' @aliases dlt_table_name,InternalDeltaTableBuilder,character-method
#'
#' @export
#' @note dlt_table_name, since 1.0.0
setMethod(
  "dlt_table_name",
  signature(dtb = "InternalDeltaTableBuilder", identifier = "character"),
  function(dtb, identifier) {
    validate_is_scalar_like_character(identifier)

    sparkR.callJMethod(dtb@jtb, "tableName", identifier)
    dtb
  }
)


#' Add column to the build table
#'
#'
#' @param dtb InternalDeltaTableBuilder
#' @param col_name character
#' @param data_type character
#' @param nullable optional, logical
#' @param generated_always_as optional, character
#' @param comment optional, character
#' @param ... other arguments, not used
#' @return this InternalDeltaTableBuilder
#'
#' @name dlt_add_column
#' @rdname dlt_add_column
#' @aliases dlt_add_column,InternalDeltaTableBuilder,character,character-method
#'
#' @export
#' @note dlt_add_column, since 1.0.0
setMethod(
  "dlt_add_column",
  signature(dtb = "InternalDeltaTableBuilder", col_name = "character", data_type = "character"),
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


#' Add column to the build table
#'
#'
#' @param dtb InternalDeltaTableBuilder
#' @param schema character (DDL string) or structType
#' @return this InternalDeltaTableBuilder
#'
#' @name dlt_add_columns
#' @rdname dlt_add_columns
#' @aliases dlt_add_columns,InternalDeltaTableBuilder,structType-method
#'
#' @export
#' @note dlt_add_columns, since 1.0.0
setMethod(
  "dlt_add_columns",
  signature(dtb = "InternalDeltaTableBuilder", schema = "structType"),
  function(dtb, schema) {
    sparkR.callJMethod(dtb@jtb, "addColumns", schema$jobj)
    dtb
  }
)


#' @rdname dlt_add_columns
#' @aliases dlt_add_columns,InternalDeltaTableBuilder,character-method
#'
#' @export
setMethod(
  "dlt_add_columns",
  signature(dtb = "InternalDeltaTableBuilder", schema = "character"),
  function(dtb, schema) {
    dlt_add_columns(dtb, SparkR::structType(schema))
  }
)

#' Add comment describing the build table.
#'
#' @param dtb InternalDeltaTableBuilder
#' @param comment character, path
#' @return this InternalDeltaTableBuilder
#'
#' @name dlt_comment
#' @rdname dlt_comment
#' @aliases dlt_comment,InternalDeltaTableBuilder,character-method
#'
#' @export
#' @note dlt_comment, since 1.0.0
setMethod(
  "dlt_comment",
  signature(dtb = "InternalDeltaTableBuilder", comment = "character"),
  function(dtb, comment) {
    validate_is_scalar_like_character(comment)

    sparkR.callJMethod(dtb@jtb, "comment", comment)
    dtb
  }
)


#' Specify partitioning columns
#'
#' @param dtb InternalDeltaTableBuilder
#' @param ... character columns
#'
#' @name dlt_partitioned_by
#' @rdname dlt_partitioned_by
#' @aliases dlt_partitioned_by,InternalDeltaTableBuilder-method
#'
#' @export
#' @note dlt_partitioned_by, since 1.0.0
setMethod(
  "dlt_partitioned_by",
  signature(dtb = "InternalDeltaTableBuilder"),
  function(dtb, ...) {
    cols <- prepare_and_validate_cols(...)

    sparkR.callJMethod(dtb@jtb, "partitionedBy", cols)
    dtb
  }
)


#' Set property for the build table
#'
#' @param dtb InternalDeltaTableBuilder
#' @param key character
#' @param value character
#'
#' @name dlt_property
#' @rdname dlt_property
#' @aliases dlt_property,InternalDeltaTableBuilder,character,character-method
#'
#' @export
#' @note dlt_property, since 1.0.0
setMethod(
  "dlt_property",
  signature(dtb = "InternalDeltaTableBuilder", key = "character", value = "character"),
  function(dtb, key, value) {
    validate_is_scalar_like_character(key)
    validate_is_scalar_like_character(value)

    sparkR.callJMethod(dtb@jtb, "property", key, value)
    dtb
  }
)
