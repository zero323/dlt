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

#' @name dlt_write
#' @rdname dlt_write
setGeneric("dlt_write", function(df, path, ...) {
  standardGeneric("dlt_write")
})


#' @name write.delta
#' @rdname write.delta
setGeneric("write.delta", function(df, path, ...) {
  standardGeneric("write.delta")
})


#' @name dlt_to_df
#' @rdname dlt_to_df
setGeneric("dlt_to_df", function(dt) {
  standardGeneric("dlt_to_df")
})


#' @name dlt_alias
#' @rdname dlt_alias
setGeneric("dlt_alias", function(dt, name) {
  standardGeneric("dlt_alias")
})


#' @name dlt_delete
#' @rdname dlt_delete
setGeneric("dlt_delete", function(dt, condition) {
  standardGeneric("dlt_delete")
})


#' @name dlt_update
#' @rdname dlt_update
setGeneric("dlt_update", function(dt, set, condition) {
  standardGeneric("dlt_update")
})


#' @name dlt_is_delta_table
#' @rdname dlt_is_delta_table
setGeneric("dlt_is_delta_table", function(identifier) {
  standardGeneric("dlt_is_delta_table")
})


#' @name dlt_history
#' @rdname dlt_history
setGeneric("dlt_history", function(dt, limit) {
  standardGeneric("dlt_history")
})


#' @name dlt_convert_to_delta
#' @rdname dlt_convert_to_delta
setGeneric("dlt_convert_to_delta", function(identifier, partition_schema) {
  standardGeneric("dlt_convert_to_delta")
})


#' @name dlt_vacuum
#' @rdname dlt_vacuum
setGeneric("dlt_vacuum", function(dt, retention_hours) {
  standardGeneric("dlt_vacuum")
})


#' @name dlt_upgrade_table_protocol
#' @rdname dlt_upgrade_table_protocol
setGeneric("dlt_upgrade_table_protocol", function(dt, reader_version, writer_version) {
  standardGeneric("dlt_upgrade_table_protocol")
})


#' @name dlt_generate_manifest
#' @rdname dlt_generate_manifest
setGeneric("dlt_generate_manifest", function(dt, mode) {
  standardGeneric("dlt_generate_manifest")
})


#' @name dlt_merge
#' @rdname dlt_merge
setGeneric("dlt_merge", function(dt, source, condition) {
  standardGeneric("dlt_merge")
})


#' @name dlt_execute
#' @rdname dlt_execute
setGeneric(
  "dlt_execute",
  function(bldr) {
    standardGeneric("dlt_execute")
  }
)


#' @name dlt_when_matched_update
#' @rdname dlt_when_matched_update
setGeneric(
  "dlt_when_matched_update",
  function(dmb, set, condition) {
    standardGeneric("dlt_when_matched_update")
  }
)


#' @name dlt_when_matched_update_all
#' @rdname dlt_when_matched_update_all
setGeneric(
  "dlt_when_matched_update_all",
  function(dmb, condition) {
    standardGeneric("dlt_when_matched_update_all")
  }
)


#' @name dlt_when_matched_delete
#' @rdname dlt_when_matched_delete
setGeneric(
  "dlt_when_matched_delete",
  function(dmb, condition) {
    standardGeneric("dlt_when_matched_delete")
  }
)


#' @name dlt_when_not_matched_insert
#' @rdname dlt_when_not_matched_insert
setGeneric(
  "dlt_when_not_matched_insert",
  function(dmb, set, condition) {
    standardGeneric("dlt_when_not_matched_insert")
  }
)


#' @name dlt_when_not_matched_insert_all
#' @rdname dlt_when_not_matched_insert_all
setGeneric(
  "dlt_when_not_matched_insert_all",
  function(dmb, condition) {
    standardGeneric("dlt_when_not_matched_insert_all")
  }
)


#' @name dlt_location
#' @rdname dlt_location
setGeneric(
  "dlt_location",
  function(dtb, location) {
    standardGeneric("dlt_location")
  }
)


#' @name dlt_table_name
#' @rdname dlt_table_name
setGeneric(
  "dlt_table_name",
  function(dtb, identifier) {
    standardGeneric("dlt_table_name")
  }
)


#' @name dlt_add_column
#' @rdname dlt_add_column
setGeneric(
  "dlt_add_column",
  function(dtb, col_name, data_type, ...) {
    standardGeneric("dlt_add_column")
  }
)
