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