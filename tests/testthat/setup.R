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

old_spark_conf_dir <- Sys.getenv("SPARK_CONF_DIR")
Sys.setenv(SPARK_CONF_DIR = test_path("conf"))

SparkR::sparkR.session()

withr::defer(
  { # nolint
    SparkR::sparkR.session.stop()
    Sys.setenv(SPARK_CONF_DIR = old_spark_conf_dir)
    unlink(testthat::test_path("metastore_db"), recursive = TRUE)
    unlink(testthat::test_path("spark-warehouse"), recursive = TRUE)
  },
  teardown_env()
)
