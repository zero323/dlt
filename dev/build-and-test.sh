#!/usr/bin/env bash
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

DLT_VERSION=$(grep -oP "(?<=Version:\ ).*" DESCRIPTION)
R CMD build .
R CMD check --no-tests --no-manual dlt_$DLT_VERSION.tar.gz
Rscript -e 'print(covr::package_coverage())'
