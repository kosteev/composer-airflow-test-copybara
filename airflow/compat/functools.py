#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import sys

if sys.version_info >= (3, 8):
    from functools import cached_property  # pylint: disable=no-name-in-module
else:
    from cached_property import cached_property

if sys.version_info >= (3, 9):
    from functools import cache  # pylint: disable=no-name-in-module
else:
    from functools import lru_cache

    cache = lru_cache(maxsize=None)


__all__ = ["cache", "cached_property"]
