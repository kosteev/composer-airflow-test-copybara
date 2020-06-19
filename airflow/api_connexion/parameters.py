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
from functools import wraps
from typing import Callable, Dict

from pendulum.parsing import ParserError

from airflow.api_connexion.exceptions import BadRequest
from airflow.utils import timezone

# Page parameters
page_offset = "offset"
page_limit = "limit"

# Database entity fields
dag_id = "dag_id"
pool_id = "pool_id"


def format_datetime(value: str):
    """
    Datetime format parser for args since connexion doesn't parse datetimes
    https://github.com/zalando/connexion/issues/476

    This should only be used within connection views because it raises 400
    """
    if value[-1] != 'Z':
        value = value.replace(" ", '+')
    try:
        return timezone.parse(value)
    except (ParserError, TypeError) as err:
        raise BadRequest(
            "Incorrect datetime argument", detail=str(err)
        )


def format_parameters(params_formatters: Dict[str, Callable[..., bool]]):
    """
    Decorator factory that create decorator that convert parameters using given formatters.

    Using it allows you to separate parameter formatting from endpoint logic.

    :param params_formatters: Map of key name and formatter function
    """
    def format_parameters_decorator(func):
        @wraps(func)
        def wrapped_function(*args, **kwargs):
            for key, formatter in params_formatters.items():
                if key in kwargs:
                    kwargs[key] = formatter(kwargs[key])
            return func(*args, **kwargs)
        return wrapped_function
    return format_parameters_decorator
