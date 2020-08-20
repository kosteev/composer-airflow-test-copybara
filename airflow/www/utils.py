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
import json
import time
from urllib.parse import urlencode

import markdown
import sqlalchemy as sqla
from flask import Markup, Response, request, url_for
from flask_appbuilder.forms import FieldConverter
from flask_appbuilder.models.sqla import filters as fab_sqlafilters
from flask_appbuilder.models.sqla.interface import SQLAInterface
from pygments import highlight, lexers
from pygments.formatters import HtmlFormatter

from airflow.configuration import conf
from airflow.utils import timezone
from airflow.utils.code_utils import get_python_source
from airflow.utils.json import AirflowJsonEncoder
from airflow.utils.state import State
from airflow.www.forms import DateTimeWithTimezoneField
from airflow.www.widgets import AirflowDateTimePickerWidget

DEFAULT_SENSITIVE_VARIABLE_FIELDS = [
    'password',
    'secret',
    'passwd',
    'authorization',
    'api_key',
    'apikey',
    'access_token',
]


def get_sensitive_variables_fields():
    """Get comma-separated sensitive Variable Fields from airflow.cfg."""
    sensitive_fields = set(DEFAULT_SENSITIVE_VARIABLE_FIELDS)
    sensitive_variable_fields = conf.get('admin', 'sensitive_variable_fields')
    if sensitive_variable_fields:
        sensitive_fields.update(set(field.strip() for field in sensitive_variable_fields.split(',')))
    return sensitive_fields


def should_hide_value_for_key(key_name):
    """Returns True if hide_sensitive_variable_fields is True, else False """
    # It is possible via importing variables from file that a key is empty.
    if key_name:
        config_set = conf.getboolean('admin', 'hide_sensitive_variable_fields')

        field_comp = any(s in key_name.strip().lower() for s in get_sensitive_variables_fields())
        return config_set and field_comp
    return False


def get_params(**kwargs):
    """Return URL-encoded params"""
    return urlencode({d: v for d, v in kwargs.items() if v is not None})


def generate_pages(current_page,
                   num_of_pages,
                   search=None,
                   status=None,
                   window=7):
    """
    Generates the HTML for a paging component using a similar logic to the paging
    auto-generated by Flask managed views. The paging component defines a number of
    pages visible in the pager (window) and once the user goes to a page beyond the
    largest visible, it would scroll to the right the page numbers and keeps the
    current one in the middle of the pager component. When in the last pages,
    the pages won't scroll and just keep moving until the last page. Pager also contains
    <first, previous, ..., next, last> pages.
    This component takes into account custom parameters such as search and status,
    which could be added to the pages link in order to maintain the state between
    client and server. It also allows to make a bookmark on a specific paging state.

    :param current_page: the current page number, 0-indexed
    :param num_of_pages: the total number of pages
    :param search: the search query string, if any
    :param status: 'all', 'active', or 'paused'
    :param window: the number of pages to be shown in the paging component (7 default)
    :return: the HTML string of the paging component
    """

    void_link = 'javascript:void(0)'
    first_node = Markup("""<li class="paginate_button {disabled}" id="dags_first">
    <a href="{href_link}" aria-controls="dags" data-dt-idx="0" tabindex="0">&laquo;</a>
</li>""")

    previous_node = Markup("""<li class="paginate_button previous {disabled}" id="dags_previous">
    <a href="{href_link}" aria-controls="dags" data-dt-idx="0" tabindex="0">&lsaquo;</a>
</li>""")

    next_node = Markup("""<li class="paginate_button next {disabled}" id="dags_next">
    <a href="{href_link}" aria-controls="dags" data-dt-idx="3" tabindex="0">&rsaquo;</a>
</li>""")

    last_node = Markup("""<li class="paginate_button {disabled}" id="dags_last">
    <a href="{href_link}" aria-controls="dags" data-dt-idx="3" tabindex="0">&raquo;</a>
</li>""")

    page_node = Markup("""<li class="paginate_button {is_active}">
    <a href="{href_link}" aria-controls="dags" data-dt-idx="2" tabindex="0">{page_num}</a>
</li>""")

    output = [Markup('<ul class="pagination" style="margin-top:0;">')]

    is_disabled = 'disabled' if current_page <= 0 else ''
    output.append(first_node.format(href_link="?{}"
                                    .format(get_params(page=0,
                                                       search=search,
                                                       status=status)),
                                    disabled=is_disabled))

    page_link = void_link
    if current_page > 0:
        page_link = '?{}'.format(get_params(page=(current_page - 1),
                                            search=search,
                                            status=status))

    output.append(previous_node.format(href_link=page_link,
                                       disabled=is_disabled))

    mid = int(window / 2)
    last_page = num_of_pages - 1

    if current_page <= mid or num_of_pages < window:
        pages = list(range(0, min(num_of_pages, window)))
    elif mid < current_page < last_page - mid:
        pages = list(range(current_page - mid, current_page + mid + 1))
    else:
        pages = list(range(num_of_pages - window, last_page + 1))

    def is_current(current, page):
        return page == current

    for page in pages:
        vals = {
            'is_active': 'active' if is_current(current_page, page) else '',
            'href_link': void_link if is_current(current_page, page)
                         else '?{}'.format(get_params(page=page,
                                                      search=search,
                                                      status=status)),
            'page_num': page + 1
        }
        output.append(page_node.format(**vals))

    is_disabled = 'disabled' if current_page >= num_of_pages - 1 else ''

    page_link = (void_link if current_page >= num_of_pages - 1
                 else '?{}'.format(get_params(page=current_page + 1,
                                              search=search,
                                              status=status)))

    output.append(next_node.format(href_link=page_link, disabled=is_disabled))
    output.append(last_node.format(href_link="?{}"
                                   .format(get_params(page=last_page,
                                                      search=search,
                                                      status=status)),
                                   disabled=is_disabled))

    output.append(Markup('</ul>'))

    return Markup('\n'.join(output))


def epoch(dttm):
    """Returns an epoch-type date"""
    return int(time.mktime(dttm.timetuple())) * 1000,


def json_response(obj):
    """
    returns a json response from a json serializable python object
    """
    return Response(
        response=json.dumps(
            obj, indent=4, cls=AirflowJsonEncoder),
        status=200,
        mimetype="application/json")


def make_cache_key(*args, **kwargs):
    """
    Used by cache to get a unique key per URL
    """
    path = request.path
    args = str(hash(frozenset(request.args.items())))
    return (path + args).encode('ascii', 'ignore')


def task_instance_link(attr):
    """Generates a URL to the Graph View for a TaskInstance."""
    dag_id = attr.get('dag_id')
    task_id = attr.get('task_id')
    execution_date = attr.get('execution_date')
    url = url_for(
        'Airflow.task',
        dag_id=dag_id,
        task_id=task_id,
        execution_date=execution_date.isoformat())
    url_root = url_for(
        'Airflow.graph',
        dag_id=dag_id,
        root=task_id,
        execution_date=execution_date.isoformat())
    return Markup(
        """
        <span style="white-space: nowrap;">
        <a href="{url}">{task_id}</a>
        <a href="{url_root}" title="Filter on this task and upstream">
        <span class="glyphicon glyphicon-filter" style="margin-left:0;"
            aria-hidden="true"></span>
        </a>
        </span>
        """).format(url=url, task_id=task_id, url_root=url_root)


def state_token(state):
    """Returns a formatted string with HTML for a given State"""
    color = State.color(state)
    return Markup(
        '<span class="label" style="background-color:{color};">'
        '{state}</span>').format(color=color, state=state)


def state_f(attr):
    """Gets 'state' & returns a formatted string with HTML for a given State"""
    state = attr.get('state')
    return state_token(state)


def nobr_f(attr_name):
    """Returns a formatted string with HTML with a Non-breaking Text element"""
    def nobr(attr):
        f = attr.get(attr_name)
        return Markup("<nobr>{}</nobr>").format(f)
    return nobr


def datetime_f(attr_name):
    """Returns a formatted string with HTML for given DataTime"""
    def dt(attr):
        f = attr.get(attr_name)
        as_iso = f.isoformat() if f else ''
        if not as_iso:
            return Markup('')
        f = as_iso
        if timezone.utcnow().isoformat()[:4] == f[:4]:
            f = f[5:]
        # The empty title will be replaced in JS code when non-UTC dates are displayed
        return Markup('<nobr><time title="" datetime="{}">{}</time></nobr>').format(as_iso, f)
    return dt


def dag_link(attr):
    """Generates a URL to the Graph View for a Dag."""
    dag_id = attr.get('dag_id')
    execution_date = attr.get('execution_date')
    url = url_for(
        'Airflow.graph',
        dag_id=dag_id,
        execution_date=execution_date)
    return Markup(
        '<a href="{}">{}</a>').format(url, dag_id)


def dag_run_link(attr):
    """Generates a URL to the Graph View for a DagRun."""
    dag_id = attr.get('dag_id')
    run_id = attr.get('run_id')
    execution_date = attr.get('execution_date')
    url = url_for(
        'Airflow.graph',
        dag_id=dag_id,
        run_id=run_id,
        execution_date=execution_date)
    return Markup(
        '<a href="{url}">{run_id}</a>').format(url=url, run_id=run_id)


def pygment_html_render(s, lexer=lexers.TextLexer):
    """Highlight text using a given Lexer"""
    return highlight(s, lexer(), HtmlFormatter(linenos=True))


def render(obj, lexer):
    """Render a given Python object with a given Pygments lexer"""
    out = ""
    if isinstance(obj, str):
        out = Markup(pygment_html_render(obj, lexer))
    elif isinstance(obj, (tuple, list)):
        for i, s in enumerate(obj):
            out += Markup("<div>List item #{}</div>").format(i)
            out += Markup("<div>" + pygment_html_render(s, lexer) + "</div>")
    elif isinstance(obj, dict):
        for k, v in obj.items():
            out += Markup('<div>Dict item "{}"</div>').format(k)
            out += Markup("<div>" + pygment_html_render(v, lexer) + "</div>")
    return out


def wrapped_markdown(s, css_class=None):
    """Convert a Markdown string to HTML."""
    if s is None:
        return None

    return Markup(
        '<div class="rich_doc {css_class}" >'.format(css_class=css_class) + markdown.markdown(s) + "</div>"
    )


def get_attr_renderer():
    """Return Dictionary containing different Pygements Lexers for Rendering & Highlighting"""
    return {
        'bash_command': lambda x: render(x, lexers.BashLexer),
        'hql': lambda x: render(x, lexers.SqlLexer),
        'sql': lambda x: render(x, lexers.SqlLexer),
        'doc': lambda x: render(x, lexers.TextLexer),
        'doc_json': lambda x: render(x, lexers.JsonLexer),
        'doc_rst': lambda x: render(x, lexers.RstLexer),
        'doc_yaml': lambda x: render(x, lexers.YamlLexer),
        'doc_md': wrapped_markdown,
        'python_callable': lambda x: render(get_python_source(x), lexers.PythonLexer),
    }


def get_chart_height(dag):
    """
    TODO(aoen): See [AIRFLOW-1263] We use the number of tasks in the DAG as a heuristic to
    approximate the size of generated chart (otherwise the charts are tiny and unreadable
    when DAGs have a large number of tasks). Ideally nvd3 should allow for dynamic-height
    charts, that is charts that take up space based on the size of the components within.
    """
    return 600 + len(dag.tasks) * 10


class UtcAwareFilterMixin:  # noqa: D101
    def apply(self, query, value):
        value = timezone.parse(value, timezone=timezone.utc)

        return super().apply(query, value)


class UtcAwareFilterEqual(UtcAwareFilterMixin, fab_sqlafilters.FilterEqual):  # noqa: D101
    pass


class UtcAwareFilterGreater(UtcAwareFilterMixin, fab_sqlafilters.FilterGreater):  # noqa: D101
    pass


class UtcAwareFilterSmaller(UtcAwareFilterMixin, fab_sqlafilters.FilterSmaller):  # noqa: D101
    pass


class UtcAwareFilterNotEqual(UtcAwareFilterMixin, fab_sqlafilters.FilterNotEqual):  # noqa: D101
    pass


class UtcAwareFilterConverter(fab_sqlafilters.SQLAFilterConverter):  # noqa: D101

    conversion_table = (
        (('is_utcdatetime', [UtcAwareFilterEqual,
                             UtcAwareFilterGreater,
                             UtcAwareFilterSmaller,
                             UtcAwareFilterNotEqual]),) +
        fab_sqlafilters.SQLAFilterConverter.conversion_table
    )


class CustomSQLAInterface(SQLAInterface):
    """
    FAB does not know how to handle columns with leading underscores because
    they are not supported by WTForm. This hack will remove the leading
    '_' from the key to lookup the column names.

    """
    def __init__(self, obj, session=None):
        super().__init__(obj, session=session)

        def clean_column_names():
            if self.list_properties:
                self.list_properties = {
                    k.lstrip('_'): v for k, v in self.list_properties.items()}
            if self.list_columns:
                self.list_columns = {
                    k.lstrip('_'): v for k, v in self.list_columns.items()}

        clean_column_names()

    def is_utcdatetime(self, col_name):
        from airflow.utils.sqlalchemy import UtcDateTime

        if col_name in self.list_columns:
            obj = self.list_columns[col_name].type
            return isinstance(obj, UtcDateTime) or \
                isinstance(obj, sqla.types.TypeDecorator) and \
                isinstance(obj.impl, UtcDateTime)
        return False

    filter_converter_class = UtcAwareFilterConverter


# This class is used directly (i.e. we cant tell Fab to use a different
# subclass) so we have no other option than to edit the converstion table in
# place
FieldConverter.conversion_table = (
    (('is_utcdatetime', DateTimeWithTimezoneField, AirflowDateTimePickerWidget),) +
    FieldConverter.conversion_table
)
