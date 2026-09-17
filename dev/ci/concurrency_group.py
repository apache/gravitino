#!/usr/bin/env python3
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

"""Evaluate concurrency group templates under GitHub's reusable-workflow context.

GitHub does not ship a Python library that models caller vs callee ``github``
context. actionlint checks workflow syntax; it does not evaluate that
``github.workflow`` and ``github.event_name`` stay the caller's values inside
a ``workflow_call`` run. yamloom generates YAML; it does not interpret it.

This module is the source of truth for the small expression subset Required CI
uses. Tests evaluate group templates without starting Actions.
"""

import re


# GitHub interpolates these as empty / false in expressions.
_FALSY = {None, False, 0, ""}

_TOKEN = re.compile(
    r"""
    \s*
    (
        '([^'\\]|\\.)*'
      | ==
      | &&
      | \|\|
      | [A-Za-z_][A-Za-z0-9_.]*
      | \d+
      | [()]
    )
    """,
    re.VERBOSE,
)
_TEMPLATE_EXPR = re.compile(r"\$\{\{(.*?)\}\}", re.DOTALL)


def github_truthy(value):
    """Return whether GitHub treats ``value`` as true in ``&&`` / ``||``."""
    return value not in _FALSY and value != "false"


def interpolate(value):
    """Return the string GitHub would splice into a concurrency group."""
    if value is None:
        return ""
    if value is True:
        return "true"
    if value is False:
        return "false"
    return str(value)


def required_ci_call_context(pr_number=12545):
    """Return the github context a reusable suite sees when Required CI calls it.

    The github context is the caller: event_name stays ``pull_request`` and
    workflow is ``Required CI``, not ``workflow_call`` / the child file name.
    """
    return {
        "github.event_name": "pull_request",
        "github.workflow": "Required CI",
        "github.event.pull_request.number": pr_number,
        "github.ref": f"refs/pull/{pr_number}/merge",
        "inputs.required_ci": True,
    }


def push_context(workflow_name, ref="refs/heads/main"):
    """Return the github context a suite sees on a branch push, not a call."""
    return {
        "github.event_name": "push",
        "github.workflow": workflow_name,
        "github.event.pull_request.number": None,
        "github.ref": ref,
        "inputs.required_ci": None,
    }


def extract_group(source):
    """Return the top-level concurrency group template from workflow YAML."""
    match = re.search(r"(?m)^  group: (.+)$", source)
    if not match:
        raise AssertionError("missing top-level concurrency group")
    return match.group(1).strip()


def eval_group(template, context):
    """Evaluate a concurrency group template against ``context``."""
    parts = []
    cursor = 0
    for match in _TEMPLATE_EXPR.finditer(template):
        parts.append(template[cursor : match.start()])
        parts.append(interpolate(eval_expr(match.group(1), context)))
        cursor = match.end()
    parts.append(template[cursor:])
    return "".join(parts)


def eval_expr(expression, context):
    """Evaluate one GitHub expression (``&&``, ``||``, ``==``, identifiers)."""
    tokens = tokenize(expression)
    value, index = _parse_or(tokens, 0, context)
    if index != len(tokens):
        raise AssertionError(
            f"unparsed expression tokens {tokens[index:]!r} in {expression!r}"
        )
    return value


def tokenize(expression):
    """Return tokens for a GitHub expression, skipping whitespace."""
    tokens = []
    cursor = 0
    length = len(expression)
    while cursor < length:
        if expression[cursor].isspace():
            cursor += 1
            continue
        match = _TOKEN.match(expression, cursor)
        if not match:
            raise AssertionError(
                f"cannot tokenize {expression[cursor:]!r} in {expression!r}"
            )
        tokens.append(match.group(1))
        cursor = match.end()
    return tokens


def collisions(groups):
    """Return resolved group strings that map to more than one suite."""
    inverted = {}
    for suite, group in groups.items():
        inverted.setdefault(group, []).append(suite)
    return {group: suites for group, suites in inverted.items() if len(suites) > 1}


def assert_unique(label, groups):
    """Fail when two suites resolve to the same concurrency group."""
    clobbered = collisions(groups)
    if clobbered:
        raise AssertionError(f"{label}: concurrency groups collide: {clobbered}")


def _parse_or(tokens, index, context):
    value, index = _parse_and(tokens, index, context)
    while index < len(tokens) and tokens[index] == "||":
        right, index = _parse_and(tokens, index + 1, context)
        value = value if github_truthy(value) else right
    return value, index


def _parse_and(tokens, index, context):
    value, index = _parse_eq(tokens, index, context)
    while index < len(tokens) and tokens[index] == "&&":
        right, index = _parse_eq(tokens, index + 1, context)
        value = right if github_truthy(value) else value
    return value, index


def _parse_eq(tokens, index, context):
    value, index = _parse_primary(tokens, index, context)
    if index < len(tokens) and tokens[index] == "==":
        right, index = _parse_primary(tokens, index + 1, context)
        return value == right, index
    return value, index


def _parse_primary(tokens, index, context):
    if index >= len(tokens):
        raise AssertionError("unexpected end of GitHub expression")
    token = tokens[index]
    if token == "(":
        value, index = _parse_or(tokens, index + 1, context)
        if index >= len(tokens) or tokens[index] != ")":
            raise AssertionError("missing closing parenthesis")
        return value, index + 1
    if token.startswith("'"):
        return token[1:-1], index + 1
    if token == "true":
        return True, index + 1
    if token == "false":
        return False, index + 1
    if token.isdigit():
        return int(token), index + 1
    if token in context:
        return context[token], index + 1
    # Undefined github / inputs fields are empty, not a parse error.
    return None, index + 1
