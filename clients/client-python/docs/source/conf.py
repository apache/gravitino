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

# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information
import os
import sys
import re

sys.path.insert(0, os.path.abspath('../..'))

from gravitino.constants.version import SETUP_FILE
from gravitino.exceptions.base import GravitinoRuntimeException

VERSION_PATTERN = r"version\s*=\s*['\"]([^'\"]+)['\"]"
AUTHOR_PATTERN = r"author\s*=\s*['\"]([^'\"]+)['\"]"


with open(SETUP_FILE, "r", encoding="utf-8") as f:
    setup_content = f.read()

    version_group = re.search(VERSION_PATTERN, setup_content)
    if version_group is not None:
        project_version = version_group.group(1)
    else:
        raise GravitinoRuntimeException("Can't find valid author info in setup.py")
    
    author_group = re.search(AUTHOR_PATTERN, setup_content)
    if author_group is not None:
        project_author = author_group.group(1)
    else:
        raise GravitinoRuntimeException("Can't find valid author info in setup.py")

project = 'Apache Gravitino Python Client'
copyright = '2024, Apache Software Foundation'
author = project_author
release = project_version

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration


extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
]

templates_path = ['_templates']
exclude_patterns = []

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'furo'
html_static_path = ['_static']
