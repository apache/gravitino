#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -e

if ! command -v cargo &> /dev/null; then
    echo "Rust is not installed. Installing Rust..."

    if command -v curl &> /dev/null; then
        curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    elif command -v wget &> /dev/null; then
        wget -qO- https://sh.rustup.rs | sh -s -- -y
    else
        echo "Error: Neither curl nor wget is available. Please install one of them to proceed."
        exit 1
    fi

    export PATH="$HOME/.cargo/bin:$PATH"
    if command -v cargo &> /dev/null; then
        echo "Rust has been installed successfully."
    else
        echo "Error: Rust installation failed. Please check your setup."
        exit 1
    fi
else
    echo "Rust is already installed: $(cargo --version)"
fi
