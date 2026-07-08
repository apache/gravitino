#!/usr/bin/env bash
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
#
# Render every diagrams/*.mmd to images/<name>.png using mermaid-cli.
# Run after editing any .mmd source so the embedded PNGs stay in sync.
#
# Usage:
#   ./diagrams/render.sh            # render all diagrams
#   ./diagrams/render.sh foo bar    # render only diagrams/foo.mmd and diagrams/bar.mmd
#
# Requires Node.js (npx). mermaid-cli is fetched on demand via npx.

set -euo pipefail

# Resolve paths relative to this script so it works from any CWD.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC_DIR="$SCRIPT_DIR"
OUT_DIR="$SCRIPT_DIR/../images"

# Render settings (must match how the existing PNGs were produced):
#   -b white : white background, embeds cleanly in any renderer
#   -s 2     : 2x scale for crisp text
BG="white"
SCALE="2"

if ! command -v npx >/dev/null 2>&1; then
  echo "error: npx not found (install Node.js)" >&2
  exit 1
fi

mkdir -p "$OUT_DIR"

# Build the list of .mmd files to render.
if [ "$#" -gt 0 ]; then
  files=()
  for name in "$@"; do
    files+=("$SRC_DIR/${name%.mmd}.mmd")
  done
else
  files=("$SRC_DIR"/*.mmd)
fi

count=0
for f in "${files[@]}"; do
  if [ ! -f "$f" ]; then
    echo "skip: $f not found" >&2
    continue
  fi
  name="$(basename "${f%.mmd}")"
  echo "rendering $name.png"
  npx -y @mermaid-js/mermaid-cli -i "$f" -o "$OUT_DIR/$name.png" -b "$BG" -s "$SCALE" >/dev/null
  count=$((count + 1))
done

echo "done: $count diagram(s) -> $OUT_DIR"
