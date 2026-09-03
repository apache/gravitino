<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# OpenAPI specifications

## Default API bundle

The YAML files in this directory are the editable source for the default Gravitino REST API.
`openapi.yaml` is the entry point and references the other YAML files that define the API.

Run the following command from the repository root to resolve the complete default API into
self-contained JSON and YAML documents:

```shell
./gradlew :docs:bundleOpenApi
```

The task writes the generated bundles to:

```text
docs/open-api/default/openapi.json
docs/open-api/default/openapi.yaml
```

Do not edit the generated bundles directly. When a change affects the default OpenAPI YAML source,
regenerate `openapi.json` and `openapi.yaml` and commit them in the same change. The standard
`./gradlew :docs:build` command also regenerates both bundles before validating the OpenAPI
specifications.

Downstream consumers should select a Gravitino Git revision and read a bundle from the paths above
at that revision. For example:

```text
https://raw.githubusercontent.com/apache/gravitino/<revision>/docs/open-api/default/openapi.json
https://raw.githubusercontent.com/apache/gravitino/<revision>/docs/open-api/default/openapi.yaml
```

The identity provider API under `idp/` remains a separate specification and is not part of the
default API bundle.
