/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.client;

import java.util.Collections;
import java.util.Locale;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.dto.responses.EntityListResponse;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.semantic.SupportsSemanticModels;

/** Shared relationship request handling for tables and Views. */
class MetadataObjectSemanticModelOperations implements SupportsSemanticModels {
  private final RESTClient restClient;
  private final String requestPath;

  MetadataObjectSemanticModelOperations(
      String metalake, MetadataObject source, RESTClient restClient) {
    this.restClient = restClient;
    this.requestPath =
        String.format(
            "api/metalakes/%s/objects/%s/%s/semantic-model-datasets",
            RESTUtils.encodeString(metalake),
            source.type().name().toLowerCase(Locale.ROOT),
            RESTUtils.encodeString(source.fullName()));
  }

  @Override
  public NameIdentifier[] listSemanticModelDatasets() {
    EntityListResponse response =
        restClient.get(
            requestPath,
            EntityListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.semanticModelErrorHandler());
    response.validate();
    return response.identifiers();
  }
}
