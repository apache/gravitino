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

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.dto.requests.OntologyMappingCreateRequest;
import org.apache.gravitino.dto.requests.OntologyUpdatesRequest;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.EntityListResponse;
import org.apache.gravitino.dto.responses.OntologyMappingResponse;
import org.apache.gravitino.ontology.OntologyMapping;
import org.apache.gravitino.ontology.OntologyMappingChange;
import org.apache.gravitino.ontology.OntologyMappingDefinition;
import org.apache.gravitino.ontology.SupportsOntologyMappings;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.semantic.OssieDocument;
import org.apache.gravitino.semantic.OssieFormat;
import org.apache.hc.core5.http.HttpHeaders;

/** REST client for mappings of a semantic model. */
final class OntologyMappingClientOperations implements SupportsOntologyMappings {

  private final RESTClient client;
  private final String path;

  OntologyMappingClientOperations(RESTClient client, Namespace modelNamespace, String model) {
    this.client = client;

    this.path =
        SemanticModelCatalogOperations.formatSemanticModelRequestPath(modelNamespace)
            + "/"
            + RESTUtils.encodeString(model)
            + "/ontology-mappings";
  }

  /** {@inheritDoc} */
  @Override
  public String[] listOntologyMappings() {
    EntityListResponse response =
        client.get(
            path,
            EntityListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.ontologyErrorHandler());
    response.validate();
    return Arrays.stream(response.identifiers()).map(NameIdentifier::name).toArray(String[]::new);
  }

  /** {@inheritDoc} */
  @Override
  public OntologyMapping loadOntologyMapping(String name) {
    return result(
        client.get(
            item(name),
            OntologyMappingResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.ontologyErrorHandler()));
  }

  /** {@inheritDoc} */
  @Override
  public OntologyMapping createOntologyMapping(
      String name,
      @Nullable String comment,
      String ontology,
      OntologyMappingDefinition definition,
      Map<String, String> properties) {
    Preconditions.checkArgument(definition != null, "definition is required");
    OntologyMappingCreateRequest request =
        new OntologyMappingCreateRequest(name, comment, ontology, definition.fields(), properties);
    request.validate();
    return result(
        client.post(
            path,
            request,
            OntologyMappingResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.ontologyErrorHandler()));
  }

  /** {@inheritDoc} */
  @Override
  public OntologyMapping importOntologyMapping(OssieDocument document) {
    Preconditions.checkArgument(document != null, "document is required");
    return result(
        client.postRaw(
            path + "/ossie",
            document.content(),
            SemanticModelCatalogOperations.ossieMediaType(document.format()),
            OntologyMappingResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.ontologyErrorHandler()));
  }

  /** {@inheritDoc} */
  @Override
  public OssieDocument exportOntologyMapping(String name, OssieFormat format) {
    Preconditions.checkArgument(format != null, "format is required");
    String content =
        client.getRaw(
            item(name) + "/ossie",
            Collections.singletonMap("format", format.name().toLowerCase(Locale.ROOT)),
            Collections.singletonMap(
                HttpHeaders.ACCEPT, SemanticModelCatalogOperations.ossieMediaType(format)),
            ErrorHandlers.ontologyErrorHandler());
    return format == OssieFormat.JSON ? OssieDocument.json(content) : OssieDocument.yaml(content);
  }

  /** {@inheritDoc} */
  @Override
  public OntologyMapping alterOntologyMapping(String name, OntologyMappingChange... changes) {
    Preconditions.checkArgument(changes != null, "changes are required");
    OntologyUpdatesRequest request = OntologyUpdatesRequest.fromMappingChanges(changes);
    request.validate();
    return result(
        client.put(
            item(name),
            request,
            OntologyMappingResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.ontologyErrorHandler()));
  }

  /** {@inheritDoc} */
  @Override
  public boolean dropOntologyMapping(String name) {
    DropResponse response =
        client.delete(
            item(name),
            DropResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.ontologyErrorHandler());
    response.validate();
    return response.dropped();
  }

  private static OntologyMapping result(OntologyMappingResponse response) {
    response.validate();
    return response.ontologyMapping();
  }

  private String item(String name) {
    Preconditions.checkArgument(name != null && !name.trim().isEmpty(), "name is required");
    return path + "/" + RESTUtils.encodeString(name);
  }
}
