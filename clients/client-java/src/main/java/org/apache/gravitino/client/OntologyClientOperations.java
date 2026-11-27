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
import org.apache.gravitino.dto.requests.OntologyCreateRequest;
import org.apache.gravitino.dto.requests.OntologyUpdatesRequest;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.EntityListResponse;
import org.apache.gravitino.dto.responses.NameListResponse;
import org.apache.gravitino.dto.responses.OntologyResponse;
import org.apache.gravitino.ontology.Ontology;
import org.apache.gravitino.ontology.OntologyChange;
import org.apache.gravitino.ontology.OntologyDefinition;
import org.apache.gravitino.ontology.SupportsOntologies;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.semantic.OssieDocument;
import org.apache.gravitino.semantic.OssieFormat;
import org.apache.hc.core5.http.HttpHeaders;

/** REST client for ontologies of a metalake. */
final class OntologyClientOperations implements SupportsOntologies {

  private final RESTClient client;
  private final String path;
  private final String metalake;

  OntologyClientOperations(RESTClient client, String metalake) {
    this.client = client;
    this.metalake = metalake;
    this.path = "api/metalakes/" + RESTUtils.encodeString(metalake) + "/ontologies";
  }

  /** {@inheritDoc} */
  @Override
  public String[] listOntologies() {
    NameListResponse response =
        client.get(
            path,
            NameListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.ontologyErrorHandler());
    response.validate();
    return response.getNames();
  }

  /** {@inheritDoc} */
  @Override
  public Ontology loadOntology(String name) {
    return result(
        client.get(
            item(name),
            OntologyResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.ontologyErrorHandler()));
  }

  /** {@inheritDoc} */
  @Override
  public Ontology createOntology(
      String name,
      @Nullable String comment,
      OntologyDefinition definition,
      Map<String, String> properties) {
    Preconditions.checkArgument(definition != null, "definition is required");
    OntologyCreateRequest request =
        new OntologyCreateRequest(name, comment, definition.fields(), properties);
    request.validate();
    return result(
        client.post(
            path,
            request,
            OntologyResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.ontologyErrorHandler()));
  }

  /** {@inheritDoc} */
  @Override
  public Ontology importOntology(OssieDocument document) {
    Preconditions.checkArgument(document != null, "document is required");
    return result(
        client.postRaw(
            path + "/ossie",
            document.content(),
            SemanticModelCatalogOperations.ossieMediaType(document.format()),
            OntologyResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.ontologyErrorHandler()));
  }

  /** {@inheritDoc} */
  @Override
  public OssieDocument exportOntology(String name, OssieFormat format) {
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
  public Ontology alterOntology(String name, OntologyChange... changes) {
    Preconditions.checkArgument(changes != null, "changes are required");
    OntologyUpdatesRequest request = OntologyUpdatesRequest.fromOntologyChanges(changes);
    request.validate();
    return result(
        client.put(
            item(name),
            request,
            OntologyResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.ontologyErrorHandler()));
  }

  /** {@inheritDoc} */
  @Override
  public boolean dropOntology(String name) {
    DropResponse response =
        client.delete(
            item(name),
            DropResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.ontologyErrorHandler());
    response.validate();
    return response.dropped();
  }

  /** {@inheritDoc} */
  @Override
  public NameIdentifier[] listOntologyMappings(String name) {
    EntityListResponse response =
        client.get(
            item(name) + "/mappings",
            EntityListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.ontologyErrorHandler());
    response.validate();
    return Arrays.stream(response.identifiers())
        .map(
            id -> {
              Namespace namespace = id.namespace();
              if (namespace.length() == 4) {
                return NameIdentifier.of(
                    namespace.level(1), namespace.level(2), namespace.level(3), id.name());
              }
              return id;
            })
        .toArray(NameIdentifier[]::new);
  }

  private Ontology result(OntologyResponse response) {
    response.validate();
    return new GenericOntology(response.ontology(), metalake, client);
  }

  private String item(String name) {
    Preconditions.checkArgument(name != null && !name.trim().isEmpty(), "name is required");
    return path + "/" + RESTUtils.encodeString(name);
  }
}
