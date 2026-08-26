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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.dto.requests.SemanticModelCreateRequest;
import org.apache.gravitino.dto.requests.SemanticModelUpdateRequest;
import org.apache.gravitino.dto.requests.SemanticModelUpdatesRequest;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.EntityListResponse;
import org.apache.gravitino.dto.responses.SemanticModelResponse;
import org.apache.gravitino.dto.semantic.SemanticModelDefinitionDTO;
import org.apache.gravitino.exceptions.IllegalSemanticModelException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchSemanticModelException;
import org.apache.gravitino.exceptions.SemanticModelAlreadyExistsException;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.semantic.SemanticModel;
import org.apache.gravitino.semantic.SemanticModelCatalog;
import org.apache.gravitino.semantic.SemanticModelChange;
import org.apache.gravitino.semantic.SemanticModelDefinition;

/** Implements schema-scoped Semantic Model operations through the Gravitino REST API. */
class SemanticModelCatalogOperations implements SemanticModelCatalog {

  private final RESTClient restClient;
  private final Namespace catalogNamespace;
  private final String catalogName;

  SemanticModelCatalogOperations(
      RESTClient restClient, Namespace catalogNamespace, String catalogName) {
    this.restClient = restClient;
    this.catalogNamespace = catalogNamespace;
    this.catalogName = catalogName;
  }

  /** {@inheritDoc} */
  @Override
  public NameIdentifier[] listSemanticModels(Namespace namespace) throws NoSuchSchemaException {
    checkSemanticModelNamespace(namespace);

    Namespace fullNamespace = getSemanticModelFullNamespace(namespace);
    EntityListResponse response =
        restClient.get(
            formatSemanticModelRequestPath(fullNamespace),
            EntityListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.semanticModelErrorHandler());
    response.validate();

    return Arrays.stream(response.identifiers())
        .map(ident -> NameIdentifier.of(ident.namespace().level(2), ident.name()))
        .toArray(NameIdentifier[]::new);
  }

  /** {@inheritDoc} */
  @Override
  public SemanticModel loadSemanticModel(NameIdentifier ident) throws NoSuchSemanticModelException {
    checkSemanticModelNameIdentifier(ident);

    Namespace fullNamespace = getSemanticModelFullNamespace(ident.namespace());
    SemanticModelResponse response =
        restClient.get(
            formatSemanticModelRequestPath(fullNamespace)
                + "/"
                + RESTUtils.encodeString(ident.name()),
            SemanticModelResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.semanticModelErrorHandler());
    response.validate();

    return new GenericSemanticModel(response.getSemanticModel());
  }

  /** {@inheritDoc} */
  @Override
  public SemanticModel createSemanticModel(
      NameIdentifier ident,
      @Nullable String comment,
      SemanticModelDefinition definition,
      Map<String, String> properties)
      throws NoSuchSchemaException, SemanticModelAlreadyExistsException,
          IllegalSemanticModelException {
    checkSemanticModelNameIdentifier(ident);
    Preconditions.checkArgument(definition != null, "Semantic Model definition must not be null");

    Namespace fullNamespace = getSemanticModelFullNamespace(ident.namespace());
    SemanticModelCreateRequest request =
        new SemanticModelCreateRequest(
            ident.name(),
            comment,
            SemanticModelDefinitionDTO.fromDefinition(definition),
            properties);
    request.validate();

    SemanticModelResponse response =
        restClient.post(
            formatSemanticModelRequestPath(fullNamespace),
            request,
            SemanticModelResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.semanticModelErrorHandler());
    response.validate();

    return new GenericSemanticModel(response.getSemanticModel());
  }

  /** {@inheritDoc} */
  @Override
  public SemanticModel alterSemanticModel(NameIdentifier ident, SemanticModelChange... changes)
      throws NoSuchSemanticModelException, SemanticModelAlreadyExistsException,
          IllegalSemanticModelException {
    checkSemanticModelNameIdentifier(ident);
    Preconditions.checkArgument(changes != null, "Semantic Model changes must not be null");

    List<SemanticModelUpdateRequest> updates =
        Arrays.stream(changes)
            .map(DTOConverters::toSemanticModelUpdateRequest)
            .collect(Collectors.toList());
    SemanticModelUpdatesRequest request = new SemanticModelUpdatesRequest(updates);
    request.validate();

    Namespace fullNamespace = getSemanticModelFullNamespace(ident.namespace());
    SemanticModelResponse response =
        restClient.put(
            formatSemanticModelRequestPath(fullNamespace)
                + "/"
                + RESTUtils.encodeString(ident.name()),
            request,
            SemanticModelResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.semanticModelErrorHandler());
    response.validate();

    return new GenericSemanticModel(response.getSemanticModel());
  }

  /** {@inheritDoc} */
  @Override
  public boolean dropSemanticModel(NameIdentifier ident) {
    checkSemanticModelNameIdentifier(ident);

    Namespace fullNamespace = getSemanticModelFullNamespace(ident.namespace());
    DropResponse response =
        restClient.delete(
            formatSemanticModelRequestPath(fullNamespace)
                + "/"
                + RESTUtils.encodeString(ident.name()),
            DropResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.semanticModelErrorHandler());
    response.validate();

    return response.dropped();
  }

  @VisibleForTesting
  static String formatSemanticModelRequestPath(Namespace namespace) {
    Namespace schemaNamespace = Namespace.of(namespace.level(0), namespace.level(1));
    return new StringBuilder()
        .append(BaseSchemaCatalog.formatSchemaRequestPath(schemaNamespace))
        .append("/")
        .append(RESTUtils.encodeString(namespace.level(2)))
        .append("/semantic-models")
        .toString();
  }

  @VisibleForTesting
  static void checkSemanticModelNamespace(Namespace namespace) {
    Namespace.check(
        namespace != null && namespace.length() == 1,
        "Semantic Model namespace must be non-null and have 1 level, the input namespace is %s",
        namespace);
  }

  @VisibleForTesting
  static void checkSemanticModelNameIdentifier(NameIdentifier ident) {
    NameIdentifier.check(ident != null, "NameIdentifier must not be null");
    NameIdentifier.check(
        ident.name() != null && !ident.name().isEmpty(), "NameIdentifier name must not be empty");
    checkSemanticModelNamespace(ident.namespace());
  }

  private Namespace getSemanticModelFullNamespace(Namespace semanticModelNamespace) {
    return Namespace.of(catalogNamespace.level(0), catalogName, semanticModelNamespace.level(0));
  }
}
