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
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.CatalogDTO;
import org.apache.gravitino.dto.requests.ModelRegisterRequest;
import org.apache.gravitino.dto.requests.ModelUpdateRequest;
import org.apache.gravitino.dto.requests.ModelUpdatesRequest;
import org.apache.gravitino.dto.requests.ModelVersionLinkRequest;
import org.apache.gravitino.dto.requests.ModelVersionUpdateRequest;
import org.apache.gravitino.dto.requests.ModelVersionUpdatesRequest;
import org.apache.gravitino.dto.responses.BaseResponse;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.EntityListResponse;
import org.apache.gravitino.dto.responses.ModelResponse;
import org.apache.gravitino.dto.responses.ModelVersionInfoListResponse;
import org.apache.gravitino.dto.responses.ModelVersionListResponse;
import org.apache.gravitino.dto.responses.ModelVersionResponse;
import org.apache.gravitino.dto.responses.ModelVersionUriResponse;
import org.apache.gravitino.exceptions.ModelAlreadyExistsException;
import org.apache.gravitino.exceptions.ModelVersionAliasesAlreadyExistException;
import org.apache.gravitino.exceptions.NoSuchModelException;
import org.apache.gravitino.exceptions.NoSuchModelVersionException;
import org.apache.gravitino.exceptions.NoSuchModelVersionURINameException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.model.Model;
import org.apache.gravitino.model.ModelCatalog;
import org.apache.gravitino.model.ModelChange;
import org.apache.gravitino.model.ModelVersion;
import org.apache.gravitino.model.ModelVersionChange;
import org.apache.gravitino.rest.RESTUtils;

class GenericModelCatalog extends BaseSchemaCatalog implements ModelCatalog {

  GenericModelCatalog(
      Namespace namespace,
      String catalogName,
      Catalog.Type catalogType,
      String provider,
      String comment,
      Map<String, String> properties,
      AuditDTO auditDTO,
      RESTClient restClient) {
    super(namespace, catalogName, catalogType, provider, comment, properties, auditDTO, restClient);
  }

  @Override
  public ModelCatalog asModelCatalog() {
    return this;
  }

  @Override
  public NameIdentifier[] listModels(Namespace namespace) throws NoSuchSchemaException {
    checkModelNamespace(namespace);

    Namespace modelFullNs = modelFullNamespace(namespace);
    EntityListResponse resp =
        restClient.get(
            formatModelRequestPath(modelFullNs),
            EntityListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.modelErrorHandler());
    resp.validate();

    return Arrays.stream(resp.identifiers())
        .map(id -> NameIdentifier.of(id.namespace().level(2), id.name()))
        .toArray(NameIdentifier[]::new);
  }

  @Override
  public Model getModel(NameIdentifier ident) throws NoSuchModelException {
    checkModelNameIdentifier(ident);

    Namespace modelFullNs = modelFullNamespace(ident.namespace());
    ModelResponse resp =
        restClient.get(
            formatModelRequestPath(modelFullNs) + "/" + RESTUtils.encodeString(ident.name()),
            ModelResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.modelErrorHandler());
    resp.validate();

    return new GenericModel(resp.getModel(), restClient, modelFullNs);
  }

  @Override
  public Model registerModel(NameIdentifier ident, String comment, Map<String, String> properties)
      throws NoSuchSchemaException, ModelAlreadyExistsException {
    checkModelNameIdentifier(ident);

    Namespace modelFullNs = modelFullNamespace(ident.namespace());
    ModelRegisterRequest req = new ModelRegisterRequest(ident.name(), comment, properties);
    req.validate();

    ModelResponse resp =
        restClient.post(
            formatModelRequestPath(modelFullNs),
            req,
            ModelResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.modelErrorHandler());

    resp.validate();
    return new GenericModel(resp.getModel(), restClient, modelFullNs);
  }

  @Override
  public boolean deleteModel(NameIdentifier ident) {
    checkModelNameIdentifier(ident);

    Namespace modelFullNs = modelFullNamespace(ident.namespace());
    DropResponse resp =
        restClient.delete(
            formatModelRequestPath(modelFullNs) + "/" + RESTUtils.encodeString(ident.name()),
            DropResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.modelErrorHandler());
    resp.validate();

    return resp.dropped();
  }

  @Override
  public int[] listModelVersions(NameIdentifier ident) throws NoSuchModelException {
    checkModelNameIdentifier(ident);

    NameIdentifier modelFullIdent = modelFullNameIdentifier(ident);
    ModelVersionListResponse resp =
        restClient.get(
            formatModelVersionRequestPath(modelFullIdent) + "/versions",
            ModelVersionListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.modelErrorHandler());
    resp.validate();

    return resp.getVersions();
  }

  @Override
  public ModelVersion[] listModelVersionInfos(NameIdentifier ident) throws NoSuchModelException {
    checkModelNameIdentifier(ident);

    NameIdentifier modelFullIdent = modelFullNameIdentifier(ident);
    ModelVersionInfoListResponse resp =
        restClient.get(
            formatModelVersionRequestPath(modelFullIdent) + "/versions",
            ImmutableMap.of("details", "true"),
            ModelVersionInfoListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.modelErrorHandler());
    resp.validate();

    return resp.getVersions();
  }

  @Override
  public ModelVersion getModelVersion(NameIdentifier ident, int version)
      throws NoSuchModelVersionException {
    checkModelNameIdentifier(ident);
    Preconditions.checkArgument(version >= 0, "Model version must be non-negative");

    NameIdentifier modelFullIdent = modelFullNameIdentifier(ident);
    ModelVersionResponse resp =
        restClient.get(
            formatModelVersionRequestPath(modelFullIdent) + "/versions/" + version,
            ModelVersionResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.modelErrorHandler());
    resp.validate();
    return new GenericModelVersion(resp.getModelVersion());
  }

  @Override
  public ModelVersion getModelVersion(NameIdentifier ident, String alias)
      throws NoSuchModelVersionException {
    checkModelNameIdentifier(ident);
    Preconditions.checkArgument(StringUtils.isNotBlank(alias), "Model alias must be non-empty");

    NameIdentifier modelFullIdent = modelFullNameIdentifier(ident);
    ModelVersionResponse resp =
        restClient.get(
            formatModelVersionRequestPath(modelFullIdent)
                + "/aliases/"
                + RESTUtils.encodeString(alias),
            ModelVersionResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.modelErrorHandler());

    resp.validate();
    return new GenericModelVersion(resp.getModelVersion());
  }

  @Override
  public void linkModelVersion(
      NameIdentifier ident,
      Map<String, String> uris,
      String[] aliases,
      String comment,
      Map<String, String> properties)
      throws NoSuchModelException, ModelVersionAliasesAlreadyExistException {
    checkModelNameIdentifier(ident);

    ModelVersionLinkRequest req = new ModelVersionLinkRequest(uris, aliases, comment, properties);
    NameIdentifier modelFullIdent = modelFullNameIdentifier(ident);
    BaseResponse resp =
        restClient.post(
            formatModelVersionRequestPath(modelFullIdent) + "/versions",
            req,
            BaseResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.modelErrorHandler());

    resp.validate();
  }

  @Override
  public String getModelVersionUri(NameIdentifier ident, int version, String uriName)
      throws NoSuchModelVersionException, NoSuchModelVersionURINameException {
    checkModelNameIdentifier(ident);
    Preconditions.checkArgument(version >= 0, "Model version must be non-negative");

    NameIdentifier modelFullIdent = modelFullNameIdentifier(ident);
    Map<String, String> queryParam =
        uriName == null ? Collections.emptyMap() : ImmutableMap.of("uriName", uriName);
    ModelVersionUriResponse resp =
        restClient.get(
            formatModelVersionRequestPath(modelFullIdent) + "/versions/" + version + "/uri",
            queryParam,
            ModelVersionUriResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.modelErrorHandler());
    resp.validate();
    return resp.getUri();
  }

  @Override
  public String getModelVersionUri(NameIdentifier ident, String alias, String uriName)
      throws NoSuchModelVersionException, NoSuchModelVersionURINameException {
    checkModelNameIdentifier(ident);
    Preconditions.checkArgument(StringUtils.isNotBlank(alias), "Model alias must be non-empty");

    NameIdentifier modelFullIdent = modelFullNameIdentifier(ident);
    Map<String, String> queryParam =
        uriName == null ? Collections.emptyMap() : ImmutableMap.of("uriName", uriName);
    ModelVersionUriResponse resp =
        restClient.get(
            formatModelVersionRequestPath(modelFullIdent)
                + "/aliases/"
                + RESTUtils.encodeString(alias)
                + "/uri",
            queryParam,
            ModelVersionUriResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.modelErrorHandler());

    resp.validate();
    return resp.getUri();
  }

  @Override
  public boolean deleteModelVersion(NameIdentifier ident, int version) {
    checkModelNameIdentifier(ident);
    Preconditions.checkArgument(version >= 0, "Model version must be non-negative");

    NameIdentifier modelFullIdent = modelFullNameIdentifier(ident);
    DropResponse resp =
        restClient.delete(
            formatModelVersionRequestPath(modelFullIdent) + "/versions/" + version,
            DropResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.modelErrorHandler());
    resp.validate();

    return resp.dropped();
  }

  @Override
  public boolean deleteModelVersion(NameIdentifier ident, String alias) {
    checkModelNameIdentifier(ident);
    Preconditions.checkArgument(StringUtils.isNotBlank(alias), "Model alias must be non-empty");

    NameIdentifier modelFullIdent = modelFullNameIdentifier(ident);
    DropResponse resp =
        restClient.delete(
            formatModelVersionRequestPath(modelFullIdent)
                + "/aliases/"
                + RESTUtils.encodeString(alias),
            DropResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.modelErrorHandler());
    resp.validate();

    return resp.dropped();
  }

  @Override
  public Model alterModel(NameIdentifier ident, ModelChange... changes)
      throws NoSuchModelException, IllegalArgumentException {
    checkModelNameIdentifier(ident);

    // Convert ModelChange objects to DTO requests
    List<ModelUpdateRequest> updateRequests =
        Arrays.stream(changes)
            .map(DTOConverters::toModelUpdateRequest)
            .collect(Collectors.toList());

    ModelUpdatesRequest req = new ModelUpdatesRequest(updateRequests);
    req.validate();

    Namespace modelFullNs = modelFullNamespace(ident.namespace());
    ModelResponse resp =
        restClient.put(
            formatModelRequestPath(modelFullNs) + "/" + RESTUtils.encodeString(ident.name()),
            req,
            ModelResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.modelErrorHandler());

    resp.validate();
    return new GenericModel(resp.getModel(), restClient, modelFullNs);
  }

  @Override
  public ModelVersion alterModelVersion(
      NameIdentifier ident, int version, ModelVersionChange... changes)
      throws NoSuchModelException, NoSuchModelVersionException, IllegalArgumentException {
    checkModelNameIdentifier(ident);

    List<ModelVersionUpdateRequest> updateRequests =
        Arrays.stream(changes)
            .map(DTOConverters::toModelVersionUpdateRequest)
            .collect(Collectors.toList());

    ModelVersionUpdatesRequest req = new ModelVersionUpdatesRequest(updateRequests);
    req.validate();

    NameIdentifier modelFullIdent = modelFullNameIdentifier(ident);
    ModelVersionResponse resp =
        restClient.put(
            formatModelVersionRequestPath(modelFullIdent) + "/versions/" + version,
            req,
            ModelVersionResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.modelErrorHandler());

    resp.validate();
    return new GenericModelVersion(resp.getModelVersion());
  }

  @Override
  public ModelVersion alterModelVersion(
      NameIdentifier ident, String alias, ModelVersionChange... changes)
      throws NoSuchModelException, IllegalArgumentException {
    checkModelNameIdentifier(ident);
    Preconditions.checkArgument(StringUtils.isNotBlank(alias), "Model alias must be non-empty");

    List<ModelVersionUpdateRequest> updateRequests =
        Arrays.stream(changes)
            .map(DTOConverters::toModelVersionUpdateRequest)
            .collect(Collectors.toList());

    ModelVersionUpdatesRequest req = new ModelVersionUpdatesRequest(updateRequests);
    req.validate();

    NameIdentifier modelFullIdent = modelFullNameIdentifier(ident);
    ModelVersionResponse resp =
        restClient.put(
            formatModelVersionRequestPath(modelFullIdent)
                + "/aliases/"
                + RESTUtils.encodeString(alias),
            req,
            ModelVersionResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.modelErrorHandler());

    resp.validate();
    return new GenericModelVersion(resp.getModelVersion());
  }

  /** @return A new builder instance for {@link GenericModelCatalog}. */
  public static Builder builder() {
    return new Builder();
  }

  private void checkModelNamespace(Namespace namespace) {
    Namespace.check(
        namespace != null && namespace.length() == 1,
        "Model namespace must be non-null and only have 1 level, the input namespace is %s",
        namespace);
  }

  private void checkModelNameIdentifier(NameIdentifier ident) {
    NameIdentifier.check(ident != null, "Model name identifier must be non-null");
    NameIdentifier.check(
        StringUtils.isNotBlank(ident.name()), "Model name identifier must have a non-empty name");
    checkModelNamespace(ident.namespace());
  }

  private Namespace modelFullNamespace(Namespace modelNs) {
    return Namespace.of(catalogNamespace().level(0), name(), modelNs.level(0));
  }

  private NameIdentifier modelFullNameIdentifier(NameIdentifier modelIdent) {
    return NameIdentifier.of(modelFullNamespace(modelIdent.namespace()), modelIdent.name());
  }

  @VisibleForTesting
  static String formatModelRequestPath(Namespace modelNs) {
    Namespace schemaNs = Namespace.of(modelNs.level(0), modelNs.level(1));
    return new StringBuilder()
        .append(formatSchemaRequestPath(schemaNs))
        .append("/")
        .append(RESTUtils.encodeString(modelNs.level(2)))
        .append("/models")
        .toString();
  }

  @VisibleForTesting
  static String formatModelVersionRequestPath(NameIdentifier modelIdent) {
    return formatModelRequestPath(modelIdent.namespace())
        + "/"
        + RESTUtils.encodeString(modelIdent.name());
  }

  static class Builder extends CatalogDTO.Builder<Builder> {

    private RESTClient restClient;

    private Namespace namespace;

    private Builder() {}

    Builder withNamespace(Namespace namespace) {
      this.namespace = namespace;
      return this;
    }

    Builder withRestClient(RESTClient restClient) {
      this.restClient = restClient;
      return this;
    }

    @Override
    public GenericModelCatalog build() {
      Namespace.check(
          namespace != null && namespace.length() == 1,
          "Catalog namespace must be non-null and have 1 level, the input namespace is %s",
          namespace);
      Preconditions.checkArgument(StringUtils.isNotBlank(name), "name must not be blank");
      Preconditions.checkArgument(type != null, "type must not be null");
      Preconditions.checkArgument(StringUtils.isNotBlank(provider), "provider must not be blank");
      Preconditions.checkArgument(audit != null, "audit must not be null");
      Preconditions.checkArgument(restClient != null, "restClient must be set");

      return new GenericModelCatalog(
          namespace, name, type, provider, comment, properties, audit, restClient);
    }
  }
}
