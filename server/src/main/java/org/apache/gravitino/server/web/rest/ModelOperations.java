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
package org.apache.gravitino.server.web.rest;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.ModelDispatcher;
import org.apache.gravitino.dto.requests.ModelRegisterRequest;
import org.apache.gravitino.dto.requests.ModelVersionLinkRequest;
import org.apache.gravitino.dto.responses.BaseResponse;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.EntityListResponse;
import org.apache.gravitino.dto.responses.ModelResponse;
import org.apache.gravitino.dto.responses.ModelVersionListResponse;
import org.apache.gravitino.dto.responses.ModelVersionResponse;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.model.Model;
import org.apache.gravitino.model.ModelVersion;
import org.apache.gravitino.server.web.Utils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/models")
public class ModelOperations {

  private static final Logger LOG = LoggerFactory.getLogger(ModelOperations.class);

  private final ModelDispatcher modelDispatcher;

  @Context private HttpServletRequest httpRequest;

  @Inject
  public ModelOperations(ModelDispatcher modelDispatcher) {
    this.modelDispatcher = modelDispatcher;
  }

  @GET
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "list-model." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-model", absolute = true)
  public Response listModels(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema) {
    LOG.info("Received list models request for schema: {}.{}.{}", metalake, catalog, schema);
    Namespace modelNs = NamespaceUtil.ofModel(metalake, catalog, schema);

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            NameIdentifier[] modelIds = modelDispatcher.listModels(modelNs);
            modelIds = modelIds == null ? new NameIdentifier[0] : modelIds;
            LOG.info("List {} models under schema {}", modelIds.length, modelNs);
            return Utils.ok(new EntityListResponse(modelIds));
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleModelException(OperationType.LIST, "", schema, e);
    }
  }

  @GET
  @Path("{model}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "get-model." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "get-model", absolute = true)
  public Response getModel(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      @PathParam("model") String model) {
    LOG.info("Received get model request: {}.{}.{}.{}", metalake, catalog, schema, model);
    NameIdentifier modelId = NameIdentifierUtil.ofModel(metalake, catalog, schema, model);

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            Model m = modelDispatcher.getModel(modelId);
            LOG.info("Model got: {}", modelId);
            return Utils.ok(new ModelResponse(DTOConverters.toDTO(m)));
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleModelException(OperationType.GET, model, schema, e);
    }
  }

  @POST
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "register-model." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "register-model", absolute = true)
  public Response registerModel(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      ModelRegisterRequest request) {
    LOG.info(
        "Received register model request: {}.{}.{}.{}",
        metalake,
        catalog,
        schema,
        request.getName());

    try {
      request.validate();
      NameIdentifier modelId =
          NameIdentifierUtil.ofModel(metalake, catalog, schema, request.getName());

      return Utils.doAs(
          httpRequest,
          () -> {
            Model m =
                modelDispatcher.registerModel(
                    modelId, request.getComment(), request.getProperties());
            LOG.info("Model registered: {}", modelId);
            return Utils.ok(new ModelResponse(DTOConverters.toDTO(m)));
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleModelException(
          OperationType.REGISTER, request.getName(), schema, e);
    }
  }

  @DELETE
  @Path("{model}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "delete-model." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "delete-model", absolute = true)
  public Response deleteModel(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      @PathParam("model") String model) {
    LOG.info("Received delete model request: {}.{}.{}.{}", metalake, catalog, schema, model);
    NameIdentifier modelId = NameIdentifierUtil.ofModel(metalake, catalog, schema, model);

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            boolean deleted = modelDispatcher.deleteModel(modelId);
            if (!deleted) {
              LOG.warn("Cannot find to be deleted model {} under schema {}", model, schema);
            } else {
              LOG.info("Model deleted: {}", modelId);
            }

            return Utils.ok(new DropResponse(deleted));
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleModelException(OperationType.DELETE, model, schema, e);
    }
  }

  @GET
  @Path("{model}/versions")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "list-model-versions." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-model-versions", absolute = true)
  public Response listModelVersions(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      @PathParam("model") String model) {
    LOG.info("Received list model versions request: {}.{}.{}.{}", metalake, catalog, schema, model);
    NameIdentifier modelId = NameIdentifierUtil.ofModel(metalake, catalog, schema, model);

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            int[] versions = modelDispatcher.listModelVersions(modelId);
            versions = versions == null ? new int[0] : versions;
            LOG.info("List {} versions of model {}", versions.length, modelId);
            return Utils.ok(new ModelVersionListResponse(versions));
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleModelException(OperationType.LIST_VERSIONS, model, schema, e);
    }
  }

  @GET
  @Path("{model}/versions/{version}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "get-model-version." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "get-model-version", absolute = true)
  public Response getModelVersion(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      @PathParam("model") String model,
      @PathParam("version") int version) {
    LOG.info(
        "Received get model version request: {}.{}.{}.{}.{}",
        metalake,
        catalog,
        schema,
        model,
        version);
    NameIdentifier modelId = NameIdentifierUtil.ofModel(metalake, catalog, schema, model);

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            ModelVersion mv = modelDispatcher.getModelVersion(modelId, version);
            LOG.info("Model version got: {}.{}", modelId, version);
            return Utils.ok(new ModelVersionResponse(DTOConverters.toDTO(mv)));
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleModelException(
          OperationType.GET, versionString(model, version), schema, e);
    }
  }

  @GET
  @Path("{model}/aliases/{alias}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "get-model-alias." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "get-model-alias", absolute = true)
  public Response getModelVersionByAlias(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      @PathParam("model") String model,
      @PathParam("alias") String alias) {
    LOG.info(
        "Received get model version alias request: {}.{}.{}.{}.{}",
        metalake,
        catalog,
        schema,
        model,
        alias);
    NameIdentifier modelId = NameIdentifierUtil.ofModel(metalake, catalog, schema, model);

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            ModelVersion mv = modelDispatcher.getModelVersion(modelId, alias);
            LOG.info("Model version alias got: {}.{}", modelId, alias);
            return Utils.ok(new ModelVersionResponse(DTOConverters.toDTO(mv)));
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleModelException(
          OperationType.GET, aliasString(model, alias), schema, e);
    }
  }

  @POST
  @Path("{model}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "link-model-version." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "link-model-version", absolute = true)
  public Response linkModelVersion(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      @PathParam("model") String model,
      ModelVersionLinkRequest request) {
    LOG.info("Received link model version request: {}.{}.{}.{}", metalake, catalog, schema, model);
    NameIdentifier modelId = NameIdentifierUtil.ofModel(metalake, catalog, schema, model);

    try {
      request.validate();

      return Utils.doAs(
          httpRequest,
          () -> {
            modelDispatcher.linkModelVersion(
                modelId,
                request.getUri(),
                request.getAliases(),
                request.getComment(),
                request.getProperties());
            LOG.info("Model version linked: {}", modelId);
            return Utils.ok(new BaseResponse());
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleModelException(OperationType.LINK, model, schema, e);
    }
  }

  @DELETE
  @Path("{model}/versions/{version}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "delete-model-version." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "delete-model-version", absolute = true)
  public Response deleteModelVersion(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      @PathParam("model") String model,
      @PathParam("version") int version) {
    LOG.info(
        "Received delete model version request: {}.{}.{}.{}.{}",
        metalake,
        catalog,
        schema,
        model,
        version);
    NameIdentifier modelId = NameIdentifierUtil.ofModel(metalake, catalog, schema, model);

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            boolean deleted = modelDispatcher.deleteModelVersion(modelId, version);
            if (!deleted) {
              LOG.warn("Cannot find to be deleted version {} in model {}", version, model);
            } else {
              LOG.info("Model version deleted: {}.{}", modelId, version);
            }

            return Utils.ok(new DropResponse(deleted));
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleModelException(
          OperationType.DELETE, versionString(model, version), schema, e);
    }
  }

  @DELETE
  @Path("{model}/aliases/{alias}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "delete-model-alias." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "delete-model-alias", absolute = true)
  public Response deleteModelVersionByAlias(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      @PathParam("model") String model,
      @PathParam("alias") String alias) {
    LOG.info(
        "Received delete model version by alias request: {}.{}.{}.{}.{}",
        metalake,
        catalog,
        schema,
        model,
        alias);
    NameIdentifier modelId = NameIdentifierUtil.ofModel(metalake, catalog, schema, model);

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            boolean deleted = modelDispatcher.deleteModelVersion(modelId, alias);
            if (!deleted) {
              LOG.warn(
                  "Cannot find to be deleted model version by alias {} in model {}", alias, model);
            } else {
              LOG.info("Model version by alias deleted: {}.{}", modelId, alias);
            }

            return Utils.ok(new DropResponse(deleted));
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleModelException(
          OperationType.DELETE, aliasString(model, alias), schema, e);
    }
  }

  private String versionString(String model, int version) {
    return model + " version(" + version + ")";
  }

  private String aliasString(String model, String alias) {
    return model + " alias(" + alias + ")";
  }
}
