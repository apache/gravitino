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
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.FunctionDispatcher;
import org.apache.gravitino.dto.function.FunctionDTO;
import org.apache.gravitino.dto.requests.FunctionDeleteRequest;
import org.apache.gravitino.dto.requests.FunctionRegisterRequest;
import org.apache.gravitino.dto.requests.FunctionUpdateRequest;
import org.apache.gravitino.dto.requests.FunctionUpdatesRequest;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.EntityListResponse;
import org.apache.gravitino.dto.responses.FunctionListResponse;
import org.apache.gravitino.dto.responses.FunctionResponse;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.function.Function;
import org.apache.gravitino.function.FunctionChange;
import org.apache.gravitino.function.FunctionSignature;
import org.apache.gravitino.function.FunctionType;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.server.web.Utils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/functions")
public class FunctionOperations {

  private static final Logger LOG = LoggerFactory.getLogger(FunctionOperations.class);

  // TODO: Add authorization checks for function REST operations.

  private final FunctionDispatcher functionDispatcher;

  @Context private HttpServletRequest httpRequest;

  @Inject
  public FunctionOperations(FunctionDispatcher functionDispatcher) {
    this.functionDispatcher = functionDispatcher;
  }

  @GET
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "list-function." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-function", absolute = true)
  public Response listFunctions(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      @QueryParam("details") @DefaultValue("false") boolean details) {
    LOG.info("Received list functions request for schema: {}.{}.{}", metalake, catalog, schema);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            Namespace namespace = NamespaceUtil.ofFunction(metalake, catalog, schema);
            NameIdentifier[] identifiers = functionDispatcher.listFunctions(namespace);
            identifiers = identifiers == null ? new NameIdentifier[0] : identifiers;
            if (!details) {
              LOG.info(
                  "List {} functions under schema: {}.{}.{}",
                  identifiers.length,
                  metalake,
                  catalog,
                  schema);
              return Utils.ok(new EntityListResponse(identifiers));
            }

            List<FunctionDTO> functionDTOs = new ArrayList<>();
            for (NameIdentifier ident : identifiers) {
              Function[] functions = functionDispatcher.getFunction(ident);
              if (functions != null) {
                functionDTOs.addAll(Arrays.stream(functions).map(DTOConverters::toDTO).toList());
              }
            }
            LOG.info(
                "List {} function definitions under schema: {}.{}.{}",
                functionDTOs.size(),
                metalake,
                catalog,
                schema);
            return Utils.ok(new FunctionListResponse(functionDTOs.toArray(new FunctionDTO[0])));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleFunctionException(OperationType.LIST, "", schema, e);
    }
  }

  @GET
  @Path("{function}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "get-function." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "get-function", absolute = true)
  public Response getFunction(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      @PathParam("function") String function,
      @QueryParam("version") Integer version) {
    LOG.info("Received get function request: {}.{}.{}.{}", metalake, catalog, schema, function);
    NameIdentifier ident = NameIdentifierUtil.ofFunction(metalake, catalog, schema, function);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            Function[] functions =
                version == null
                    ? functionDispatcher.getFunction(ident)
                    : functionDispatcher.getFunction(ident, version);
            functions = functions == null ? new Function[0] : functions;
            LOG.info("Loaded {} function definitions for {}", functions.length, ident);
            return Utils.ok(new FunctionListResponse(DTOConverters.toDTOs(functions)));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleFunctionException(OperationType.GET, function, schema, e);
    }
  }

  @POST
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "register-function." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "register-function", absolute = true)
  public Response registerFunction(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      FunctionRegisterRequest request) {
    LOG.info("Received register function request: {}.{}.{}", metalake, catalog, schema);
    try {
      Preconditions.checkArgument(request != null, "\"request\" must not be null");
      request.validate();
      FunctionSignature signature = request.getSignature().toFunctionSignature();
      NameIdentifier ident =
          NameIdentifierUtil.ofFunction(metalake, catalog, schema, signature.name());

      return Utils.doAs(
          httpRequest,
          () -> {
            Function function;
            if (request.getType() == FunctionType.TABLE) {
              function =
                  functionDispatcher.registerFunction(
                      ident,
                      request.getComment(),
                      request.isDeterministic(),
                      DTOConverters.fromDTOs(request.getSignature().getFunctionParams()),
                      DTOConverters.fromDTOs(request.getReturnColumns()),
                      DTOConverters.fromDTOs(request.getImpls()));
            } else {
              function =
                  functionDispatcher.registerFunction(
                      ident,
                      request.getComment(),
                      request.getType(),
                      request.isDeterministic(),
                      DTOConverters.fromDTOs(request.getSignature().getFunctionParams()),
                      request.getReturnType(),
                      DTOConverters.fromDTOs(request.getImpls()));
            }
            LOG.info("Function registered: {}", ident);
            return Utils.ok(new FunctionResponse(DTOConverters.toDTO(function)));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleFunctionException(
          OperationType.CREATE,
          request == null || request.getSignature() == null ? "" : request.getSignature().getName(),
          schema,
          e);
    }
  }

  @PUT
  @Path("{function}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "alter-function." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "alter-function", absolute = true)
  public Response alterFunction(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      @PathParam("function") String function,
      FunctionUpdatesRequest request) {
    LOG.info("Received alter function request: {}.{}.{}.{}", metalake, catalog, schema, function);
    NameIdentifier ident = NameIdentifierUtil.ofFunction(metalake, catalog, schema, function);
    try {
      Preconditions.checkArgument(request != null, "\"request\" must not be null");
      request.validate();
      FunctionChange[] changes =
          request.getUpdates().stream()
              .map(FunctionUpdateRequest::functionChange)
              .toArray(FunctionChange[]::new);
      return Utils.doAs(
          httpRequest,
          () -> {
            Function updated = functionDispatcher.alterFunction(ident, changes);
            LOG.info("Altered function: {}", ident);
            return Utils.ok(new FunctionResponse(DTOConverters.toDTO(updated)));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleFunctionException(OperationType.ALTER, function, schema, e);
    }
  }

  @DELETE
  @Path("{function}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "delete-function." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "delete-function", absolute = true)
  public Response deleteFunction(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      @PathParam("function") String function,
      FunctionDeleteRequest request) {
    LOG.info("Received delete function request: {}.{}.{}.{}", metalake, catalog, schema, function);
    NameIdentifier ident = NameIdentifierUtil.ofFunction(metalake, catalog, schema, function);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            boolean deleted;
            if (request != null && request.getSignature() != null) {
              request.validate();
              deleted =
                  functionDispatcher.deleteFunction(
                      ident, request.getSignature().toFunctionSignature());
            } else {
              deleted = functionDispatcher.deleteFunction(ident);
            }
            if (!deleted) {
              LOG.warn("Function not found to delete: {}", ident);
            } else {
              LOG.info("Function deleted: {}", ident);
            }
            return Utils.ok(new DropResponse(deleted));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleFunctionException(OperationType.DELETE, function, schema, e);
    }
  }
}
