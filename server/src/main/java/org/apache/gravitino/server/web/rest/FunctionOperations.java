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
import java.util.Arrays;
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
import org.apache.gravitino.dto.function.FunctionColumnDTO;
import org.apache.gravitino.dto.function.FunctionDTO;
import org.apache.gravitino.dto.function.FunctionDefinitionDTO;
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
import org.apache.gravitino.function.FunctionColumn;
import org.apache.gravitino.function.FunctionDefinition;
import org.apache.gravitino.function.FunctionType;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.server.web.Utils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** REST operations for function management. */
@Path("metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/functions")
public class FunctionOperations {

  private static final Logger LOG = LoggerFactory.getLogger(FunctionOperations.class);

  private final FunctionDispatcher dispatcher;

  @Context private HttpServletRequest httpRequest;

  @Inject
  public FunctionOperations(FunctionDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  // TODO: Add authorization support for function operations
  @GET
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "list-function." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-function", absolute = true)
  public Response listFunctions(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      @QueryParam("details") @DefaultValue("false") boolean details) {
    try {
      LOG.info("Received list functions request for schema: {}.{}.{}", metalake, catalog, schema);
      return Utils.doAs(
          httpRequest,
          () -> {
            Namespace namespace = NamespaceUtil.ofFunction(metalake, catalog, schema);
            if (!details) {
              NameIdentifier[] identifiers = dispatcher.listFunctions(namespace);
              LOG.info(
                  "List {} function names under schema: {}.{}.{}",
                  identifiers.length,
                  metalake,
                  catalog,
                  schema);
              return Utils.ok(new EntityListResponse(identifiers));
            }

            Function[] functions = dispatcher.listFunctionInfos(namespace);
            FunctionDTO[] functionDTOs =
                Arrays.stream(functions)
                    .map(DTOConverters::toDTO)
                    .toList()
                    .toArray(new FunctionDTO[0]);
            LOG.info(
                "List {} function definitions under schema: {}.{}.{}",
                functionDTOs.length,
                metalake,
                catalog,
                schema);
            return Utils.ok(new FunctionListResponse(functionDTOs));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleFunctionException(OperationType.LIST, "", schema, e);
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
    LOG.info(
        "Received register function request: {}.{}.{}.{}",
        metalake,
        catalog,
        schema,
        request.getName());
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            NameIdentifier ident =
                NameIdentifierUtil.ofFunction(metalake, catalog, schema, request.getName());

            FunctionDefinition[] definitions =
                Arrays.stream(request.getDefinitions())
                    .map(FunctionDefinitionDTO::toFunctionDefinition)
                    .toArray(FunctionDefinition[]::new);

            Function function;
            if (request.getFunctionType() == FunctionType.TABLE) {
              FunctionColumn[] returnColumns =
                  Arrays.stream(request.getReturnColumns())
                      .map(FunctionColumnDTO::toFunctionColumn)
                      .toArray(FunctionColumn[]::new);
              function =
                  dispatcher.registerFunction(
                      ident,
                      request.getComment(),
                      request.isDeterministic(),
                      returnColumns,
                      definitions);
            } else {
              Type returnType = request.getReturnType();
              function =
                  dispatcher.registerFunction(
                      ident,
                      request.getComment(),
                      request.getFunctionType(),
                      request.isDeterministic(),
                      returnType,
                      definitions);
            }

            Response response = Utils.ok(new FunctionResponse(DTOConverters.toDTO(function)));
            LOG.info(
                "Function registered: {}.{}.{}.{}", metalake, catalog, schema, request.getName());
            return response;
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleFunctionException(
          OperationType.CREATE, request.getName(), schema, e);
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
      @PathParam("function") String function) {
    LOG.info("Received get function request: {}.{}.{}.{}", metalake, catalog, schema, function);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            NameIdentifier ident =
                NameIdentifierUtil.ofFunction(metalake, catalog, schema, function);
            Function f = dispatcher.getFunction(ident);
            Response response = Utils.ok(new FunctionResponse(DTOConverters.toDTO(f)));
            LOG.info("Function loaded: {}.{}.{}.{}", metalake, catalog, schema, function);
            return response;
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleFunctionException(OperationType.LOAD, function, schema, e);
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
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            NameIdentifier ident =
                NameIdentifierUtil.ofFunction(metalake, catalog, schema, function);
            FunctionChange[] changes =
                request.getUpdates().stream()
                    .map(FunctionUpdateRequest::functionChange)
                    .toArray(FunctionChange[]::new);
            Function f = dispatcher.alterFunction(ident, changes);
            Response response = Utils.ok(new FunctionResponse(DTOConverters.toDTO(f)));
            LOG.info("Function altered: {}.{}.{}.{}", metalake, catalog, schema, f.name());
            return response;
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleFunctionException(OperationType.ALTER, function, schema, e);
    }
  }

  @DELETE
  @Path("{function}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "drop-function." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "drop-function", absolute = true)
  public Response dropFunction(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      @PathParam("function") String function) {
    LOG.info("Received drop function request: {}.{}.{}.{}", metalake, catalog, schema, function);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            NameIdentifier ident =
                NameIdentifierUtil.ofFunction(metalake, catalog, schema, function);
            boolean dropped = dispatcher.dropFunction(ident);
            if (!dropped) {
              LOG.warn("Cannot find to be dropped function {} under schema {}", function, schema);
            }
            Response response = Utils.ok(new DropResponse(dropped));
            LOG.info("Function dropped: {}.{}.{}.{}", metalake, catalog, schema, function);
            return response;
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleFunctionException(OperationType.DROP, function, schema, e);
    }
  }
}
