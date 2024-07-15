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
package com.apache.gravitino.server.web.rest;

import com.apache.gravitino.NameIdentifier;
import com.apache.gravitino.Namespace;
import com.apache.gravitino.catalog.FilesetDispatcher;
import com.apache.gravitino.dto.requests.FilesetCreateRequest;
import com.apache.gravitino.dto.requests.FilesetUpdateRequest;
import com.apache.gravitino.dto.requests.FilesetUpdatesRequest;
import com.apache.gravitino.dto.responses.DropResponse;
import com.apache.gravitino.dto.responses.EntityListResponse;
import com.apache.gravitino.dto.responses.FilesetResponse;
import com.apache.gravitino.dto.util.DTOConverters;
import com.apache.gravitino.file.Fileset;
import com.apache.gravitino.file.FilesetChange;
import com.apache.gravitino.lock.LockType;
import com.apache.gravitino.lock.TreeLockUtils;
import com.apache.gravitino.metrics.MetricNames;
import com.apache.gravitino.server.web.Utils;
import com.apache.gravitino.utils.NameIdentifierUtil;
import com.apache.gravitino.utils.NamespaceUtil;
import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import java.util.Optional;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/filesets")
public class FilesetOperations {

  private static final Logger LOG = LoggerFactory.getLogger(FilesetOperations.class);

  private final FilesetDispatcher dispatcher;

  @Context private HttpServletRequest httpRequest;

  @Inject
  public FilesetOperations(FilesetDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  @GET
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "list-fileset." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-fileset", absolute = true)
  public Response listFilesets(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema) {
    try {
      LOG.info("Received list filesets request for schema: {}.{}.{}", metalake, catalog, schema);
      return Utils.doAs(
          httpRequest,
          () -> {
            Namespace filesetNS = NamespaceUtil.ofFileset(metalake, catalog, schema);
            NameIdentifier[] idents =
                TreeLockUtils.doWithTreeLock(
                    NameIdentifier.of(metalake, catalog, schema),
                    LockType.READ,
                    () -> dispatcher.listFilesets(filesetNS));
            Response response = Utils.ok(new EntityListResponse(idents));
            LOG.info(
                "List {} filesets under schema: {}.{}.{}",
                idents.length,
                metalake,
                catalog,
                schema);
            return response;
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleFilesetException(OperationType.LIST, "", schema, e);
    }
  }

  @POST
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "create-fileset." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "create-fileset", absolute = true)
  public Response createFileset(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      FilesetCreateRequest request) {
    LOG.info(
        "Received create fileset request: {}.{}.{}.{}",
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
                NameIdentifierUtil.ofFileset(metalake, catalog, schema, request.getName());

            Fileset fileset =
                TreeLockUtils.doWithTreeLock(
                    NameIdentifierUtil.ofSchema(metalake, catalog, schema),
                    LockType.WRITE,
                    () ->
                        dispatcher.createFileset(
                            ident,
                            request.getComment(),
                            Optional.ofNullable(request.getType()).orElse(Fileset.Type.MANAGED),
                            request.getStorageLocation(),
                            request.getProperties()));
            Response response = Utils.ok(new FilesetResponse(DTOConverters.toDTO(fileset)));
            LOG.info("Fileset created: {}.{}.{}.{}", metalake, catalog, schema, request.getName());
            return response;
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleFilesetException(
          OperationType.CREATE, request.getName(), schema, e);
    }
  }

  @GET
  @Path("{fileset}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "load-fileset." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "load-fileset", absolute = true)
  public Response loadFileset(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      @PathParam("fileset") String fileset) {
    LOG.info("Received load fileset request: {}.{}.{}.{}", metalake, catalog, schema, fileset);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            NameIdentifier ident = NameIdentifierUtil.ofFileset(metalake, catalog, schema, fileset);
            Fileset t =
                TreeLockUtils.doWithTreeLock(
                    ident, LockType.READ, () -> dispatcher.loadFileset(ident));
            Response response = Utils.ok(new FilesetResponse(DTOConverters.toDTO(t)));
            LOG.info("Fileset loaded: {}.{}.{}.{}", metalake, catalog, schema, fileset);
            return response;
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleFilesetException(OperationType.LOAD, fileset, schema, e);
    }
  }

  @PUT
  @Path("{fileset}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "alter-fileset." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "alter-fileset", absolute = true)
  public Response alterFileset(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      @PathParam("fileset") String fileset,
      FilesetUpdatesRequest request) {
    LOG.info("Received alter fileset request: {}.{}.{}.{}", metalake, catalog, schema, fileset);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            NameIdentifier ident = NameIdentifierUtil.ofFileset(metalake, catalog, schema, fileset);
            FilesetChange[] changes =
                request.getUpdates().stream()
                    .map(FilesetUpdateRequest::filesetChange)
                    .toArray(FilesetChange[]::new);
            Fileset t =
                TreeLockUtils.doWithTreeLock(
                    NameIdentifierUtil.ofSchema(metalake, catalog, schema),
                    LockType.WRITE,
                    () -> dispatcher.alterFileset(ident, changes));
            Response response = Utils.ok(new FilesetResponse(DTOConverters.toDTO(t)));
            LOG.info("Fileset altered: {}.{}.{}.{}", metalake, catalog, schema, t.name());
            return response;
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleFilesetException(OperationType.ALTER, fileset, schema, e);
    }
  }

  @DELETE
  @Path("{fileset}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "drop-fileset." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "drop-fileset", absolute = true)
  public Response dropFileset(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      @PathParam("fileset") String fileset) {
    LOG.info("Received drop fileset request: {}.{}.{}.{}", metalake, catalog, schema, fileset);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            NameIdentifier ident = NameIdentifierUtil.ofFileset(metalake, catalog, schema, fileset);
            boolean dropped =
                TreeLockUtils.doWithTreeLock(
                    NameIdentifierUtil.ofSchema(metalake, catalog, schema),
                    LockType.WRITE,
                    () -> dispatcher.dropFileset(ident));
            if (!dropped) {
              LOG.warn("Failed to drop fileset {} under schema {}", fileset, schema);
            }

            Response response = Utils.ok(new DropResponse(dropped));
            LOG.info("Fileset dropped: {}.{}.{}.{}", metalake, catalog, schema, fileset);
            return response;
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleFilesetException(OperationType.DROP, fileset, schema, e);
    }
  }
}
