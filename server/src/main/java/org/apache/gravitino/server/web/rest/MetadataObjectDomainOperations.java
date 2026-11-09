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
import java.util.Locale;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.domain.DomainDispatcher;
import org.apache.gravitino.domain.DomainMember;
import org.apache.gravitino.dto.domain.DomainDTO;
import org.apache.gravitino.dto.responses.DomainListResponse;
import org.apache.gravitino.dto.responses.NameListResponse;
import org.apache.gravitino.meta.DomainEntity;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.server.web.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** REST queries for the domains whose scopes contain a recorded leaf object. */
@Path("/metalakes/{metalake}/objects/{type}/{fullName}/domains")
public class MetadataObjectDomainOperations {

  private static final Logger LOG = LoggerFactory.getLogger(MetadataObjectDomainOperations.class);
  private final DomainDispatcher domainDispatcher;
  @Context private HttpServletRequest httpRequest;

  /**
   * Creates the object's domain resource.
   *
   * @param domainDispatcher The domain dispatcher.
   */
  @Inject
  public MetadataObjectDomainOperations(DomainDispatcher domainDispatcher) {
    this.domainDispatcher = domainDispatcher;
  }

  /**
   * Lists domains containing a leaf metadata object.
   *
   * @param metalake The metalake name.
   * @param type The supported leaf object type.
   * @param fullName The object's full name.
   * @param details Whether to return snapshots including relations instead of names.
   * @return The domain names or snapshots.
   */
  @GET
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "list-object-domains." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-object-domains", absolute = true)
  public Response listDomains(
      @PathParam("metalake") String metalake,
      @PathParam("type") String type,
      @PathParam("fullName") String fullName,
      @QueryParam("details") @DefaultValue("false") boolean details) {
    LOG.info("Received list domains request for {} {} under metalake {}", type, fullName, metalake);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            MetadataObject.Type objectType =
                MetadataObject.Type.valueOf(type.toUpperCase(Locale.ROOT));
            if (!DomainMember.SUPPORTED_TYPES.contains(objectType)) {
              throw new IllegalArgumentException(
                  "Type " + objectType + " cannot be a domain member");
            }
            MetadataObject object = MetadataObjects.parse(fullName, objectType);
            // Domain visibility and member authorization will be added with domain access control.
            DomainEntity[] domains =
                domainDispatcher.listDomainsForMetadataObject(metalake, object);
            if (!details) {
              return Utils.ok(
                  new NameListResponse(
                      Arrays.stream(domains).map(DomainEntity::name).toArray(String[]::new)));
            }
            DomainDTO[] dtos =
                Arrays.stream(domains).map(DomainOperations::toDTO).toArray(DomainDTO[]::new);
            return Utils.ok(new DomainListResponse(dtos));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleDomainException(OperationType.LIST, fullName, metalake, e);
    }
  }
}
