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
import java.util.List;
import java.util.Locale;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.domain.DomainDispatcher;
import org.apache.gravitino.domain.DomainMember;
import org.apache.gravitino.domain.DomainMemberFilter;
import org.apache.gravitino.dto.domain.DomainMemberDTO;
import org.apache.gravitino.dto.responses.DomainMemberListResponse;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.server.web.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** REST queries for the recorded leaf members in a domain's scope. */
@Path("/metalakes/{metalake}/domains/{domain}/objects")
public class DomainMembershipOperations {

  private static final Logger LOG = LoggerFactory.getLogger(DomainMembershipOperations.class);
  private final DomainDispatcher domainDispatcher;
  @Context private HttpServletRequest httpRequest;

  /**
   * Creates the domain membership resource.
   *
   * @param domainDispatcher The domain dispatcher.
   */
  @Inject
  public DomainMembershipOperations(DomainDispatcher domainDispatcher) {
    this.domainDispatcher = domainDispatcher;
  }

  /**
   * Lists members matching the intersection of the supplied filters.
   *
   * @param metalake The metalake name.
   * @param domain The domain name.
   * @param types Comma-separated leaf types, or null for all supported types.
   * @param nameContains Case-insensitive full-name substring, or null for any name.
   * @param domains Repeated domain names selecting the union of their scopes.
   * @return The matching recorded members with all applicable relations.
   */
  @GET
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "list-domain-members." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-domain-members", absolute = true)
  public Response listMembers(
      @PathParam("metalake") String metalake,
      @PathParam("domain") String domain,
      @QueryParam("types") @Nullable String types,
      @QueryParam("nameContains") @Nullable String nameContains,
      @QueryParam("domains") List<String> domains) {
    LOG.info("Received list members request for domain {} under metalake {}", domain, metalake);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            DomainMemberFilter.Builder filter =
                DomainMemberFilter.builder().withNameContains(nameContains);
            if (types != null) {
              filter.withTypes(
                  Arrays.stream(types.split(",", -1))
                      .map(DomainMembershipOperations::parseMemberType)
                      .toArray(MetadataObject.Type[]::new));
            }
            if (domains != null) {
              filter.withDomains(domains.toArray(new String[0]));
            }
            // Domain visibility and member authorization will be added with domain access control.
            DomainMemberDTO[] members =
                Arrays.stream(domainDispatcher.listMembers(metalake, domain, filter.build()))
                    .map(DomainMemberDTO::from)
                    .toArray(DomainMemberDTO[]::new);
            return Utils.ok(new DomainMemberListResponse(members));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleDomainException(OperationType.LIST, domain, metalake, e);
    }
  }

  private static MetadataObject.Type parseMemberType(String value) {
    try {
      MetadataObject.Type type = MetadataObject.Type.valueOf(value.trim().toUpperCase(Locale.ROOT));
      if (DomainMember.SUPPORTED_TYPES.contains(type)) {
        return type;
      }
    } catch (IllegalArgumentException ignored) {
      // Report unknown and unsupported types with the same public API error message.
    }
    throw new IllegalArgumentException(
        "Unsupported domain member type '"
            + value
            + "'. Supported types: "
            + DomainMember.SUPPORTED_TYPES);
  }
}
