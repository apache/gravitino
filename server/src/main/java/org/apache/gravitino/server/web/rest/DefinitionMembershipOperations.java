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
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.apache.gravitino.Entity;
import org.apache.gravitino.dto.responses.DefinitionMemberListResponse;
import org.apache.gravitino.glossary.GlossaryManager;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.annotations.AuthorizationMetadata;
import org.apache.gravitino.server.web.Utils;

/** Administration of definitions assigned directly to a Domain. */
@Path("/metalakes/{metalake}/domains/{domain}/definition-members")
@Produces("application/vnd.gravitino.v1+json")
public class DefinitionMembershipOperations {
  private final GlossaryManager manager;
  @Context private HttpServletRequest request;

  /** Creates the resource with the manager that authorizes membership changes. */
  @Inject
  public DefinitionMembershipOperations(GlossaryManager manager) {
    this.manager = manager;
  }

  /** Lists current definition names, including drafts, for a Domain administrator. */
  @GET
  @AuthorizationExpression(expression = "")
  @Timed(name = "list-definition-members." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-definition-members", absolute = true)
  public Response list(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("domain") String domain,
      @QueryParam("limit") @DefaultValue("20") String limit,
      @QueryParam("offset") @DefaultValue("0") String offset) {
    try {
      return Utils.doAs(
          request,
          () ->
              Utils.ok(
                  new DefinitionMemberListResponse(
                      manager.listDefinitionMembers(
                          metalake, domain, Integer.parseInt(limit), Integer.parseInt(offset)))));
    } catch (Exception e) {
      return ExceptionHandlers.handleDomainException(OperationType.LIST, domain, metalake, e);
    }
  }
}
