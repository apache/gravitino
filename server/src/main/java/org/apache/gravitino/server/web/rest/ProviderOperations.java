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
import java.util.List;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.apache.gravitino.dto.responses.SecretProviderListResponse;
import org.apache.gravitino.dto.secret.SecretProviderDTO;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.secret.SecretProviderInfo;
import org.apache.gravitino.secret.SecretProviderRegistry;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.web.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lists process-global secrets-provider discovery metadata.
 *
 * <p>The provider registry is server configuration, not a metalake resource, so the path has no
 * metalake segment. Access is restricted to service administrators.
 */
@Path("/")
public class ProviderOperations {

  private static final Logger LOG = LoggerFactory.getLogger(ProviderOperations.class);

  private final SecretProviderRegistry secretProviderRegistry;

  @Context private HttpServletRequest httpRequest;

  /**
   * Creates a secrets-provider REST resource.
   *
   * @param secretProviderRegistry the process-owned provider registry
   */
  @Inject
  public ProviderOperations(SecretProviderRegistry secretProviderRegistry) {
    this.secretProviderRegistry = secretProviderRegistry;
  }

  /**
   * Lists configured secrets providers.
   *
   * @return a list of provider names and types
   */
  @GET
  @Path("/secrets/providers")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "list-secret-providers." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-secret-providers", absolute = true)
  @AuthorizationExpression(
      expression = "SERVICE_ADMIN",
      errorMessage = "Only service admins can list secrets providers")
  public Response listSecretProviders() {
    LOG.info("Received list secrets providers request.");
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            List<SecretProviderInfo> infos = secretProviderRegistry.listProviders();
            SecretProviderDTO[] providers =
                infos.stream()
                    .map(
                        info ->
                            SecretProviderDTO.builder()
                                .withName(info.name())
                                .withType(info.type())
                                .build())
                    .toArray(SecretProviderDTO[]::new);
            Response response = Utils.ok(new SecretProviderListResponse(providers));
            LOG.info("Listed {} secrets providers", providers.length);
            return response;
          });
    } catch (Exception e) {
      LOG.error("Failed to list secrets providers", e);
      return Utils.internalError(e.getMessage(), e);
    }
  }
}
