/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.server.web.rest;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import java.util.Locale;
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
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.CredentialOperationDispatcher;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.S3TokenCredential;
import org.apache.gravitino.dto.credential.CredentialDTO;
import org.apache.gravitino.dto.responses.CredentialResponse;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.server.web.Utils;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/metalakes/{metalake}/objects/{type}/{fullName}/credentials")
public class MetadataObjectCredentialOperations {

  private static final Logger LOG =
      LoggerFactory.getLogger(MetadataObjectCredentialOperations.class);

  private CredentialOperationDispatcher dispatcher;

  @SuppressWarnings("unused")
  @Context
  private HttpServletRequest httpRequest;

  @Inject
  public MetadataObjectCredentialOperations(CredentialOperationDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  @GET
  @Path("hello")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "get-credential." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "get-credential", absolute = true)
  public Response getStaticCredential(@PathParam("metalake") String metalake) {
    return Utils.ok(getS3Credential());
  }

  @GET
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "get-credential." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "get-credential", absolute = true)
  public Response getCredential(
      @PathParam("metalake") String metalake,
      @PathParam("type") String type,
      @PathParam("fullName") String fullName,
      @QueryParam("credential-type") String credentialType) {
    LOG.info(
        "Received get credential request for object type: {}, full name: {} under metalake: {}",
        type,
        fullName,
        metalake);

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            MetadataObject object =
                MetadataObjects.parse(
                    fullName, MetadataObject.Type.valueOf(type.toUpperCase(Locale.ROOT)));

            NameIdentifier identifier = MetadataObjectUtil.toEntityIdent(metalake, object);
            Credential credential = getCredential(identifier, credentialType);
            if (credential == null) {
              return Utils.ok(new CredentialResponse(new CredentialDTO[0]));
            }
            return Utils.ok(
                new CredentialResponse(DTOConverters.toDTO(new Credential[] {credential})));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleCredentialException(OperationType.GET, "", fullName, e);
    }
  }

  private Credential getS3Credential() {
    return new S3TokenCredential("access-id", "secret-key", "token", 1000);
  }

  private Credential getCredential(NameIdentifier identifier, String credentialType) {
    return dispatcher.getCredential(identifier, credentialType);
  }
}
