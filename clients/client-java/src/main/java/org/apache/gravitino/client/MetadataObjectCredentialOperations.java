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

import java.util.Collections;
import java.util.Locale;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.audit.CallerContext;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.SupportsCredentials;
import org.apache.gravitino.dto.responses.CredentialResponse;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.rest.RESTUtils;

/**
 * The implementation of {@link SupportsCredentials}. This interface will be composited into
 * catalog, table, fileset to provide credential operations for these metadata objects
 */
class MetadataObjectCredentialOperations implements SupportsCredentials {

  private final RESTClient restClient;

  private final String credentialRequestPath;

  MetadataObjectCredentialOperations(
      String metalakeName, MetadataObject metadataObject, RESTClient restClient) {
    this.restClient = restClient;
    this.credentialRequestPath =
        String.format(
            "api/metalakes/%s/objects/%s/%s/credentials",
            RESTUtils.encodeString(metalakeName),
            metadataObject.type().name().toLowerCase(Locale.ROOT),
            RESTUtils.encodeString(metadataObject.fullName()));
  }

  @Override
  public Credential[] getCredentials() {
    try {
      CallerContext callerContext = CallerContext.CallerContextHolder.get();
      CredentialResponse resp =
          restClient.get(
              credentialRequestPath,
              CredentialResponse.class,
              callerContext != null ? callerContext.context() : Collections.emptyMap(),
              ErrorHandlers.credentialErrorHandler());
      resp.validate();
      return DTOConverters.fromDTO(resp.getCredentials());
    } finally {
      // Clear the caller context
      CallerContext.CallerContextHolder.remove();
    }
  }
}
