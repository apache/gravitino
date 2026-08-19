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
import java.util.Map;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.dto.responses.SecretPropertiesResponse;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.secret.SupportsSecretProperties;

/**
 * The implementation of {@link SupportsSecretProperties}. Composited into catalog and fileset
 * clients to fetch secret-backed properties as plaintext.
 */
class MetadataObjectSecretPropertiesOperations implements SupportsSecretProperties {

  private final RESTClient restClient;

  private final String secretPropertiesRequestPath;

  MetadataObjectSecretPropertiesOperations(
      String metalakeName, MetadataObject metadataObject, RESTClient restClient) {
    this.restClient = restClient;
    this.secretPropertiesRequestPath =
        String.format(
            "api/metalakes/%s/objects/%s/%s/secret-properties",
            RESTUtils.encodeString(metalakeName),
            metadataObject.type().name().toLowerCase(Locale.ROOT),
            RESTUtils.encodeString(metadataObject.fullName()));
  }

  @Override
  public Map<String, String> getSecretProperties() {
    SecretPropertiesResponse resp =
        restClient.get(
            secretPropertiesRequestPath,
            SecretPropertiesResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.secretPropertiesErrorHandler());
    resp.validate();
    return resp.getSecretProperties();
  }
}
