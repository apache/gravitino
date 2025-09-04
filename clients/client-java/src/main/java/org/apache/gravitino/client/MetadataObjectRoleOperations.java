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
import org.apache.gravitino.authorization.SupportsRoles;
import org.apache.gravitino.dto.responses.NameListResponse;
import org.apache.gravitino.rest.RESTUtils;

class MetadataObjectRoleOperations implements SupportsRoles {

  private final RESTClient restClient;

  private final String roleRequestPath;

  MetadataObjectRoleOperations(
      String metalakeName, MetadataObject metadataObject, RESTClient restClient) {
    this.restClient = restClient;
    this.roleRequestPath =
        String.format(
            "api/metalakes/%s/objects/%s/%s/roles",
            RESTUtils.encodeString(metalakeName),
            metadataObject.type().name().toLowerCase(Locale.ROOT),
            RESTUtils.encodeString(metadataObject.fullName()));
  }

  @Override
  public String[] listBindingRoleNames() {
    NameListResponse resp =
        restClient.get(
            roleRequestPath,
            NameListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.roleErrorHandler());
    resp.validate();

    return resp.getNames();
  }
}
