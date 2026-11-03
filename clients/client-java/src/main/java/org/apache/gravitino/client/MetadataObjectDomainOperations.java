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

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.domain.SupportsDomains;
import org.apache.gravitino.dto.responses.DomainListResponse;
import org.apache.gravitino.dto.responses.NameListResponse;
import org.apache.gravitino.rest.RESTUtils;

/** Domain lookup for a leaf metadata object. */
class MetadataObjectDomainOperations implements SupportsDomains {
  private final RESTClient restClient;
  private final String metalake;
  private final String path;

  MetadataObjectDomainOperations(String metalake, MetadataObject object, RESTClient restClient) {
    this.restClient = restClient;
    this.metalake = metalake;
    this.path =
        String.format(
            "api/metalakes/%s/objects/%s/%s/domains",
            RESTUtils.encodeString(metalake),
            object.type().name().toLowerCase(Locale.ROOT),
            RESTUtils.encodeString(object.fullName()));
  }

  /** {@inheritDoc} */
  @Override
  public String[] listDomains() {
    NameListResponse response =
        restClient.get(
            path,
            NameListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.domainErrorHandler());
    response.validate();
    return response.getNames();
  }

  /** {@inheritDoc} */
  @Override
  public Domain[] listDomainsInfo() {
    DomainListResponse response =
        restClient.get(
            path,
            ImmutableMap.of("details", "true"),
            DomainListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.domainErrorHandler());
    response.validate();
    return Arrays.stream(response.getDomains())
        .map(dto -> new GenericDomain(dto, restClient, metalake))
        .toArray(Domain[]::new);
  }
}
