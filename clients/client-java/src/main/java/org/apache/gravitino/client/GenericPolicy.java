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

import static org.apache.gravitino.dto.util.DTOConverters.fromDTO;

import java.util.Collections;
import java.util.Optional;
import org.apache.gravitino.Audit;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.dto.policy.PolicyDTO;
import org.apache.gravitino.dto.responses.MetadataObjectListResponse;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.gravitino.rest.RESTUtils;

/** Represents a generic policy. */
class GenericPolicy implements Policy, Policy.AssociatedObjects {

  private final PolicyDTO policyDTO;

  private final PolicyContent content;

  private final RESTClient restClient;

  private final String metalake;

  GenericPolicy(PolicyDTO policyDTO, RESTClient restClient, String metalake) {
    this.policyDTO = policyDTO;
    this.content = fromDTO(policyDTO.content());
    this.restClient = restClient;
    this.metalake = metalake;
  }

  @Override
  public String name() {
    return policyDTO.name();
  }

  @Override
  public String policyType() {
    return policyDTO.policyType();
  }

  @Override
  public String comment() {
    return policyDTO.comment();
  }

  @Override
  public boolean enabled() {
    return policyDTO.enabled();
  }

  @Override
  public PolicyContent content() {
    return content;
  }

  @Override
  public Optional<Boolean> inherited() {
    return policyDTO.inherited();
  }

  @Override
  public Audit auditInfo() {
    return policyDTO.auditInfo();
  }

  @Override
  public AssociatedObjects associatedObjects() {
    return this;
  }

  @Override
  public MetadataObject[] objects() {
    MetadataObjectListResponse resp =
        restClient.get(
            String.format(
                "api/metalakes/%s/policies/%s/objects",
                RESTUtils.encodeString(metalake), RESTUtils.encodeString(name())),
            MetadataObjectListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.policyErrorHandler());

    resp.validate();
    return resp.getMetadataObjects();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof GenericPolicy)) {
      return false;
    }

    GenericPolicy that = (GenericPolicy) obj;
    return policyDTO.equals(that.policyDTO);
  }

  @Override
  public int hashCode() {
    return policyDTO.hashCode();
  }

  @Override
  public String toString() {
    return "GenericPolicy{"
        + "policyDTO="
        + policyDTO
        + ", content="
        + content
        + ", metalake='"
        + metalake
        + '\''
        + '}';
  }
}
