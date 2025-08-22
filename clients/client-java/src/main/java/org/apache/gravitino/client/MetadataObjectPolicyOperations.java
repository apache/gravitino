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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.dto.requests.PoliciesAssociateRequest;
import org.apache.gravitino.dto.responses.NameListResponse;
import org.apache.gravitino.dto.responses.PolicyListResponse;
import org.apache.gravitino.dto.responses.PolicyResponse;
import org.apache.gravitino.exceptions.NoSuchPolicyException;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.SupportsPolicies;
import org.apache.gravitino.rest.RESTUtils;

/**
 * The implementation of {@link SupportsPolicies}. This interface will be composited into catalog,
 * schema, table, fileset and topic to provide policy operations for these metadata objects
 */
class MetadataObjectPolicyOperations implements SupportsPolicies {

  private final String metalakeName;

  private final RESTClient restClient;

  private final String policyRequestPath;

  MetadataObjectPolicyOperations(
      String metalakeName, MetadataObject metadataObject, RESTClient restClient) {
    this.metalakeName = metalakeName;
    this.restClient = restClient;
    this.policyRequestPath =
        String.format(
            "api/metalakes/%s/objects/%s/%s/policies",
            RESTUtils.encodeString(metalakeName),
            metadataObject.type().name().toLowerCase(Locale.ROOT),
            RESTUtils.encodeString(metadataObject.fullName()));
  }

  @Override
  public String[] listPolicies() {
    NameListResponse resp =
        restClient.get(
            policyRequestPath,
            NameListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.policyErrorHandler());

    resp.validate();
    return resp.getNames();
  }

  @Override
  public Policy[] listPolicyInfos() {
    PolicyListResponse resp =
        restClient.get(
            policyRequestPath,
            ImmutableMap.of("details", "true"),
            PolicyListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.policyErrorHandler());

    resp.validate();
    return Arrays.stream(resp.getPolicies())
        .map(policyDTO -> new GenericPolicy(policyDTO, restClient, metalakeName))
        .toArray(Policy[]::new);
  }

  @Override
  public Policy getPolicy(String name) throws NoSuchPolicyException {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(name), "Policy name must not be null or empty");

    PolicyResponse resp =
        restClient.get(
            policyRequestPath + "/" + RESTUtils.encodeString(name),
            PolicyResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.policyErrorHandler());

    resp.validate();
    return new GenericPolicy(resp.getPolicy(), restClient, metalakeName);
  }

  @Override
  public String[] associatePolicies(String[] policiesToAdd, String[] policiesToRemove) {
    PoliciesAssociateRequest request =
        new PoliciesAssociateRequest(policiesToAdd, policiesToRemove);
    request.validate();

    NameListResponse resp =
        restClient.post(
            policyRequestPath,
            request,
            NameListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.policyErrorHandler());

    resp.validate();
    return resp.getNames();
  }
}
