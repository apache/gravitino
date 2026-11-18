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

package org.apache.gravitino.authorization;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.tag.TagAssignment;
import org.apache.gravitino.utils.NamespaceUtil;

/** Shared tag-policy entities for authorization tests. */
final class TagPolicyTestUtils {
  static final String METALAKE = "metalake";

  private TagPolicyTestUtils() {}

  static TagEntity tag(long id, String name, TagAssignment assignment) {
    return TagEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(NamespaceUtil.ofTag(METALAKE))
        .withProperties(Collections.emptyMap())
        .withAuditInfo(audit())
        .withAssignment(assignment)
        .build();
  }

  static PolicyEntity policy(
      long id, boolean enabled, List<Privilege.Name> privileges, List<String> roles) {
    return PolicyEntity.builder()
        .withId(id)
        .withName("policy_" + id)
        .withNamespace(NamespaceUtil.ofPolicy(METALAKE))
        .withPolicyType(Policy.BuiltInType.ACCESS_CONTROL)
        .withEnabled(enabled)
        .withContent(PolicyContents.accessControl(privileges, roles))
        .withAuditInfo(audit())
        .build();
  }

  static AuditInfo audit() {
    return AuditInfo.builder().withCreator("tester").withCreateTime(Instant.EPOCH).build();
  }

  static PolicyEntity customPolicy() {
    return PolicyEntity.builder()
        .withId(99L)
        .withName("custom")
        .withNamespace(NamespaceUtil.ofPolicy(METALAKE))
        .withPolicyType(Policy.BuiltInType.CUSTOM)
        .withEnabled(true)
        .withContent(
            PolicyContents.custom(
                Collections.singletonMap("rule", "value"),
                Collections.singleton(MetadataObject.Type.TABLE),
                Collections.emptyMap()))
        .withAuditInfo(audit())
        .build();
  }
}
