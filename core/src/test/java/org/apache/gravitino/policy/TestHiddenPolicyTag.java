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
package org.apache.gravitino.policy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.Collections;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.tag.TagValueConstraint;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.Test;

/** Tests hidden tag models and reserved namespace protection. */
public class TestHiddenPolicyTag {

  /** Only the complete internal prefix is reserved, including alternate casing. */
  @Test
  public void testReservedNames() {
    assertTrue(HiddenPolicyTag.isReservedName("GRAVITINO_HIDDEN_123"));
    assertFalse(HiddenPolicyTag.isReservedName(null));
    assertFalse(HiddenPolicyTag.isReservedName("data_domain"));
    assertFalse(HiddenPolicyTag.isReservedName("reserved_123"));
    assertFalse(HiddenPolicyTag.isReservedName("gravitino_hidden"));
    assertFalse(HiddenPolicyTag.isReservedName("gravitino_hiddenish"));
    assertEquals("gravitino_hidden_123", HiddenPolicyTag.name(123));
  }

  /** New policies use the tag model, with matching ownership and audit metadata. */
  @Test
  public void testCreateHiddenTagModel() throws Exception {
    AuditInfo audit = AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();
    PolicyEntity policy =
        PolicyEntity.builder()
            .withId(12L)
            .withName("policy")
            .withNamespace(NamespaceUtil.ofPolicy("m"))
            .withPolicyType(Policy.BuiltInType.CUSTOM)
            .withEnabled(false)
            .withContent(
                PolicyContents.custom(
                    Collections.singletonMap("rule", 1),
                    Collections.singleton(MetadataObject.Type.TABLE),
                    null))
            .withAuditInfo(audit)
            .build();
    TagEntity tag = HiddenPolicyTag.create(policy, 34L);
    assertEquals(34L, tag.id());
    assertTrue(HiddenPolicyTag.isReservedName(tag.name()));
    assertEquals(HiddenPolicyTag.name(policy.id()), tag.name());
    assertEquals(NamespaceUtil.ofTag("m"), tag.namespace());
    assertEquals(audit, tag.auditInfo());
    assertEquals(TagValueConstraint.noValue(), tag.valueConstraint());
    assertFalse(tag.assignment().isPresent());
    assertEquals(Collections.singletonMap("gravitino.internal.policy-id", "12"), tag.properties());
  }
}
