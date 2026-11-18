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
package org.apache.gravitino.dto.policy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Map;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.policy.AccessControlContent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPolicyContentDTO {

  @Test
  public void testCustomContentDtoRulesReturnsCustomRules() {
    Map<String, Object> customRules = ImmutableMap.of("rule1", "value1");
    PolicyContentDTO.CustomContentDTO content =
        PolicyContentDTO.CustomContentDTO.builder()
            .withCustomRules(customRules)
            .withProperties(ImmutableMap.of("k", "v"))
            .withSupportedObjectTypes(ImmutableSet.of(MetadataObject.Type.TABLE))
            .build();

    // Before the fix, CustomContentDTO inherited PolicyContent.rules() which throws
    // UnsupportedOperationException, even though the domain CustomContent returns its
    // customRules from rules().
    Assertions.assertEquals(customRules, content.rules());
  }

  @Test
  public void testAccessControlContentDtoRules() {
    PolicyContentDTO.AccessControlContentDTO content =
        PolicyContentDTO.AccessControlContentDTO.builder()
            .withPrivileges(ImmutableList.of(Privilege.Name.SELECT_TABLE))
            .withApplicableRoles(ImmutableList.of("analyst"))
            .build();

    Assertions.assertEquals(
        ImmutableList.of(Privilege.Name.SELECT_TABLE.name()),
        content.rules().get(AccessControlContent.PRIVILEGES_KEY));
    Assertions.assertEquals(
        ImmutableList.of("analyst"),
        content.rules().get(AccessControlContent.APPLICABLE_ROLES_KEY));
    Assertions.assertTrue(content.properties().isEmpty());
    Assertions.assertFalse(content.supportedObjectTypes().contains(MetadataObject.Type.METALAKE));
    Assertions.assertDoesNotThrow(content::validate);
  }

  @Test
  public void testAccessControlContentDtoAbsentFields() {
    PolicyContentDTO.AccessControlContentDTO content =
        PolicyContentDTO.AccessControlContentDTO.builder().build();

    Assertions.assertTrue(content.privileges().isEmpty());
    Assertions.assertTrue(content.applicableRoles().isEmpty());
  }

  @Test
  public void testAccessControlContentDtoDelegatesValidation() {
    // A DTO that skips validate() would let a privilege outside the allowlist reach the server, so
    // the delegation to the domain content is the contract under test here, not the rule itself.
    PolicyContentDTO.AccessControlContentDTO unpermittedPrivilege =
        PolicyContentDTO.AccessControlContentDTO.builder()
            .withPrivileges(ImmutableList.of(Privilege.Name.CREATE_TABLE))
            .withApplicableRoles(ImmutableList.of("analyst"))
            .build();
    Assertions.assertThrows(
        IllegalArgumentException.class,
        unpermittedPrivilege::validate,
        "expected a privilege outside PERMITTED_PRIVILEGES to be rejected");

    PolicyContentDTO.AccessControlContentDTO noRoles =
        PolicyContentDTO.AccessControlContentDTO.builder()
            .withPrivileges(ImmutableList.of(Privilege.Name.SELECT_TABLE))
            .build();
    Assertions.assertThrows(IllegalArgumentException.class, noRoles::validate);

    PolicyContentDTO.AccessControlContentDTO noPrivileges =
        PolicyContentDTO.AccessControlContentDTO.builder()
            .withApplicableRoles(ImmutableList.of("analyst"))
            .build();
    Assertions.assertThrows(IllegalArgumentException.class, noPrivileges::validate);
  }
}
