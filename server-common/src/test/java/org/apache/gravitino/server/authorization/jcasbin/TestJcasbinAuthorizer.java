/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.server.authorization.jcasbin;

import static org.apache.gravitino.authorization.Privilege.Name.USE_CATALOG;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import java.security.Principal;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.server.authorization.MetadataIdConverter;
import org.apache.gravitino.server.web.ObjectMapperProvider;
import org.apache.gravitino.storage.relational.po.RolePO;
import org.apache.gravitino.storage.relational.po.SecurableObjectPO;
import org.apache.gravitino.storage.relational.service.MetalakeMetaService;
import org.apache.gravitino.storage.relational.service.RoleMetaService;
import org.apache.gravitino.storage.relational.service.UserMetaService;
import org.apache.gravitino.utils.PrincipalUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/** Test of {@link JcasbinAuthorizer} */
public class TestJcasbinAuthorizer {

  private static final Long USER_METALAKE_ID = 1L;

  private static final Long USER_ID = 2L;

  private static final Long ROLE_ID = 3L;

  private static final Long CATALOG_ID = 4L;

  private static UserMetaService mockUserMetaService = mock(UserMetaService.class);

  private static MetalakeMetaService metalakeMetaService = mock(MetalakeMetaService.class);

  private static RoleMetaService roleMetaService = mock(RoleMetaService.class);

  private static MockedStatic<PrincipalUtils> principalUtilsMockedStatic;

  private static MockedStatic<UserMetaService> userMetaServiceMockedStatic;

  private static MockedStatic<MetalakeMetaService> metalakeMetaServiceMockedStatic;

  private static MockedStatic<MetadataIdConverter> metadataIdConverterMockedStatic;

  private static MockedStatic<RoleMetaService> roleMetaServiceMockedStatic;

  private static JcasbinAuthorizer jcasbinAuthorizer;

  @BeforeAll
  public static void setup() {
    jcasbinAuthorizer = new JcasbinAuthorizer();
    jcasbinAuthorizer.initialize();
    when(mockUserMetaService.getUserIdByMetalakeIdAndName(USER_METALAKE_ID, "tester"))
        .thenReturn(USER_ID);
    when(metalakeMetaService.getMetalakeIdByName("testMetalake")).thenReturn(USER_METALAKE_ID);

    principalUtilsMockedStatic = mockStatic(PrincipalUtils.class);
    userMetaServiceMockedStatic = mockStatic(UserMetaService.class);
    metalakeMetaServiceMockedStatic = mockStatic(MetalakeMetaService.class);
    roleMetaServiceMockedStatic = mockStatic(RoleMetaService.class);
    metadataIdConverterMockedStatic = mockStatic(MetadataIdConverter.class);
    principalUtilsMockedStatic
        .when(PrincipalUtils::getCurrentPrincipal)
        .thenReturn(new UserPrincipal("tester"));
    metalakeMetaServiceMockedStatic
        .when(MetalakeMetaService::getInstance)
        .thenReturn(metalakeMetaService);
    metadataIdConverterMockedStatic
        .when(() -> MetadataIdConverter.doConvert(any()))
        .thenReturn(CATALOG_ID);
    SecurableObjectPO securableObjectPO = getSecurableObjectPO();
    roleMetaServiceMockedStatic
        .when(() -> RoleMetaService.listSecurableObjectsByRoleId(eq(ROLE_ID)))
        .thenReturn(ImmutableList.of(securableObjectPO));
    roleMetaServiceMockedStatic.when(RoleMetaService::getInstance).thenReturn(roleMetaService);
    userMetaServiceMockedStatic.when(UserMetaService::getInstance).thenReturn(mockUserMetaService);
  }

  @AfterAll
  public static void stop() {
    if (principalUtilsMockedStatic != null) {
      principalUtilsMockedStatic.close();
    }
    if (userMetaServiceMockedStatic != null) {
      userMetaServiceMockedStatic.close();
    }
    if (metalakeMetaServiceMockedStatic != null) {
      metalakeMetaServiceMockedStatic.close();
    }
  }

  @Test
  public void testAuthorize() {
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    assertFalse(doAuthorize(currentPrincipal));
    RolePO rolePO1 = getRolePO(ROLE_ID);
    // Mock adds roles to users.
    when(roleMetaService.listRolesByUserId(USER_ID)).thenReturn(ImmutableList.of(rolePO1));
    assertTrue(doAuthorize(currentPrincipal));
    // Test role cache.
    // When permissions are changed but handleRolePrivilegeChange is not executed, the system will
    // use the cached permissions in JCasbin, so authorize can succeed.
    long newRoleId = -1L;
    RolePO rolePO2 = getRolePO(newRoleId);
    when(roleMetaService.listRolesByUserId(USER_ID)).thenReturn(ImmutableList.of(rolePO2));
    assertTrue(doAuthorize(currentPrincipal));
    // After clearing the cache, authorize will fail
    jcasbinAuthorizer.handleRolePrivilegeChange(ROLE_ID);
    assertFalse(doAuthorize(currentPrincipal));
    // When the user is re-assigned the correct role, the authorization will succeed.
    when(roleMetaService.listRolesByUserId(USER_ID)).thenReturn(ImmutableList.of(rolePO1));
    assertTrue(doAuthorize(currentPrincipal));
  }

  private boolean doAuthorize(Principal currentPrincipal) {
    return jcasbinAuthorizer.authorize(
        currentPrincipal,
        "testMetalake",
        MetadataObjects.of(null, "testCatalog", MetadataObject.Type.CATALOG),
        USE_CATALOG);
  }

  private static RolePO getRolePO(Long roleId) {
    return RolePO.builder()
        .withRoleId(roleId)
        .withMetalakeId(USER_METALAKE_ID)
        .withRoleName("roleName")
        .withAuditInfo("")
        .withDeletedAt(0L)
        .withCurrentVersion(1L)
        .withLastVersion(1L)
        .build();
  }

  private static SecurableObjectPO getSecurableObjectPO() {
    ImmutableList<Privilege.Name> privileges = ImmutableList.of(USE_CATALOG);
    ImmutableList<String> conditions = ImmutableList.of("ALLOW");
    ObjectMapper objectMapper = ObjectMapperProvider.objectMapper();
    try {
      return SecurableObjectPO.builder()
          .withType(String.valueOf(MetadataObject.Type.CATALOG))
          .withMetadataObjectId(CATALOG_ID)
          .withRoleId(ROLE_ID)
          .withPrivilegeNames(objectMapper.writeValueAsString(privileges))
          .withPrivilegeConditions(objectMapper.writeValueAsString(conditions))
          .withDeletedAt(0L)
          .withCurrentVersion(1L)
          .withLastVersion(1L)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
