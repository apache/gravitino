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
package org.apache.gravitino.authorization.ranger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import org.apache.gravitino.authorization.Owner;
import org.apache.ranger.RangerClient;
import org.apache.ranger.plugin.model.RangerBaseModelObject;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

/**
 * Builds Ranger policies with both null-safe collection modes of the Ranger model classes.
 *
 * <p>Ranger 2.8 keeps this mode in a JVM-wide static. The default ("v1") gives every model object
 * mutable empty collections. Initializing a {@link
 * org.apache.ranger.plugin.service.RangerBasePlugin} in the same JVM, as the Spark authz plugin
 * used by other tests and by some deployments does, switches it to "v2", where empty collections
 * are immutable. The tests restore the exact previous mode so that they leave the JVM as they found
 * it.
 */
public class TestRangerImmutableModelDefaults {

  private RangerBaseModelObject.NullSafeSupplier previousSupplier;
  private RangerHelper rangerHelper;

  @BeforeEach
  public void setUp() {
    previousSupplier = SupplierAccess.current();
    rangerHelper =
        new RangerHelper(
            Mockito.mock(RangerClient.class),
            "admin",
            "service",
            ImmutableSet.of(RangerPrivileges.RangerHadoopSQLPrivilege.ALL),
            ImmutableList.of());
  }

  @AfterEach
  public void tearDown() {
    RangerBaseModelObject.setNullSafeSupplier(previousSupplier);
  }

  @ParameterizedTest
  @ValueSource(strings = {"v1", RangerBaseModelObject.NULL_SAFE_SUPPLIER_V2})
  public void testUpdatePolicyOwnerOnNewPolicy(String supplier) {
    RangerBaseModelObject.setNullSafeSupplier(supplier);
    Owner owner = Mockito.mock(Owner.class);
    Mockito.when(owner.type()).thenReturn(Owner.Type.USER);
    Mockito.when(owner.name()).thenReturn("user1");
    RangerPolicy policy = new RangerPolicy();

    rangerHelper.updatePolicyOwner(policy, null, owner);

    Assertions.assertEquals(1, policy.getPolicyItems().size());
    RangerPolicy.RangerPolicyItem policyItem = policy.getPolicyItems().get(0);
    Assertions.assertEquals(Collections.singletonList("user1"), policyItem.getUsers());
    Assertions.assertEquals(
        Collections.singletonList(RangerHelper.GRAVITINO_OWNER_ROLE), policyItem.getRoles());
    Assertions.assertEquals(
        RangerPrivileges.RangerHadoopSQLPrivilege.ALL.getName(),
        policyItem.getAccesses().get(0).getType());

    // Changing the owner removes the previous one from the same item.
    Owner newOwner = Mockito.mock(Owner.class);
    Mockito.when(newOwner.type()).thenReturn(Owner.Type.USER);
    Mockito.when(newOwner.name()).thenReturn("user2");
    rangerHelper.updatePolicyOwner(policy, owner, newOwner);
    Assertions.assertEquals(
        Collections.singletonList("user2"), policy.getPolicyItems().get(0).getUsers());
  }

  @ParameterizedTest
  @ValueSource(strings = {"v1", RangerBaseModelObject.NULL_SAFE_SUPPLIER_V2})
  public void testUpdatePolicyOwnerRoleOnNewPolicy(String supplier) {
    RangerBaseModelObject.setNullSafeSupplier(supplier);
    RangerPolicy policy = new RangerPolicy();
    String ownerRole = RangerHelper.GRAVITINO_CATALOG_OWNER_ROLE;

    rangerHelper.updatePolicyOwnerRole(policy, ownerRole);

    Assertions.assertEquals(1, policy.getPolicyItems().size());
    Assertions.assertEquals(
        Collections.singletonList(ownerRole), policy.getPolicyItems().get(0).getRoles());
  }

  @ParameterizedTest
  @ValueSource(strings = {"v1", RangerBaseModelObject.NULL_SAFE_SUPPLIER_V2})
  public void testHasGravitinoManagedPolicyItemDoesNotChangePolicy(String supplier) {
    RangerBaseModelObject.setNullSafeSupplier(supplier);
    RangerPolicy policy = new RangerPolicy();
    RangerPolicy.RangerPolicyItem denyItem = new RangerPolicy.RangerPolicyItem();
    denyItem.addRole(RangerHelper.GRAVITINO_OWNER_ROLE);
    policy.addDenyPolicyItem(denyItem);

    Assertions.assertTrue(RangerHelper.hasGravitinoManagedPolicyItem(policy));
    Assertions.assertTrue(policy.getPolicyItems().isEmpty());
    Assertions.assertEquals(1, policy.getDenyPolicyItems().size());
  }

  @ParameterizedTest
  @ValueSource(strings = {"v1", RangerBaseModelObject.NULL_SAFE_SUPPLIER_V2})
  public void testCreatePolicyAddResourcesOnNewPolicy(String supplier) {
    RangerBaseModelObject.setNullSafeSupplier(supplier);
    RangerAuthorizationHadoopSQLPlugin plugin =
        Mockito.mock(RangerAuthorizationHadoopSQLPlugin.class, Mockito.CALLS_REAL_METHODS);

    RangerPolicy policy =
        plugin.createPolicyAddResources(
            new RangerHadoopSQLMetadataObject(
                "db1", "table1", RangerHadoopSQLMetadataObject.Type.TABLE));

    Assertions.assertEquals(2, policy.getResources().size());
    Assertions.assertEquals(
        Collections.singletonList("db1"),
        policy.getResources().get(plugin.policyResourceDefinesRule().get(0)).getValues());
    Assertions.assertEquals(
        Collections.singletonList("table1"),
        policy.getResources().get(plugin.policyResourceDefinesRule().get(1)).getValues());
  }

  /** Reads the current JVM-wide supplier, which Ranger only exposes to subclasses. */
  private static class SupplierAccess extends RangerBaseModelObject {
    private static final long serialVersionUID = 1L;

    static NullSafeSupplier current() {
      return new SupplierAccess().getNullSafeSupplier();
    }
  }
}
