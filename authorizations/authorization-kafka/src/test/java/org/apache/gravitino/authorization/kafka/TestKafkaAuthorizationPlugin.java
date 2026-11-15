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
package org.apache.gravitino.authorization.kafka;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.gravitino.Audit;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.RelationalEntity;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.authorization.Group;
import org.apache.gravitino.authorization.MetadataObjectChange;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.RoleChange;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.authorization.User;
import org.apache.gravitino.connector.authorization.BaseAuthorization;
import org.apache.gravitino.exceptions.AuthorizationPluginException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TopicEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateAclsResult;
import org.apache.kafka.clients.admin.DeleteAclsResult;
import org.apache.kafka.clients.admin.DescribeAclsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.Resource;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

public class TestKafkaAuthorizationPlugin {

  private static final String METALAKE = "test_ml";
  private static final String CATALOG = "kafka_cat";

  private AdminClient adminClient;
  private Set<AclBinding> brokerAcls;
  private InMemoryStateProvider stateProvider;
  private KafkaAuthorizationPlugin plugin;

  @BeforeEach
  public void setUp() {
    brokerAcls = ConcurrentHashMap.newKeySet();
    adminClient = mockAdminClient(brokerAcls);
    stateProvider = new InMemoryStateProvider();
    stateProvider.catalogNames.put(1L, CATALOG);

    KafkaAuthorizationProperties props =
        new KafkaAuthorizationProperties(
            ImmutableMap.of(
                "bootstrap.servers", "localhost:9092", BaseAuthorization.CATALOG_ID, "1"));
    props.validate();
    plugin = new KafkaAuthorizationPlugin(METALAKE, props, adminClient, stateProvider, null);
  }

  @Test
  public void testGrantProduceAndConsumeWithNonKafkaPrivileges() {
    SecurableObject catalogObj =
        SecurableObjects.ofCatalog(
            CATALOG, Collections.singletonList(Privileges.UseCatalog.allow()));
    SecurableObject schemaObj =
        SecurableObjects.ofSchema(
            catalogObj, "default", Collections.singletonList(Privileges.UseSchema.allow()));
    SecurableObject ordersTopic =
        SecurableObjects.ofTopic(
            schemaObj,
            "orders",
            Arrays.asList(Privileges.ProduceTopic.allow(), Privileges.ConsumeTopic.allow()));
    Role role = testRole("orders_rw", catalogObj, schemaObj, ordersTopic);

    stateProvider.grantRoleToUser("alice", role);
    Assertions.assertTrue(
        plugin.onGrantedRolesToUser(Collections.singletonList(role), testUser("alice")));

    Assertions.assertTrue(
        brokerAcls.contains(
            acl(
                ResourceType.TOPIC,
                "orders",
                "User:alice",
                AclOperation.WRITE,
                AclPermissionType.ALLOW)));
    Assertions.assertTrue(
        brokerAcls.contains(
            acl(
                ResourceType.TOPIC,
                "orders",
                "User:alice",
                AclOperation.READ,
                AclPermissionType.ALLOW)));
    Assertions.assertTrue(
        brokerAcls.contains(
            acl(
                ResourceType.TOPIC,
                "orders",
                "User:alice",
                AclOperation.DESCRIBE,
                AclPermissionType.ALLOW)));
    Assertions.assertTrue(
        brokerAcls.contains(
            acl(
                ResourceType.GROUP,
                "*",
                "User:alice",
                AclOperation.READ,
                AclPermissionType.ALLOW)));
    Assertions.assertTrue(
        brokerAcls.contains(
            acl(
                ResourceType.CLUSTER,
                Resource.CLUSTER_NAME,
                "User:alice",
                AclOperation.IDEMPOTENT_WRITE,
                AclPermissionType.ALLOW)));
    Assertions.assertEquals(5, brokerAcls.size());
  }

  @Test
  public void testDenyProduceDoesNotDenyDescribeOrIdempotentWrite() {
    SecurableObject catalogObj = SecurableObjects.ofCatalog(CATALOG, Collections.emptyList());
    SecurableObject schemaObj =
        SecurableObjects.ofSchema(
            catalogObj,
            "default",
            Arrays.asList(Privileges.ProduceTopic.allow(), Privileges.ConsumeTopic.allow()));
    SecurableObject ordersTopic =
        SecurableObjects.ofTopic(
            schemaObj, "orders", Collections.singletonList(Privileges.ProduceTopic.deny()));
    Role role = testRole("deny_orders_write", schemaObj, ordersTopic);

    stateProvider.grantRoleToUser("bob", role);
    plugin.onGrantedRolesToUser(Collections.singletonList(role), testUser("bob"));

    Assertions.assertTrue(
        brokerAcls.contains(
            acl(
                ResourceType.TOPIC,
                "orders",
                "User:bob",
                AclOperation.WRITE,
                AclPermissionType.DENY)));
    Assertions.assertFalse(
        brokerAcls.contains(
            acl(
                ResourceType.TOPIC,
                "orders",
                "User:bob",
                AclOperation.DESCRIBE,
                AclPermissionType.DENY)));
    Assertions.assertFalse(
        brokerAcls.contains(
            acl(
                ResourceType.CLUSTER,
                Resource.CLUSTER_NAME,
                "User:bob",
                AclOperation.IDEMPOTENT_WRITE,
                AclPermissionType.DENY)));
  }

  @Test
  public void testMultiRoleOverlapPreservedOnRevokeAndRoleDelete() {
    SecurableObject catalogObj = SecurableObjects.ofCatalog(CATALOG, Collections.emptyList());
    SecurableObject schemaObj =
        SecurableObjects.ofSchema(catalogObj, "default", Collections.emptyList());
    SecurableObject topicObjProduce =
        SecurableObjects.ofTopic(
            schemaObj, "events", Collections.singletonList(Privileges.ProduceTopic.allow()));
    SecurableObject topicObjConsume =
        SecurableObjects.ofTopic(
            schemaObj, "events", Collections.singletonList(Privileges.ConsumeTopic.allow()));

    Role producerRole = testRole("producer_role", topicObjProduce);
    Role consumerRole = testRole("consumer_role", topicObjConsume);

    stateProvider.grantRoleToUser("alice", producerRole);
    stateProvider.grantRoleToUser("alice", consumerRole);
    plugin.onGrantedRolesToUser(Arrays.asList(producerRole, consumerRole), testUser("alice"));

    // Simulate PermissionManager removing producerRole from EntityStore before calling
    // onRevokedRolesFromUser
    stateProvider.revokeRoleFromUser("alice", "producer_role");
    plugin.onRevokedRolesFromUser(Collections.singletonList(producerRole), testUser("alice"));

    // WRITE and IDEMPOTENT_WRITE are removed, but shared DESCRIBE and READ from consumerRole
    // remain.
    Assertions.assertFalse(
        brokerAcls.contains(
            acl(
                ResourceType.TOPIC,
                "events",
                "User:alice",
                AclOperation.WRITE,
                AclPermissionType.ALLOW)));
    Assertions.assertTrue(
        brokerAcls.contains(
            acl(
                ResourceType.TOPIC,
                "events",
                "User:alice",
                AclOperation.DESCRIBE,
                AclPermissionType.ALLOW)));
    Assertions.assertTrue(
        brokerAcls.contains(
            acl(
                ResourceType.TOPIC,
                "events",
                "User:alice",
                AclOperation.READ,
                AclPermissionType.ALLOW)));

    // Simulate RoleManager calling onRoleDeleted BEFORE store deletes the relation
    plugin.onRoleDeleted(consumerRole);
    Assertions.assertTrue(
        brokerAcls.isEmpty(), "All ACLs should be gone after consumerRole deletion");
  }

  @Test
  public void testRoleUpdateAndGroupBindings() {
    KafkaAuthorizationProperties groupEnabledProps =
        new KafkaAuthorizationProperties(
            ImmutableMap.of(
                "bootstrap.servers",
                "localhost:9092",
                BaseAuthorization.CATALOG_ID,
                "1",
                KafkaAuthorizationProperties.GROUP_ACL_ENABLED,
                "true"));
    groupEnabledProps.validate();
    KafkaAuthorizationPlugin groupPlugin =
        new KafkaAuthorizationPlugin(METALAKE, groupEnabledProps, adminClient, stateProvider, null);

    SecurableObject catalogObj = SecurableObjects.ofCatalog(CATALOG, Collections.emptyList());
    SecurableObject schemaObj =
        SecurableObjects.ofSchema(catalogObj, "default", Collections.emptyList());
    SecurableObject initialTopic =
        SecurableObjects.ofTopic(
            schemaObj,
            "payments",
            Arrays.asList(Privileges.ProduceTopic.allow(), Privileges.ConsumeTopic.allow()));
    Role groupRole = testRole("eng_role", initialTopic);

    stateProvider.grantRoleToGroup("eng", groupRole);
    groupPlugin.onGrantedRolesToGroup(Collections.singletonList(groupRole), testGroup("eng"));
    Assertions.assertTrue(
        brokerAcls.contains(
            acl(
                ResourceType.TOPIC,
                "payments",
                "Group:eng",
                AclOperation.WRITE,
                AclPermissionType.ALLOW)));

    // Remove CONSUME_TOPIC from payments via UpdateSecurableObject (PermissionManager passes
    // pre-update role)
    SecurableObject updatedTopic =
        SecurableObjects.ofTopic(
            schemaObj, "payments", Collections.singletonList(Privileges.ProduceTopic.allow()));
    SecurableObject newTopic =
        SecurableObjects.ofTopic(
            schemaObj, "audit", Collections.singletonList(Privileges.ConsumeTopic.allow()));
    Role postUpdate = testRole("eng_role", updatedTopic, newTopic);
    stateProvider.grantRoleToGroup("eng", postUpdate);

    groupPlugin.onRoleUpdated(
        groupRole,
        RoleChange.updateSecurableObject("eng_role", initialTopic, updatedTopic),
        RoleChange.addSecurableObject("eng_role", newTopic));

    Assertions.assertTrue(
        brokerAcls.contains(
            acl(
                ResourceType.TOPIC,
                "payments",
                "Group:eng",
                AclOperation.WRITE,
                AclPermissionType.ALLOW)));
    Assertions.assertFalse(
        brokerAcls.contains(
            acl(
                ResourceType.TOPIC,
                "payments",
                "Group:eng",
                AclOperation.READ,
                AclPermissionType.ALLOW)));
    Assertions.assertTrue(
        brokerAcls.contains(
            acl(
                ResourceType.TOPIC,
                "payments",
                "Group:eng",
                AclOperation.DESCRIBE,
                AclPermissionType.ALLOW)));
    Assertions.assertTrue(
        brokerAcls.contains(
            acl(
                ResourceType.TOPIC,
                "audit",
                "Group:eng",
                AclOperation.READ,
                AclPermissionType.ALLOW)));

    // UserGroupManager invokes onGroupRemoved BEFORE deleting group relations from EntityStore
    groupPlugin.onGroupRemoved(testGroup("eng"));
    Assertions.assertTrue(brokerAcls.isEmpty());
  }

  @Test
  public void testOwnerTransferPreservesRoleAndCleansOwnerAcls() {
    MetadataObject topicObj =
        MetadataObjects.of(Arrays.asList(CATALOG, "default", "orders"), MetadataObject.Type.TOPIC);
    SecurableObject catalogObj = SecurableObjects.ofCatalog(CATALOG, Collections.emptyList());
    SecurableObject schemaObj =
        SecurableObjects.ofSchema(catalogObj, "default", Collections.emptyList());
    SecurableObject ordersConsume =
        SecurableObjects.ofTopic(
            schemaObj, "orders", Collections.singletonList(Privileges.ConsumeTopic.allow()));
    Role readerRole = testRole("orders_reader", ordersConsume);

    stateProvider.grantRoleToUser("alice", readerRole);
    plugin.onGrantedRolesToUser(Collections.singletonList(readerRole), testUser("alice"));

    Owner aliceOwner = testOwner(Owner.Type.USER, "alice");
    Owner bobOwner = testOwner(Owner.Type.USER, "bob");
    stateProvider.owners.put(topicObj, aliceOwner);
    plugin.onOwnerSet(topicObj, null, aliceOwner);

    Assertions.assertTrue(
        brokerAcls.contains(
            acl(
                ResourceType.TOPIC,
                "orders",
                "User:alice",
                AclOperation.ALTER,
                AclPermissionType.ALLOW)));

    // Transfer ownership from alice to bob
    stateProvider.owners.put(topicObj, bobOwner);
    plugin.onOwnerSet(topicObj, aliceOwner, bobOwner);

    // Alice loses owner-only ALTER, but retains role-derived READ and DESCRIBE
    Assertions.assertFalse(
        brokerAcls.contains(
            acl(
                ResourceType.TOPIC,
                "orders",
                "User:alice",
                AclOperation.ALTER,
                AclPermissionType.ALLOW)));
    Assertions.assertTrue(
        brokerAcls.contains(
            acl(
                ResourceType.TOPIC,
                "orders",
                "User:alice",
                AclOperation.READ,
                AclPermissionType.ALLOW)));
    Assertions.assertTrue(
        brokerAcls.contains(
            acl(
                ResourceType.TOPIC,
                "orders",
                "User:alice",
                AclOperation.DESCRIBE,
                AclPermissionType.ALLOW)));
    // Bob gains owner ACLs
    Assertions.assertTrue(
        brokerAcls.contains(
            acl(
                ResourceType.TOPIC,
                "orders",
                "User:bob",
                AclOperation.ALTER,
                AclPermissionType.ALLOW)));
    Assertions.assertTrue(
        brokerAcls.contains(
            acl(
                ResourceType.TOPIC,
                "orders",
                "User:bob",
                AclOperation.DELETE,
                AclPermissionType.ALLOW)));
  }

  @Test
  public void testMetadataRenameAndDrop() {
    MetadataObject topicOrders =
        MetadataObjects.of(Arrays.asList(CATALOG, "default", "orders"), MetadataObject.Type.TOPIC);
    MetadataObject topicOrdersV2 =
        MetadataObjects.of(
            Arrays.asList(CATALOG, "default", "orders_v2"), MetadataObject.Type.TOPIC);
    MetadataObject otherCatalogTopic =
        MetadataObjects.of(
            Arrays.asList("other_cat", "default", "orders"), MetadataObject.Type.TOPIC);

    plugin.onOwnerSet(topicOrders, null, testOwner(Owner.Type.USER, "alice"));
    Assertions.assertTrue(brokerAcls.stream().anyMatch(b -> b.pattern().name().equals("orders")));

    // Ignoring topic in another catalog
    plugin.onMetadataUpdated(
        MetadataObjectChange.remove(otherCatalogTopic, Collections.emptyList()));
    Assertions.assertTrue(brokerAcls.stream().anyMatch(b -> b.pattern().name().equals("orders")));

    // Rename moves topic ACLs from orders -> orders_v2
    plugin.onMetadataUpdated(
        MetadataObjectChange.rename(topicOrders, topicOrdersV2, Collections.emptyList()));
    Assertions.assertFalse(brokerAcls.stream().anyMatch(b -> b.pattern().name().equals("orders")));
    Assertions.assertTrue(
        brokerAcls.stream().anyMatch(b -> b.pattern().name().equals("orders_v2")));

    // Dropping a Gravitino Kafka catalog must NOT wipe broker topic ACLs
    plugin.onMetadataUpdated(
        MetadataObjectChange.remove(
            MetadataObjects.of(Collections.singletonList(CATALOG), MetadataObject.Type.CATALOG),
            Collections.emptyList()));
    Assertions.assertTrue(
        brokerAcls.stream().anyMatch(b -> b.pattern().name().equals("orders_v2")));

    // Dropping the topic deletes orders_v2 ACLs
    plugin.onMetadataUpdated(MetadataObjectChange.remove(topicOrdersV2, Collections.emptyList()));
    Assertions.assertFalse(
        brokerAcls.stream().anyMatch(b -> b.pattern().name().equals("orders_v2")));
  }

  @Test
  public void testDropTopicCleansOrphanedAuxiliaryAclsAndPreservesSharedGrants() {
    MetadataObject topicOrders =
        MetadataObjects.of(Arrays.asList(CATALOG, "default", "orders"), MetadataObject.Type.TOPIC);
    MetadataObject topicPayments =
        MetadataObjects.of(
            Arrays.asList(CATALOG, "default", "payments"), MetadataObject.Type.TOPIC);
    Owner aliceOwner = testOwner(Owner.Type.USER, "alice");

    stateProvider.owners.put(topicOrders, aliceOwner);
    stateProvider.owners.put(topicPayments, aliceOwner);
    plugin.onOwnerSet(topicOrders, null, aliceOwner);
    plugin.onOwnerSet(topicPayments, null, aliceOwner);

    AclBinding groupReadAcl =
        acl(ResourceType.GROUP, "*", "User:alice", AclOperation.READ, AclPermissionType.ALLOW);
    AclBinding clusterIdempotentAcl =
        acl(
            ResourceType.CLUSTER,
            Resource.CLUSTER_NAME,
            "User:alice",
            AclOperation.IDEMPOTENT_WRITE,
            AclPermissionType.ALLOW);
    Assertions.assertTrue(brokerAcls.contains(groupReadAcl));
    Assertions.assertTrue(brokerAcls.contains(clusterIdempotentAcl));

    // Dropping orders while alice still owns payments must preserve shared auxiliary ACLs.
    stateProvider.owners.remove(topicOrders);
    plugin.onMetadataUpdated(MetadataObjectChange.remove(topicOrders, Collections.emptyList()));
    Assertions.assertFalse(brokerAcls.stream().anyMatch(b -> b.pattern().name().equals("orders")));
    Assertions.assertTrue(brokerAcls.contains(groupReadAcl));
    Assertions.assertTrue(brokerAcls.contains(clusterIdempotentAcl));

    // Dropping the last owned topic cleans up orphaned GROUP:* READ and CLUSTER:IDEMPOTENT_WRITE.
    stateProvider.owners.remove(topicPayments);
    plugin.onMetadataUpdated(MetadataObjectChange.remove(topicPayments, Collections.emptyList()));
    Assertions.assertTrue(
        brokerAcls.isEmpty(), "Expected no orphaned auxiliary ACLs: " + brokerAcls);
  }

  @Test
  public void testUserRemovedDeletesOnlyCatalogManagedBindingsAndPreservesExternalAcls() {
    SecurableObject catalogObj = SecurableObjects.ofCatalog(CATALOG, Collections.emptyList());
    SecurableObject schemaObj =
        SecurableObjects.ofSchema(catalogObj, "default", Collections.emptyList());
    SecurableObject ordersTopic =
        SecurableObjects.ofTopic(
            schemaObj,
            "orders",
            Arrays.asList(Privileges.ProduceTopic.allow(), Privileges.ConsumeTopic.allow()));
    Role aliceRole = testRole("alice_rw", ordersTopic);

    stateProvider.grantRoleToUser("alice", aliceRole);
    plugin.onGrantedRolesToUser(Collections.singletonList(aliceRole), testUser("alice"));

    AclBinding externalTxnAcl =
        acl(
            ResourceType.TRANSACTIONAL_ID,
            "txn-1",
            "User:alice",
            AclOperation.WRITE,
            AclPermissionType.ALLOW);
    AclBinding externalTopicAcl =
        acl(
            ResourceType.TOPIC,
            "external_unmanaged_topic",
            "User:alice",
            AclOperation.READ,
            AclPermissionType.ALLOW);
    AclBinding externalClusterActionAcl =
        acl(
            ResourceType.CLUSTER,
            Resource.CLUSTER_NAME,
            "User:alice",
            AclOperation.CLUSTER_ACTION,
            AclPermissionType.ALLOW);
    AclBinding bobTopicAcl =
        acl(ResourceType.TOPIC, "orders", "User:bob", AclOperation.READ, AclPermissionType.ALLOW);
    brokerAcls.add(externalTxnAcl);
    brokerAcls.add(externalTopicAcl);
    brokerAcls.add(externalClusterActionAcl);
    brokerAcls.add(bobTopicAcl);

    plugin.onUserRemoved(testUser("alice"));

    Assertions.assertEquals(
        Set.of(externalTxnAcl, externalTopicAcl, externalClusterActionAcl, bobTopicAcl),
        brokerAcls);
  }

  @Test
  public void testOnOwnerSetSameOwnerIsIdempotent() {
    MetadataObject topicOrders =
        MetadataObjects.of(Arrays.asList(CATALOG, "default", "orders"), MetadataObject.Type.TOPIC);
    Owner alicePre = testOwner(Owner.Type.USER, "alice");
    Owner aliceNew = testOwner(Owner.Type.USER, "alice");

    stateProvider.owners.put(topicOrders, aliceNew);
    plugin.onOwnerSet(topicOrders, alicePre, aliceNew);

    verify(adminClient, never()).deleteAcls(any());
    verify(adminClient).createAcls(any());
    Assertions.assertTrue(
        brokerAcls.contains(
            acl(
                ResourceType.TOPIC,
                "orders",
                "User:alice",
                AclOperation.WRITE,
                AclPermissionType.ALLOW)));
  }

  @Test
  public void testRoleUpdatedCancelsOutAddThenRemoveInSingleBatch() {
    SecurableObject catalogObj = SecurableObjects.ofCatalog(CATALOG, Collections.emptyList());
    SecurableObject schemaObj =
        SecurableObjects.ofSchema(catalogObj, "default", Collections.emptyList());
    SecurableObject ordersTopic =
        SecurableObjects.ofTopic(
            schemaObj, "orders", Collections.singletonList(Privileges.ProduceTopic.allow()));
    Role emptyRole = testRole("r_noop");
    stateProvider.grantRoleToUser("alice", emptyRole);

    plugin.onRoleUpdated(
        emptyRole,
        RoleChange.addSecurableObject("r_noop", ordersTopic),
        RoleChange.removeSecurableObject("r_noop", ordersTopic));

    verify(adminClient, never()).createAcls(any());
    Assertions.assertTrue(brokerAcls.isEmpty());
  }

  @Test
  public void testCreateTopicDoesNotEmitClusterCreateAndRespectsTopicDeny() {
    SecurableObject catalogObj = SecurableObjects.ofCatalog(CATALOG, Collections.emptyList());
    SecurableObject schemaAllowCreate =
        SecurableObjects.ofSchema(
            catalogObj, "default", Collections.singletonList(Privileges.CreateTopic.allow()));
    SecurableObject topicDenyCreate =
        SecurableObjects.ofTopic(
            schemaAllowCreate,
            "restricted_orders",
            Collections.singletonList(Privileges.CreateTopic.deny()));
    Role creatorRole = testRole("creator", schemaAllowCreate, topicDenyCreate);

    stateProvider.grantRoleToUser("alice", creatorRole);
    plugin.onGrantedRolesToUser(Collections.singletonList(creatorRole), testUser("alice"));

    AclBinding clusterCreateAcl =
        acl(
            ResourceType.CLUSTER,
            Resource.CLUSTER_NAME,
            "User:alice",
            AclOperation.CREATE,
            AclPermissionType.ALLOW);
    Assertions.assertFalse(
        brokerAcls.contains(clusterCreateAcl),
        "CLUSTER:CREATE must not be emitted because it bypasses topic-level CREATE DENY");
    Assertions.assertTrue(
        brokerAcls.contains(
            acl(
                ResourceType.TOPIC,
                "*",
                "User:alice",
                AclOperation.CREATE,
                AclPermissionType.ALLOW)));
    Assertions.assertTrue(
        brokerAcls.contains(
            acl(
                ResourceType.TOPIC,
                "restricted_orders",
                "User:alice",
                AclOperation.CREATE,
                AclPermissionType.DENY)));

    // Parent catalog ownership also uses TOPIC:* CREATE rather than CLUSTER:CREATE
    MetadataObject catalogMetadataObj =
        MetadataObjects.of(Collections.singletonList(CATALOG), MetadataObject.Type.CATALOG);
    Owner bobOwner = testOwner(Owner.Type.USER, "bob");
    stateProvider.owners.put(catalogMetadataObj, bobOwner);
    plugin.onOwnerSet(catalogMetadataObj, null, bobOwner);
    Assertions.assertFalse(
        brokerAcls.contains(
            acl(
                ResourceType.CLUSTER,
                Resource.CLUSTER_NAME,
                "User:bob",
                AclOperation.CREATE,
                AclPermissionType.ALLOW)));
    Assertions.assertTrue(
        brokerAcls.contains(
            acl(
                ResourceType.TOPIC,
                "*",
                "User:bob",
                AclOperation.CREATE,
                AclPermissionType.ALLOW)));
  }

  @Test
  public void testDropTopicPreservesAuxiliaryAclsForSamePrefixGroup() {
    KafkaAuthorizationProperties samePrefixProps =
        new KafkaAuthorizationProperties(
            ImmutableMap.of(
                "bootstrap.servers",
                "localhost:9092",
                BaseAuthorization.CATALOG_ID,
                "1",
                KafkaAuthorizationProperties.USER_PRINCIPAL_TYPE,
                "User",
                KafkaAuthorizationProperties.GROUP_PRINCIPAL_TYPE,
                "User",
                KafkaAuthorizationProperties.GROUP_ACL_ENABLED,
                "true"));
    samePrefixProps.validate();
    KafkaAuthorizationPlugin sharedPrefixPlugin =
        new KafkaAuthorizationPlugin(METALAKE, samePrefixProps, adminClient, stateProvider, null);

    MetadataObject topicOrders =
        MetadataObjects.of(Arrays.asList(CATALOG, "default", "orders"), MetadataObject.Type.TOPIC);
    Owner aliceUserOwner = testOwner(Owner.Type.USER, "shared");
    stateProvider.owners.put(topicOrders, aliceUserOwner);
    sharedPrefixPlugin.onOwnerSet(topicOrders, null, aliceUserOwner);

    SecurableObject catalogObj = SecurableObjects.ofCatalog(CATALOG, Collections.emptyList());
    SecurableObject schemaObj =
        SecurableObjects.ofSchema(catalogObj, "default", Collections.emptyList());
    SecurableObject paymentsTopic =
        SecurableObjects.ofTopic(
            schemaObj, "payments", Collections.singletonList(Privileges.ConsumeTopic.allow()));
    Role groupRole = testRole("shared_group_consume", paymentsTopic);
    stateProvider.grantRoleToGroup("shared", groupRole);
    sharedPrefixPlugin.onGrantedRolesToGroup(
        Collections.singletonList(groupRole), testGroup("shared"));

    AclBinding groupReadAcl =
        acl(ResourceType.GROUP, "*", "User:shared", AclOperation.READ, AclPermissionType.ALLOW);
    Assertions.assertTrue(brokerAcls.contains(groupReadAcl));

    stateProvider.owners.remove(topicOrders);
    sharedPrefixPlugin.onMetadataUpdated(
        MetadataObjectChange.remove(topicOrders, Collections.emptyList()));

    Assertions.assertFalse(brokerAcls.stream().anyMatch(b -> b.pattern().name().equals("orders")));
    Assertions.assertTrue(brokerAcls.contains(groupReadAcl));
  }

  @Test
  public void testGroupOperationsRejectedByDefaultUnlessEnabled() {
    SecurableObject catalogUseOnly =
        SecurableObjects.ofCatalog(
            CATALOG, Collections.singletonList(Privileges.UseCatalog.allow()));
    Role metadataOnlyRole = testRole("use_catalog_only", catalogUseOnly);

    // Non-Kafka metadata privileges (such as USE_CATALOG) produce no Kafka ACL bindings and must
    // not block group operations in multi-catalog metalakes.
    Assertions.assertTrue(
        plugin.onGrantedRolesToGroup(
            Collections.singletonList(metadataOnlyRole), testGroup("analysts")));
    Assertions.assertTrue(plugin.onGroupRemoved(testGroup("analysts")));

    SecurableObject schemaObj =
        SecurableObjects.ofSchema(catalogUseOnly, "default", Collections.emptyList());
    SecurableObject kafkaTopicObj =
        SecurableObjects.ofTopic(
            schemaObj, "orders", Collections.singletonList(Privileges.ConsumeTopic.allow()));
    Role kafkaGroupRole = testRole("kafka_group_reader", kafkaTopicObj);

    Assertions.assertThrows(
        AuthorizationPluginException.class,
        () ->
            plugin.onGrantedRolesToGroup(
                Collections.singletonList(kafkaGroupRole), testGroup("analysts")));
    Assertions.assertThrows(
        AuthorizationPluginException.class,
        () ->
            plugin.onRevokedRolesFromGroup(
                Collections.singletonList(kafkaGroupRole), testGroup("analysts")));

    stateProvider.grantRoleToGroup("analysts", metadataOnlyRole);
    Assertions.assertThrows(
        AuthorizationPluginException.class,
        () ->
            plugin.onRoleUpdated(
                metadataOnlyRole,
                RoleChange.addSecurableObject("use_catalog_only", kafkaTopicObj)));

    MetadataObject ordersMetadataObj =
        MetadataObjects.of(Arrays.asList(CATALOG, "default", "orders"), MetadataObject.Type.TOPIC);
    Assertions.assertThrows(
        AuthorizationPluginException.class,
        () -> plugin.onOwnerSet(ordersMetadataObj, null, testOwner(Owner.Type.GROUP, "analysts")));
  }

  @Test
  public void testOverridePrivilegesMultiCallbackReconcilesAgainstPersistedRole() {
    SecurableObject catalogObj = SecurableObjects.ofCatalog(CATALOG, Collections.emptyList());
    SecurableObject schemaObj =
        SecurableObjects.ofSchema(catalogObj, "default", Collections.emptyList());
    SecurableObject topicA =
        SecurableObjects.ofTopic(
            schemaObj, "topic_a", Collections.singletonList(Privileges.ConsumeTopic.allow()));
    SecurableObject topicB =
        SecurableObjects.ofTopic(
            schemaObj, "topic_b", Collections.singletonList(Privileges.ConsumeTopic.allow()));
    SecurableObject topicC =
        SecurableObjects.ofTopic(
            schemaObj, "topic_c", Collections.singletonList(Privileges.ProduceTopic.allow()));

    Role preOverrideRole = testRole("override_role", topicA, topicB);
    stateProvider.grantRoleToUser("alice", preOverrideRole);
    plugin.onGrantedRolesToUser(Collections.singletonList(preOverrideRole), testUser("alice"));

    // PermissionManager commits the full post-override role to EntityStore FIRST, then invokes
    // per-object onRoleUpdated callbacks passing the pre-override role snapshot.
    Role postOverrideRole = testRole("override_role", topicC);
    stateProvider.grantRoleToUser("alice", postOverrideRole);

    plugin.onRoleUpdated(
        preOverrideRole, RoleChange.removeSecurableObject("override_role", topicA));
    plugin.onRoleUpdated(
        preOverrideRole, RoleChange.removeSecurableObject("override_role", topicB));
    plugin.onRoleUpdated(preOverrideRole, RoleChange.addSecurableObject("override_role", topicC));

    Assertions.assertFalse(
        brokerAcls.stream().anyMatch(b -> b.pattern().name().equals("topic_a")),
        "topic_a ACLs must be removed: " + brokerAcls);
    Assertions.assertFalse(
        brokerAcls.stream().anyMatch(b -> b.pattern().name().equals("topic_b")),
        "topic_b ACLs must be removed: " + brokerAcls);
    Assertions.assertFalse(
        brokerAcls.contains(
            acl(ResourceType.GROUP, "*", "User:alice", AclOperation.READ, AclPermissionType.ALLOW)),
        "Orphaned GROUP:* READ must be removed once neither topic_a nor topic_b remains");
    Assertions.assertTrue(
        brokerAcls.contains(
            acl(
                ResourceType.TOPIC,
                "topic_c",
                "User:alice",
                AclOperation.WRITE,
                AclPermissionType.ALLOW)));

    // Now override topic_c -> CONSUME_TOPIC on topic_d across two callbacks (remove topic_c, add
    // topic_d) and verify shared/new bindings match the persisted post-update role.
    SecurableObject topicCConsume =
        SecurableObjects.ofTopic(
            schemaObj, "topic_c", Collections.singletonList(Privileges.ConsumeTopic.allow()));
    SecurableObject topicDConsume =
        SecurableObjects.ofTopic(
            schemaObj, "topic_d", Collections.singletonList(Privileges.ConsumeTopic.allow()));
    Role preMoveRole = testRole("override_role", topicCConsume);
    stateProvider.grantRoleToUser("alice", preMoveRole);
    plugin.onRoleUpdated(
        postOverrideRole, RoleChange.updateSecurableObject("override_role", topicC, topicCConsume));

    Role postMoveRole = testRole("override_role", topicDConsume);
    stateProvider.grantRoleToUser("alice", postMoveRole);
    plugin.onRoleUpdated(
        preMoveRole, RoleChange.removeSecurableObject("override_role", topicCConsume));
    plugin.onRoleUpdated(
        preMoveRole, RoleChange.addSecurableObject("override_role", topicDConsume));

    Assertions.assertTrue(
        brokerAcls.contains(
            acl(ResourceType.GROUP, "*", "User:alice", AclOperation.READ, AclPermissionType.ALLOW)),
        "GROUP:* READ must be preserved when moving CONSUME_TOPIC from topic_c to topic_d");
  }

  @Test
  public void testSamePrefixUserRevokePreservesGroupGrantWhileRoleDeleteRemovesBoth() {
    KafkaAuthorizationProperties samePrefixProps =
        new KafkaAuthorizationProperties(
            ImmutableMap.of(
                "bootstrap.servers",
                "localhost:9092",
                BaseAuthorization.CATALOG_ID,
                "1",
                KafkaAuthorizationProperties.USER_PRINCIPAL_TYPE,
                "User",
                KafkaAuthorizationProperties.GROUP_PRINCIPAL_TYPE,
                "User",
                KafkaAuthorizationProperties.GROUP_ACL_ENABLED,
                "true"));
    samePrefixProps.validate();
    KafkaAuthorizationPlugin sharedPrefixPlugin =
        new KafkaAuthorizationPlugin(METALAKE, samePrefixProps, adminClient, stateProvider, null);

    SecurableObject catalogObj = SecurableObjects.ofCatalog(CATALOG, Collections.emptyList());
    SecurableObject schemaObj =
        SecurableObjects.ofSchema(catalogObj, "default", Collections.emptyList());
    SecurableObject ordersProduce =
        SecurableObjects.ofTopic(
            schemaObj, "orders", Collections.singletonList(Privileges.ProduceTopic.allow()));
    Role sharedRole = testRole("shared_role", ordersProduce);

    stateProvider.grantRoleToUser("shared", sharedRole);
    stateProvider.grantRoleToGroup("shared", sharedRole);
    sharedPrefixPlugin.onGrantedRolesToUser(
        Collections.singletonList(sharedRole), testUser("shared"));
    sharedPrefixPlugin.onGrantedRolesToGroup(
        Collections.singletonList(sharedRole), testGroup("shared"));

    AclBinding writeBinding =
        acl(
            ResourceType.TOPIC,
            "orders",
            "User:shared",
            AclOperation.WRITE,
            AclPermissionType.ALLOW);
    Assertions.assertTrue(brokerAcls.contains(writeBinding));

    // Revoking sharedRole from user "shared" must preserve User:shared ACLs because group "shared"
    // still holds sharedRole.
    stateProvider.revokeRoleFromUser("shared", "shared_role");
    sharedPrefixPlugin.onRevokedRolesFromUser(
        Collections.singletonList(sharedRole), testUser("shared"));
    Assertions.assertTrue(
        brokerAcls.contains(writeBinding),
        "Peer group grant must preserve shared Kafka principal ACLs on user revoke");

    // Re-grant to user, then delete sharedRole completely -> both user and group lose sharedRole,
    // so User:shared ACLs must be deleted.
    stateProvider.grantRoleToUser("shared", sharedRole);
    sharedPrefixPlugin.onRoleDeleted(sharedRole);
    Assertions.assertTrue(brokerAcls.isEmpty(), "Expected all ACLs removed: " + brokerAcls);
  }

  @Test
  public void testConcurrentGrantAndRevokeSerializesSharedBindings() throws Exception {
    SecurableObject catalogObj = SecurableObjects.ofCatalog(CATALOG, Collections.emptyList());
    SecurableObject schemaObj =
        SecurableObjects.ofSchema(catalogObj, "default", Collections.emptyList());
    SecurableObject ordersConsume =
        SecurableObjects.ofTopic(
            schemaObj, "orders", Collections.singletonList(Privileges.ConsumeTopic.allow()));
    SecurableObject paymentsConsume =
        SecurableObjects.ofTopic(
            schemaObj, "payments", Collections.singletonList(Privileges.ConsumeTopic.allow()));
    Role ordersRole = testRole("orders_consume", ordersConsume);
    Role paymentsRole = testRole("payments_consume", paymentsConsume);

    stateProvider.grantRoleToUser("alice", ordersRole);
    plugin.onGrantedRolesToUser(Collections.singletonList(ordersRole), testUser("alice"));

    // Simulate PermissionManager committing bothEntityStore updates concurrently before callbacks
    stateProvider.revokeRoleFromUser("alice", "orders_consume");
    stateProvider.grantRoleToUser("alice", paymentsRole);

    CountDownLatch startGate = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<Boolean> revokeFuture =
          executor.submit(
              () -> {
                startGate.await(5, TimeUnit.SECONDS);
                return plugin.onRevokedRolesFromUser(
                    Collections.singletonList(ordersRole), testUser("alice"));
              });
      Future<Boolean> grantFuture =
          executor.submit(
              () -> {
                startGate.await(5, TimeUnit.SECONDS);
                return plugin.onGrantedRolesToUser(
                    Collections.singletonList(paymentsRole), testUser("alice"));
              });
      startGate.countDown();
      Assertions.assertTrue(revokeFuture.get(10, TimeUnit.SECONDS));
      Assertions.assertTrue(grantFuture.get(10, TimeUnit.SECONDS));
    } finally {
      executor.shutdownNow();
    }

    Assertions.assertTrue(
        brokerAcls.contains(
            acl(ResourceType.GROUP, "*", "User:alice", AclOperation.READ, AclPermissionType.ALLOW)),
        "Shared GROUP:* READ must survive concurrent grant + revoke");
    Assertions.assertTrue(
        brokerAcls.contains(
            acl(
                ResourceType.TOPIC,
                "payments",
                "User:alice",
                AclOperation.READ,
                AclPermissionType.ALLOW)));
    Assertions.assertFalse(
        brokerAcls.contains(
            acl(
                ResourceType.TOPIC,
                "orders",
                "User:alice",
                AclOperation.READ,
                AclPermissionType.ALLOW)));
  }

  @Test
  public void testWildcardIdentityRejectedOnGrantAndIgnoredOnRemove() {
    SecurableObject catalogObj = SecurableObjects.ofCatalog(CATALOG, Collections.emptyList());
    SecurableObject schemaObj =
        SecurableObjects.ofSchema(catalogObj, "default", Collections.emptyList());
    SecurableObject ordersTopic =
        SecurableObjects.ofTopic(
            schemaObj, "orders", Collections.singletonList(Privileges.ProduceTopic.allow()));
    Role role = testRole("wildcard_test", ordersTopic);

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> plugin.onGrantedRolesToUser(Collections.singletonList(role), testUser("*")));

    MetadataObject ordersMetadataObj =
        MetadataObjects.of(Arrays.asList(CATALOG, "default", "orders"), MetadataObject.Type.TOPIC);
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> plugin.onOwnerSet(ordersMetadataObj, null, testOwner(Owner.Type.USER, "*")));

    AclBinding existingWildcardAcl =
        acl(ResourceType.TOPIC, "orders", "User:*", AclOperation.READ, AclPermissionType.ALLOW);
    brokerAcls.add(existingWildcardAcl);

    Assertions.assertTrue(plugin.onUserRemoved(testUser("*")));
    Assertions.assertTrue(plugin.onGroupRemoved(testGroup("*")));
    Assertions.assertTrue(brokerAcls.contains(existingWildcardAcl));
  }

  @Test
  public void testEntityStoreStateProviderUsesBatchOwnerLookup() throws Exception {
    EntityStore store = mock(EntityStore.class);
    SupportsRelationOperations relationOps = mock(SupportsRelationOperations.class);
    when(store.relationOperations()).thenReturn(relationOps);

    AuditInfo audit =
        AuditInfo.builder().withCreator("admin").withCreateTime(Instant.now()).build();
    SchemaEntity defaultSchema =
        SchemaEntity.builder()
            .withId(10L)
            .withName("default")
            .withNamespace(NamespaceUtil.ofSchema(METALAKE, CATALOG))
            .withAuditInfo(audit)
            .build();
    TopicEntity t1 =
        TopicEntity.builder()
            .withId(101L)
            .withName("orders")
            .withNamespace(NamespaceUtil.ofTopic(METALAKE, CATALOG, "default"))
            .withAuditInfo(audit)
            .build();
    TopicEntity t2 =
        TopicEntity.builder()
            .withId(102L)
            .withName("payments")
            .withNamespace(NamespaceUtil.ofTopic(METALAKE, CATALOG, "default"))
            .withAuditInfo(audit)
            .build();
    UserEntity alice =
        UserEntity.builder()
            .withId(201L)
            .withName("alice")
            .withNamespace(NamespaceUtil.ofUser(METALAKE))
            .withAuditInfo(audit)
            .build();
    UserEntity bob =
        UserEntity.builder()
            .withId(202L)
            .withName("bob")
            .withNamespace(NamespaceUtil.ofUser(METALAKE))
            .withAuditInfo(audit)
            .build();

    when(relationOps.listEntitiesByRelation(
            eq(SupportsRelationOperations.Type.OWNER_REL), any(), any()))
        .thenReturn(Collections.emptyList());
    when(store.list(
            eq(NamespaceUtil.ofSchema(METALAKE, CATALOG)),
            eq(SchemaEntity.class),
            eq(Entity.EntityType.SCHEMA)))
        .thenReturn(Collections.singletonList(defaultSchema));
    when(store.list(
            eq(NamespaceUtil.ofTopic(METALAKE, CATALOG, "default")),
            eq(TopicEntity.class),
            eq(Entity.EntityType.TOPIC)))
        .thenReturn(Arrays.asList(t1, t2));

    NameIdentifier t1Ident = NameIdentifierUtil.ofTopic(METALAKE, CATALOG, "default", "orders");
    NameIdentifier t2Ident = NameIdentifierUtil.ofTopic(METALAKE, CATALOG, "default", "payments");
    when(relationOps.batchListEntitiesByRelation(
            eq(SupportsRelationOperations.Type.OWNER_REL),
            eq(Arrays.asList(t1Ident, t2Ident)),
            eq(Entity.EntityType.TOPIC)))
        .thenReturn(
            Arrays.asList(
                new RelationalEntity<>(
                    SupportsRelationOperations.Type.OWNER_REL,
                    t1Ident,
                    Entity.EntityType.TOPIC,
                    alice),
                new RelationalEntity<>(
                    SupportsRelationOperations.Type.OWNER_REL,
                    t2Ident,
                    Entity.EntityType.TOPIC,
                    bob)));

    GravitinoEnv env = mock(GravitinoEnv.class);
    when(env.entityStore()).thenReturn(store);
    try (MockedStatic<GravitinoEnv> envStatic = mockStatic(GravitinoEnv.class)) {
      envStatic.when(GravitinoEnv::getInstance).thenReturn(env);
      EntityStoreAuthorizationStateProvider provider = new EntityStoreAuthorizationStateProvider();
      List<MetadataObject> owned =
          provider.listOwnedObjectsForPrincipal(METALAKE, CATALOG, Owner.Type.USER, "alice");
      Assertions.assertEquals(
          Collections.singletonList(
              MetadataObjects.of(
                  Arrays.asList(CATALOG, "default", "orders"), MetadataObject.Type.TOPIC)),
          owned);
    }

    verify(relationOps)
        .batchListEntitiesByRelation(
            eq(SupportsRelationOperations.Type.OWNER_REL),
            eq(Arrays.asList(t1Ident, t2Ident)),
            eq(Entity.EntityType.TOPIC));
    verify(relationOps, never())
        .listEntitiesByRelation(
            eq(SupportsRelationOperations.Type.OWNER_REL), any(), eq(Entity.EntityType.TOPIC));
  }

  private static AclBinding acl(
      ResourceType type,
      String name,
      String principal,
      AclOperation operation,
      AclPermissionType permType) {
    return new AclBinding(
        new ResourcePattern(type, name, PatternType.LITERAL),
        new AccessControlEntry(principal, "*", operation, permType));
  }

  private static AdminClient mockAdminClient(Set<AclBinding> store) {
    AdminClient client = mock(AdminClient.class);

    when(client.createAcls(any()))
        .thenAnswer(
            inv -> {
              Collection<AclBinding> added = inv.getArgument(0);
              store.addAll(added);
              CreateAclsResult result = mock(CreateAclsResult.class);
              when(result.all()).thenReturn(KafkaFuture.completedFuture(null));
              return result;
            });

    when(client.deleteAcls(any()))
        .thenAnswer(
            inv -> {
              Collection<AclBindingFilter> filters = inv.getArgument(0);
              List<AclBinding> removed = new ArrayList<>();
              store.removeIf(
                  binding -> {
                    for (AclBindingFilter filter : filters) {
                      if (filter.matches(binding)) {
                        removed.add(binding);
                        return true;
                      }
                    }
                    return false;
                  });
              DeleteAclsResult result = mock(DeleteAclsResult.class);
              when(result.all()).thenReturn(KafkaFuture.completedFuture(removed));
              return result;
            });

    when(client.describeAcls(any()))
        .thenAnswer(
            inv -> {
              AclBindingFilter filter = inv.getArgument(0);
              List<AclBinding> matched = new ArrayList<>();
              for (AclBinding binding : store) {
                if (filter.matches(binding)) {
                  matched.add(binding);
                }
              }
              DescribeAclsResult result = mock(DescribeAclsResult.class);
              when(result.values()).thenReturn(KafkaFuture.completedFuture(matched));
              return result;
            });

    return client;
  }

  private static Role testRole(String name, SecurableObject... objects) {
    List<SecurableObject> list = Arrays.asList(objects);
    return new Role() {
      @Override
      public String name() {
        return name;
      }

      @Override
      public Map<String, String> properties() {
        return Collections.emptyMap();
      }

      @Override
      public List<SecurableObject> securableObjects() {
        return list;
      }

      @Override
      public Audit auditInfo() {
        return null;
      }
    };
  }

  private static User testUser(String name) {
    return new User() {
      @Override
      public Long id() {
        return 1L;
      }

      @Override
      public String name() {
        return name;
      }

      @Override
      public List<String> roles() {
        return Collections.emptyList();
      }

      @Override
      public Audit auditInfo() {
        return null;
      }
    };
  }

  private static Group testGroup(String name) {
    return new Group() {
      @Override
      public Long id() {
        return 1L;
      }

      @Override
      public String name() {
        return name;
      }

      @Override
      public List<String> roles() {
        return Collections.emptyList();
      }

      @Override
      public Audit auditInfo() {
        return null;
      }
    };
  }

  private static Owner testOwner(Owner.Type type, String name) {
    return new Owner() {
      @Override
      public String name() {
        return name;
      }

      @Override
      public Type type() {
        return type;
      }
    };
  }

  private static final class InMemoryStateProvider implements KafkaAuthorizationStateProvider {

    final Map<Long, String> catalogNames = new ConcurrentHashMap<>();
    final Map<String, Map<String, Role>> userRoles = new ConcurrentHashMap<>();
    final Map<String, Map<String, Role>> groupRoles = new ConcurrentHashMap<>();
    final Map<MetadataObject, Owner> owners = new ConcurrentHashMap<>();

    void grantRoleToUser(String user, Role role) {
      userRoles.computeIfAbsent(user, k -> new ConcurrentHashMap<>()).put(role.name(), role);
    }

    void revokeRoleFromUser(String user, String roleName) {
      userRoles.getOrDefault(user, Collections.emptyMap()).remove(roleName);
    }

    void grantRoleToGroup(String group, Role role) {
      groupRoles.computeIfAbsent(group, k -> new ConcurrentHashMap<>()).put(role.name(), role);
    }

    @Override
    public Optional<String> resolveCatalogName(String metalake, @Nullable Long catalogId) {
      return Optional.ofNullable(catalogNames.get(catalogId));
    }

    @Override
    public List<String> listUserNamesByRole(String metalake, String roleName) {
      Set<String> users = new LinkedHashSet<>();
      userRoles.forEach(
          (user, roles) -> {
            if (roles.containsKey(roleName)) {
              users.add(user);
            }
          });
      return new ArrayList<>(users);
    }

    @Override
    public List<String> listGroupNamesByRole(String metalake, String roleName) {
      Set<String> groups = new LinkedHashSet<>();
      groupRoles.forEach(
          (group, roles) -> {
            if (roles.containsKey(roleName)) {
              groups.add(group);
            }
          });
      return new ArrayList<>(groups);
    }

    @Override
    public List<Role> listRolesForUser(String metalake, String username) {
      return new ArrayList<>(userRoles.getOrDefault(username, Collections.emptyMap()).values());
    }

    @Override
    public List<Role> listRolesForGroup(String metalake, String groupName) {
      return new ArrayList<>(groupRoles.getOrDefault(groupName, Collections.emptyMap()).values());
    }

    @Override
    public List<MetadataObject> listOwnedObjectsForPrincipal(
        String metalake, @Nullable String catalogName, Owner.Type ownerType, String ownerName) {
      List<MetadataObject> result = new ArrayList<>();
      owners.forEach(
          (obj, owner) -> {
            if (owner != null && owner.type() == ownerType && ownerName.equals(owner.name())) {
              result.add(obj);
            }
          });
      return result;
    }
  }
}
