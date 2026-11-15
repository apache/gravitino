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

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.Privilege;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.Resource;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Translates Apache Gravitino {@link MetadataObject} privileges and ownership into native Apache
 * Kafka {@link AclBinding} entries.
 */
public final class KafkaPrivilegeMapper {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaPrivilegeMapper.class);

  private static final ResourcePattern WILDCARD_TOPIC_RESOURCE =
      new ResourcePattern(ResourceType.TOPIC, "*", PatternType.LITERAL);

  private static final ResourcePattern CLUSTER_RESOURCE =
      new ResourcePattern(ResourceType.CLUSTER, Resource.CLUSTER_NAME, PatternType.LITERAL);

  private static final Set<AclOperation> OWNER_TOPIC_OPERATIONS;

  static {
    Set<AclOperation> ops = new LinkedHashSet<>();
    ops.add(AclOperation.READ);
    ops.add(AclOperation.WRITE);
    ops.add(AclOperation.CREATE);
    ops.add(AclOperation.DELETE);
    ops.add(AclOperation.ALTER);
    ops.add(AclOperation.DESCRIBE);
    ops.add(AclOperation.ALTER_CONFIGS);
    ops.add(AclOperation.DESCRIBE_CONFIGS);
    OWNER_TOPIC_OPERATIONS = Collections.unmodifiableSet(ops);
  }

  private KafkaPrivilegeMapper() {}

  /**
   * Maps a Gravitino privilege on a metadata object for a specific Kafka principal into one or more
   * Kafka {@link AclBinding}s.
   *
   * @param metadataObject Target securable object.
   * @param privilege Gravitino privilege (name + ALLOW/DENY condition).
   * @param kafkaPrincipal Formatted Kafka principal (e.g. {@code User:alice}).
   * @param properties Authorization plugin configuration properties.
   * @return Immutable or ordered set of Kafka ACL bindings (empty if non-Kafka privilege or type).
   */
  public static Set<AclBinding> toAclBindings(
      MetadataObject metadataObject,
      Privilege privilege,
      String kafkaPrincipal,
      KafkaAuthorizationProperties properties) {
    Preconditions.checkArgument(metadataObject != null, "MetadataObject must not be null");
    Preconditions.checkArgument(privilege != null, "Privilege must not be null");
    Preconditions.checkArgument(kafkaPrincipal != null, "Kafka principal must not be null");
    Preconditions.checkArgument(properties != null, "Properties must not be null");

    ResourcePattern topicTarget = resolveTopicResource(metadataObject);
    if (topicTarget == null) {
      return Collections.emptySet();
    }

    AclPermissionType permissionType = toPermissionType(privilege.condition());
    String host = properties.getAclHost();
    Set<AclBinding> bindings = new LinkedHashSet<>();

    switch (privilege.name()) {
      case PRODUCE_TOPIC:
        if (permissionType == AclPermissionType.ALLOW) {
          bindings.add(
              new AclBinding(
                  topicTarget,
                  new AccessControlEntry(
                      kafkaPrincipal, host, AclOperation.WRITE, AclPermissionType.ALLOW)));
          bindings.add(
              new AclBinding(
                  topicTarget,
                  new AccessControlEntry(
                      kafkaPrincipal, host, AclOperation.DESCRIBE, AclPermissionType.ALLOW)));
          if (properties.isIdempotentWriteAclEnabled()) {
            bindings.add(
                new AclBinding(
                    CLUSTER_RESOURCE,
                    new AccessControlEntry(
                        kafkaPrincipal,
                        host,
                        AclOperation.IDEMPOTENT_WRITE,
                        AclPermissionType.ALLOW)));
          }
        } else {
          // Only deny the primary WRITE operation so shared DESCRIBE or cluster ops remain usable.
          bindings.add(
              new AclBinding(
                  topicTarget,
                  new AccessControlEntry(
                      kafkaPrincipal, host, AclOperation.WRITE, AclPermissionType.DENY)));
        }
        break;

      case CONSUME_TOPIC:
        if (permissionType == AclPermissionType.ALLOW) {
          bindings.add(
              new AclBinding(
                  topicTarget,
                  new AccessControlEntry(
                      kafkaPrincipal, host, AclOperation.READ, AclPermissionType.ALLOW)));
          bindings.add(
              new AclBinding(
                  topicTarget,
                  new AccessControlEntry(
                      kafkaPrincipal, host, AclOperation.DESCRIBE, AclPermissionType.ALLOW)));
          bindings.add(
              new AclBinding(
                  consumerGroupResource(properties),
                  new AccessControlEntry(
                      kafkaPrincipal, host, AclOperation.READ, AclPermissionType.ALLOW)));
        } else {
          // Only deny topic READ so consuming other topics or producing is unaffected.
          bindings.add(
              new AclBinding(
                  topicTarget,
                  new AccessControlEntry(
                      kafkaPrincipal, host, AclOperation.READ, AclPermissionType.DENY)));
        }
        break;

      case CREATE_TOPIC:
        if (permissionType == AclPermissionType.ALLOW) {
          bindings.add(
              new AclBinding(
                  topicTarget,
                  new AccessControlEntry(
                      kafkaPrincipal, host, AclOperation.CREATE, AclPermissionType.ALLOW)));
          bindings.add(
              new AclBinding(
                  topicTarget,
                  new AccessControlEntry(
                      kafkaPrincipal, host, AclOperation.DESCRIBE, AclPermissionType.ALLOW)));
        } else {
          bindings.add(
              new AclBinding(
                  topicTarget,
                  new AccessControlEntry(
                      kafkaPrincipal, host, AclOperation.CREATE, AclPermissionType.DENY)));
        }
        break;

      default:
        LOG.debug(
            "Ignoring non-Kafka privilege {} on {} ({})",
            privilege.name(),
            metadataObject.fullName(),
            metadataObject.type());
        break;
    }

    return Collections.unmodifiableSet(bindings);
  }

  /**
   * Computes Kafka {@link AclBinding}s representing full ownership of a metadata object.
   *
   * @param metadataObject Owned Gravitino object.
   * @param kafkaPrincipal Formatted Kafka principal for the owner.
   * @param properties Authorization plugin configuration properties.
   * @return Set of Kafka ACL bindings granting ownership rights.
   */
  public static Set<AclBinding> ownerAclBindings(
      MetadataObject metadataObject,
      String kafkaPrincipal,
      KafkaAuthorizationProperties properties) {
    Preconditions.checkArgument(metadataObject != null, "MetadataObject must not be null");
    Preconditions.checkArgument(kafkaPrincipal != null, "Kafka principal must not be null");
    Preconditions.checkArgument(properties != null, "Properties must not be null");

    ResourcePattern topicTarget = resolveTopicResource(metadataObject);
    if (topicTarget == null) {
      return Collections.emptySet();
    }

    String host = properties.getAclHost();
    Set<AclBinding> bindings = new LinkedHashSet<>();
    for (AclOperation operation : OWNER_TOPIC_OPERATIONS) {
      bindings.add(
          new AclBinding(
              topicTarget,
              new AccessControlEntry(kafkaPrincipal, host, operation, AclPermissionType.ALLOW)));
    }
    bindings.add(
        new AclBinding(
            consumerGroupResource(properties),
            new AccessControlEntry(
                kafkaPrincipal, host, AclOperation.READ, AclPermissionType.ALLOW)));
    if (properties.isIdempotentWriteAclEnabled()) {
      bindings.add(
          new AclBinding(
              CLUSTER_RESOURCE,
              new AccessControlEntry(
                  kafkaPrincipal, host, AclOperation.IDEMPOTENT_WRITE, AclPermissionType.ALLOW)));
    }
    return Collections.unmodifiableSet(bindings);
  }

  /**
   * Resolves the Kafka topic {@link ResourcePattern} targeted by a Gravitino metadata object, or
   * {@code null} if the object type is not relevant to Kafka topics.
   *
   * @param metadataObject Gravitino metadata object.
   * @return Topic resource pattern, or {@code null}.
   */
  public static ResourcePattern resolveTopicResource(MetadataObject metadataObject) {
    switch (metadataObject.type()) {
      case TOPIC:
        return new ResourcePattern(ResourceType.TOPIC, metadataObject.name(), PatternType.LITERAL);
      case METALAKE:
      case CATALOG:
      case SCHEMA:
        return WILDCARD_TOPIC_RESOURCE;
      default:
        return null;
    }
  }

  private static ResourcePattern consumerGroupResource(KafkaAuthorizationProperties properties) {
    return new ResourcePattern(
        ResourceType.GROUP,
        properties.getConsumerGroupPattern(),
        properties.getConsumerGroupPatternType());
  }

  private static AclPermissionType toPermissionType(Privilege.Condition condition) {
    switch (condition) {
      case ALLOW:
        return AclPermissionType.ALLOW;
      case DENY:
        return AclPermissionType.DENY;
      default:
        throw new IllegalArgumentException("Unsupported privilege condition: " + condition);
    }
  }
}
