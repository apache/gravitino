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
package org.apache.gravitino.catalog;

import static org.apache.gravitino.Entity.EntityType.TOPIC;
import static org.apache.gravitino.StringIdentifier.fromProperties;
import static org.apache.gravitino.catalog.PropertiesMetadataHelpers.validatePropertyForCreate;
import static org.apache.gravitino.utils.NameIdentifierUtil.getCatalogIdentifier;

import java.time.Instant;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTopicException;
import org.apache.gravitino.exceptions.TopicAlreadyExistsException;
import org.apache.gravitino.lock.LockType;
import org.apache.gravitino.lock.TreeLockUtils;
import org.apache.gravitino.messaging.DataLayout;
import org.apache.gravitino.messaging.Topic;
import org.apache.gravitino.messaging.TopicChange;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.TopicEntity;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.utils.PrincipalUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicOperationDispatcher extends OperationDispatcher implements TopicDispatcher {
  private static final Logger LOG = LoggerFactory.getLogger(TopicOperationDispatcher.class);

  /**
   * Creates a new TopicOperationDispatcher instance.
   *
   * @param catalogManager The CatalogManager instance to be used for catalog operations.
   * @param store The EntityStore instance to be used for catalog operations.
   * @param idGenerator The IdGenerator instance to be used for catalog operations.
   */
  public TopicOperationDispatcher(
      CatalogManager catalogManager, EntityStore store, IdGenerator idGenerator) {
    super(catalogManager, store, idGenerator);
  }

  /**
   * List the topics in a schema namespace from the catalog.
   *
   * @param namespace A schema namespace.
   * @return An array of topic identifiers in the namespace.
   * @throws NoSuchSchemaException If the schema does not exist.
   */
  @Override
  public NameIdentifier[] listTopics(Namespace namespace) throws NoSuchSchemaException {
    return doWithCatalog(
        getCatalogIdentifier(NameIdentifier.of(namespace.levels())),
        c -> c.doWithTopicOps(t -> t.listTopics(namespace)),
        NoSuchSchemaException.class);
  }

  /**
   * Load topic metadata by {@link NameIdentifier} from the catalog.
   *
   * @param ident A topic identifier.
   * @return The topic metadata.
   * @throws NoSuchTopicException If the topic does not exist.
   */
  @Override
  public Topic loadTopic(NameIdentifier ident) throws NoSuchTopicException {
    EntityCombinedTopic topic =
        TreeLockUtils.doWithTreeLock(ident, LockType.READ, () -> internalLoadTopic(ident));

    if (!topic.imported()) {
      // Load the schema to make sure the schema is imported.
      // This is not necessary for Kafka catalog.
      SchemaDispatcher schemaDispatcher = GravitinoEnv.getInstance().schemaDispatcher();
      NameIdentifier schemaIdent = NameIdentifier.of(ident.namespace().levels());
      schemaDispatcher.loadSchema(schemaIdent);

      // Import the topic
      TreeLockUtils.doWithTreeLock(
          schemaIdent,
          LockType.WRITE,
          () -> {
            importTopic(ident);
            return null;
          });
    }

    return topic;
  }

  /**
   * Create a topic in the catalog.
   *
   * @param ident A topic identifier.
   * @param comment The comment of the topic object. Null is set if no comment is specified.
   * @param dataLayout The message schema of the topic object. Always null because it's not
   *     supported yet.
   * @param properties The properties of the topic object. Empty map is set if no properties are
   *     specified.
   * @return The topic metadata.
   * @throws NoSuchSchemaException If the schema does not exist.
   * @throws TopicAlreadyExistsException If the topic already exists.
   */
  @Override
  public Topic createTopic(
      NameIdentifier ident, String comment, DataLayout dataLayout, Map<String, String> properties)
      throws NoSuchSchemaException, TopicAlreadyExistsException {

    // Load the schema to make sure the schema exists.
    SchemaDispatcher schemaDispatcher = GravitinoEnv.getInstance().schemaDispatcher();
    NameIdentifier schemaIdent = NameIdentifier.of(ident.namespace().levels());
    schemaDispatcher.loadSchema(schemaIdent);

    return TreeLockUtils.doWithTreeLock(
        NameIdentifier.of(ident.namespace().levels()),
        LockType.WRITE,
        () -> internalCreateTopic(ident, comment, dataLayout, properties));
  }

  /**
   * Apply the {@link TopicChange changes} to a topic in the catalog.
   *
   * @param ident A topic identifier.
   * @param changes The changes to apply to the topic.
   * @return The altered topic metadata.
   * @throws NoSuchTopicException If the topic does not exist.
   * @throws IllegalArgumentException If the changes is rejected by the implementation.
   */
  @Override
  public Topic alterTopic(NameIdentifier ident, TopicChange... changes)
      throws NoSuchTopicException, IllegalArgumentException {
    validateAlterProperties(ident, HasPropertyMetadata::topicPropertiesMetadata, changes);

    NameIdentifier catalogIdent = getCatalogIdentifier(ident);

    // we do not retrieve the topic again (to obtain some values generated by underlying catalog)
    // since some catalogs' API is async and the topic may not be created immediately
    Topic alteredTopic =
        doWithCatalog(
            catalogIdent,
            c -> c.doWithTopicOps(t -> t.alterTopic(ident, changes)),
            NoSuchTopicException.class,
            IllegalArgumentException.class);

    TopicEntity updatedTopicEntity =
        operateOnEntity(
            ident,
            id ->
                store.update(
                    id,
                    TopicEntity.class,
                    TOPIC,
                    topicEntity ->
                        TopicEntity.builder()
                            .withId(topicEntity.id())
                            .withName(topicEntity.name())
                            .withNamespace(ident.namespace())
                            .withComment(
                                StringUtils.isBlank(alteredTopic.comment())
                                    ? topicEntity.comment()
                                    : alteredTopic.comment())
                            .withAuditInfo(
                                AuditInfo.builder()
                                    .withCreator(topicEntity.auditInfo().creator())
                                    .withCreateTime(topicEntity.auditInfo().createTime())
                                    .withLastModifier(
                                        PrincipalUtils.getCurrentPrincipal().getName())
                                    .withLastModifiedTime(Instant.now())
                                    .build())
                            .build()),
            "UPDATE",
            getStringIdFromProperties(alteredTopic.properties()).id());

    return EntityCombinedTopic.of(alteredTopic, updatedTopicEntity)
        .withHiddenPropertiesSet(
            getHiddenPropertyNames(
                catalogIdent,
                HasPropertyMetadata::topicPropertiesMetadata,
                alteredTopic.properties()));
  }

  /**
   * Drop a topic from the catalog.
   *
   * @param ident A topic identifier.
   * @return true If the topic is dropped, false if the topic does not exist.
   * @throws RuntimeException If an error occurs while dropping the topic.
   */
  @Override
  public boolean dropTopic(NameIdentifier ident) {
    NameIdentifier catalogIdent = getCatalogIdentifier(ident);
    boolean droppedFromCatalog =
        doWithCatalog(
            catalogIdent, c -> c.doWithTopicOps(t -> t.dropTopic(ident)), RuntimeException.class);

    // For unmanaged topic, it could happen that the topic:
    // 1. Is not found in the catalog (dropped directly from underlying sources)
    // 2. Is found in the catalog but not in the store (not managed by Gravitino)
    // 3. Is found in the catalog and the store (managed by Gravitino)
    // 4. Neither found in the catalog nor in the store.
    // In all situations, we try to delete the schema from the store, but we don't take the
    // return value of the store operation into account. We only take the return value of the
    // catalog into account.
    //
    // For managed topic, we should take the return value of the store operation into account.
    boolean droppedFromStore = false;
    try {
      droppedFromStore = store.delete(ident, TOPIC);
    } catch (NoSuchEntityException e) {
      LOG.warn("The topic to be dropped does not exist in the store: {}", ident, e);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return isManagedEntity(catalogIdent, Capability.Scope.TOPIC)
        ? droppedFromStore
        : droppedFromCatalog;
  }

  private void importTopic(NameIdentifier identifier) {

    EntityCombinedTopic topic = internalLoadTopic(identifier);

    if (topic.imported()) {
      return;
    }

    StringIdentifier stringId = null;
    try {
      stringId = topic.stringIdentifier();
    } catch (IllegalArgumentException ie) {
      LOG.warn(FormattedErrorMessages.STRING_ID_PARSE_ERROR, ie.getMessage());
    }

    long uid;
    if (stringId != null) {
      // For Kafka topic, the uid is coming from topic UUID, which is always existed.
      LOG.warn(
          "The Topic uid {} existed but still needs to be imported, this could be happened "
              + "when Topic is created externally without leveraging Gravitino. In this "
              + "case, we need to store the stored entity to keep consistency.",
          stringId);
      uid = stringId.id();
    } else {
      // This will not be happened for now, since we only support Kafka, and it always has an uid.
      uid = idGenerator.nextId();
    }

    TopicEntity topicEntity =
        TopicEntity.builder()
            .withId(uid)
            .withName(topic.name())
            .withComment(topic.comment())
            .withNamespace(identifier.namespace())
            .withAuditInfo(
                AuditInfo.builder()
                    .withCreator(topic.auditInfo().creator())
                    .withCreateTime(topic.auditInfo().createTime())
                    .withLastModifier(topic.auditInfo().lastModifier())
                    .withLastModifiedTime(topic.auditInfo().lastModifiedTime())
                    .build())
            .build();

    try {
      store.put(topicEntity, true);
    } catch (Exception e) {
      LOG.error(FormattedErrorMessages.STORE_OP_FAILURE, "put", identifier, e);
      throw new RuntimeException("Fail to import topic entity to store.", e);
    }
  }

  private EntityCombinedTopic internalLoadTopic(NameIdentifier ident) {
    NameIdentifier catalogIdent = getCatalogIdentifier(ident);
    Topic topic =
        doWithCatalog(
            catalogIdent,
            c -> c.doWithTopicOps(t -> t.loadTopic(ident)),
            NoSuchTopicException.class);

    StringIdentifier stringId = getStringIdFromProperties(topic.properties());
    if (stringId == null) {
      return EntityCombinedTopic.of(topic)
          .withHiddenPropertiesSet(
              getHiddenPropertyNames(
                  catalogIdent, HasPropertyMetadata::topicPropertiesMetadata, topic.properties()))
          .withImported(isEntityExist(ident, TOPIC));
    }

    TopicEntity topicEntity =
        operateOnEntity(
            ident,
            identifier -> store.get(identifier, TOPIC, TopicEntity.class),
            "GET",
            getStringIdFromProperties(topic.properties()).id());

    return EntityCombinedTopic.of(topic, topicEntity)
        .withHiddenPropertiesSet(
            getHiddenPropertyNames(
                catalogIdent, HasPropertyMetadata::topicPropertiesMetadata, topic.properties()))
        .withImported(topicEntity != null);
  }

  private Topic internalCreateTopic(
      NameIdentifier ident, String comment, DataLayout dataLayout, Map<String, String> properties) {
    NameIdentifier catalogIdent = getCatalogIdentifier(ident);
    doWithCatalog(
        catalogIdent,
        c ->
            c.doWithPropertiesMeta(
                p -> {
                  validatePropertyForCreate(p.topicPropertiesMetadata(), properties);
                  return null;
                }),
        IllegalArgumentException.class);
    Long uid = idGenerator.nextId();
    StringIdentifier stringId = StringIdentifier.fromId(uid);
    Map<String, String> updatedProperties =
        StringIdentifier.newPropertiesWithId(stringId, properties);

    // we do not retrieve the topic again (to obtain some values generated by underlying catalog)
    // since some catalogs' API is async and the table may not be created immediately
    Topic topic =
        doWithCatalog(
            catalogIdent,
            c ->
                c.doWithTopicOps(t -> t.createTopic(ident, comment, dataLayout, updatedProperties)),
            NoSuchSchemaException.class,
            TopicAlreadyExistsException.class);

    TopicEntity topicEntity =
        TopicEntity.builder()
            .withId(fromProperties(topic.properties()).id())
            .withName(ident.name())
            .withComment(comment)
            .withNamespace(ident.namespace())
            .withAuditInfo(
                AuditInfo.builder()
                    .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                    .withCreateTime(Instant.now())
                    .build())
            .build();

    try {
      store.put(topicEntity, true /* overwrite */);
    } catch (Exception e) {
      LOG.error(OperationDispatcher.FormattedErrorMessages.STORE_OP_FAILURE, "put", ident, e);
      return EntityCombinedTopic.of(topic)
          .withHiddenPropertiesSet(
              getHiddenPropertyNames(
                  catalogIdent, HasPropertyMetadata::topicPropertiesMetadata, topic.properties()));
    }

    return EntityCombinedTopic.of(topic, topicEntity)
        .withHiddenPropertiesSet(
            getHiddenPropertyNames(
                catalogIdent, HasPropertyMetadata::topicPropertiesMetadata, topic.properties()));
  }
}
