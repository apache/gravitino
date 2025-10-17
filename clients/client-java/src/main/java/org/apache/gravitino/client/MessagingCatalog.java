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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.CatalogDTO;
import org.apache.gravitino.dto.requests.TopicCreateRequest;
import org.apache.gravitino.dto.requests.TopicUpdateRequest;
import org.apache.gravitino.dto.requests.TopicUpdatesRequest;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.EntityListResponse;
import org.apache.gravitino.dto.responses.TopicResponse;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTopicException;
import org.apache.gravitino.exceptions.TopicAlreadyExistsException;
import org.apache.gravitino.messaging.DataLayout;
import org.apache.gravitino.messaging.Topic;
import org.apache.gravitino.messaging.TopicCatalog;
import org.apache.gravitino.messaging.TopicChange;
import org.apache.gravitino.rest.RESTUtils;

/**
 * Messaging catalog is a catalog implementation that supports messaging-like metadata operations,
 * for example, topics list, creation, update and deletion. A Messaging catalog is under the
 * metalake.
 */
class MessagingCatalog extends BaseSchemaCatalog implements TopicCatalog {

  MessagingCatalog(
      Namespace namespace,
      String name,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties,
      AuditDTO auditDTO,
      RESTClient restClient) {
    super(namespace, name, type, provider, comment, properties, auditDTO, restClient);
  }

  /**
   * @return A new builder for {@link MessagingCatalog}.
   */
  public static Builder builder() {
    return new Builder();
  }

  @Override
  public TopicCatalog asTopicCatalog() throws UnsupportedOperationException {
    return this;
  }

  /**
   * List all the topics under the given namespace.
   *
   * @param namespace A schema namespace. This namespace should have 1 level, which is the schema
   *     name;
   * @return An array of {@link NameIdentifier} of the topics under the specified namespace.
   * @throws NoSuchSchemaException if the schema with specified namespace does not exist.
   */
  @Override
  public NameIdentifier[] listTopics(Namespace namespace) throws NoSuchSchemaException {
    checkTopicNamespace(namespace);

    Namespace fullNamespace = getTopicFullNamespace(namespace);
    EntityListResponse resp =
        restClient.get(
            formatTopicRequestPath(fullNamespace),
            EntityListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.topicErrorHandler());

    resp.validate();

    return Arrays.stream(resp.identifiers())
        .map(ident -> NameIdentifier.of(ident.namespace().level(2), ident.name()))
        .toArray(NameIdentifier[]::new);
  }

  /**
   * Load the topic with the given identifier.
   *
   * @param ident The identifier of the topic to load, which should be "schema.topic" format.
   * @return The {@link Topic} with the specified identifier.
   * @throws NoSuchTopicException if the topic with the specified identifier does not exist.
   */
  @Override
  public Topic loadTopic(NameIdentifier ident) throws NoSuchTopicException {
    checkTopicNameIdentifier(ident);

    Namespace fullNamespace = getTopicFullNamespace(ident.namespace());
    TopicResponse resp =
        restClient.get(
            formatTopicRequestPath(fullNamespace) + "/" + RESTUtils.encodeString(ident.name()),
            TopicResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.topicErrorHandler());
    resp.validate();

    return new GenericTopic(resp.getTopic(), restClient, fullNamespace);
  }

  /**
   * Create a new topic with the given identifier, comment, data layout and properties.
   *
   * @param ident A topic identifier, which should be "schema.topic" format.
   * @param comment The comment of the topic object. Null is set if no comment is specified.
   * @param dataLayout The message schema of the topic object. Always null because it's not
   *     supported yet.
   * @param properties The properties of the topic object. Empty map is set if no properties are
   *     specified.
   * @return The created topic object.
   * @throws NoSuchSchemaException if the schema with specified namespace does not exist.
   * @throws TopicAlreadyExistsException if the topic with specified identifier already exists.
   */
  @Override
  public Topic createTopic(
      NameIdentifier ident, String comment, DataLayout dataLayout, Map<String, String> properties)
      throws NoSuchSchemaException, TopicAlreadyExistsException {
    checkTopicNameIdentifier(ident);

    Namespace fullNamespace = getTopicFullNamespace(ident.namespace());
    TopicCreateRequest req =
        TopicCreateRequest.builder()
            .name(ident.name())
            .comment(comment)
            .properties(properties)
            .build();

    TopicResponse resp =
        restClient.post(
            formatTopicRequestPath(fullNamespace),
            req,
            TopicResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.topicErrorHandler());
    resp.validate();

    return new GenericTopic(resp.getTopic(), restClient, fullNamespace);
  }

  /**
   * Alter the topic with the given identifier.
   *
   * @param ident A topic identifier, which should be "schema.topic" format.
   * @param changes The changes to apply to the topic.
   * @return The altered topic object.
   * @throws NoSuchTopicException if the topic with the specified identifier does not exist.
   * @throws IllegalArgumentException if the changes are invalid.
   */
  @Override
  public Topic alterTopic(NameIdentifier ident, TopicChange... changes)
      throws NoSuchTopicException, IllegalArgumentException {
    checkTopicNameIdentifier(ident);

    Namespace fullNamespace = getTopicFullNamespace(ident.namespace());
    List<TopicUpdateRequest> updates =
        Arrays.stream(changes)
            .map(DTOConverters::toTopicUpdateRequest)
            .collect(Collectors.toList());
    TopicUpdatesRequest updatesRequest = new TopicUpdatesRequest(updates);
    updatesRequest.validate();

    TopicResponse resp =
        restClient.put(
            formatTopicRequestPath(fullNamespace) + "/" + RESTUtils.encodeString(ident.name()),
            updatesRequest,
            TopicResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.topicErrorHandler());
    resp.validate();

    return new GenericTopic(resp.getTopic(), restClient, fullNamespace);
  }

  /**
   * Drop the topic with the given identifier.
   *
   * @param ident A topic identifier, which should be "schema.topic" format.
   * @return True if the topic is dropped successfully, false the topic does not exist.
   */
  @Override
  public boolean dropTopic(NameIdentifier ident) {
    checkTopicNameIdentifier(ident);

    Namespace fullNamespace = getTopicFullNamespace(ident.namespace());
    DropResponse resp =
        restClient.delete(
            formatTopicRequestPath(fullNamespace) + "/" + RESTUtils.encodeString(ident.name()),
            DropResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.topicErrorHandler());
    resp.validate();

    return resp.dropped();
  }

  @VisibleForTesting
  static String formatTopicRequestPath(Namespace ns) {
    Namespace schemaNs = Namespace.of(ns.level(0), ns.level(1));
    return formatSchemaRequestPath(schemaNs)
        + "/"
        + RESTUtils.encodeString(ns.level(2))
        + "/topics";
  }

  /**
   * Check whether the namespace of a topic is valid.
   *
   * @param namespace The namespace to check.
   */
  static void checkTopicNamespace(Namespace namespace) {
    Namespace.check(
        namespace != null && namespace.length() == 1,
        "Topic namespace must be non-null and have 1 level, the input namespace is %s",
        namespace);
  }

  /**
   * Check whether the NameIdentifier of a topic is valid.
   *
   * @param ident The NameIdentifier to check, which should be "schema.topic" format.
   */
  static void checkTopicNameIdentifier(NameIdentifier ident) {
    NameIdentifier.check(ident != null, "NameIdentifier must not be null");
    NameIdentifier.check(
        ident.name() != null && !ident.name().isEmpty(), "NameIdentifier name must not be empty");
    checkTopicNamespace(ident.namespace());
  }

  /**
   * Get the full namespace of the topic with the given topic's short namespace (schema name).
   *
   * @param topicNamespace The topic's short namespace, which is the schema name.
   * @return full namespace of the topic, which is "metalake.catalog.schema" format.
   */
  private Namespace getTopicFullNamespace(Namespace topicNamespace) {
    return Namespace.of(this.catalogNamespace().level(0), this.name(), topicNamespace.level(0));
  }

  static class Builder extends CatalogDTO.Builder<Builder> {
    /** The REST client to send the requests. */
    private RESTClient restClient;

    /** The namespace of the catalog */
    private Namespace namespace;

    private Builder() {}

    Builder withNamespace(Namespace namespace) {
      this.namespace = namespace;
      return this;
    }

    Builder withRestClient(RESTClient restClient) {
      this.restClient = restClient;
      return this;
    }

    @Override
    public MessagingCatalog build() {
      Namespace.check(
          namespace != null && namespace.length() == 1,
          "Catalog namespace must be non-null and have 1 level, the input namespace is %s",
          namespace);
      Preconditions.checkArgument(StringUtils.isNotBlank(name), "name must not be blank");
      Preconditions.checkArgument(type != null, "type must not be null");
      Preconditions.checkArgument(StringUtils.isNotBlank(provider), "provider must not be blank");
      Preconditions.checkArgument(audit != null, "audit must not be null");
      Preconditions.checkArgument(restClient != null, "restClient must be set");

      return new MessagingCatalog(
          namespace, name, type, provider, comment, properties, audit, restClient);
    }
  }
}
