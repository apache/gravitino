/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.client;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.dto.AuditDTO;
import com.datastrato.gravitino.dto.CatalogDTO;
import com.datastrato.gravitino.dto.requests.TopicCreateRequest;
import com.datastrato.gravitino.dto.requests.TopicUpdateRequest;
import com.datastrato.gravitino.dto.requests.TopicUpdatesRequest;
import com.datastrato.gravitino.dto.responses.DropResponse;
import com.datastrato.gravitino.dto.responses.EntityListResponse;
import com.datastrato.gravitino.dto.responses.TopicResponse;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NoSuchTopicException;
import com.datastrato.gravitino.exceptions.TopicAlreadyExistsException;
import com.datastrato.gravitino.messaging.DataLayout;
import com.datastrato.gravitino.messaging.Topic;
import com.datastrato.gravitino.messaging.TopicCatalog;
import com.datastrato.gravitino.messaging.TopicChange;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

/**
 * Messaging catalog is a catalog implementation that supports messaging-like metadata operations,
 * for example, topics list, creation, update and deletion. A Messaging catalog is under the
 * metalake.
 */
public class MessagingCatalog extends BaseSchemaCatalog implements TopicCatalog {

  MessagingCatalog(
      String name,
      Type type,
      String provider,
      String comment,
      Map<String, String> properties,
      AuditDTO auditDTO,
      RESTClient restClient) {
    super(name, type, provider, comment, properties, auditDTO, restClient);
  }

  /** @return A new builder for {@link MessagingCatalog}. */
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
   * @param namespace The namespace to list the topics under it.
   * @return An array of {@link NameIdentifier} of the topics under the specified namespace.
   * @throws NoSuchSchemaException if the schema with specified namespace does not exist.
   */
  @Override
  public NameIdentifier[] listTopics(Namespace namespace) throws NoSuchSchemaException {
    Namespace.checkTopic(namespace);

    EntityListResponse resp =
        restClient.get(
            formatTopicRequestPath(namespace),
            EntityListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.topicErrorHandler());

    resp.validate();

    return resp.identifiers();
  }

  /**
   * Load the topic with the given identifier.
   *
   * @param ident The identifier of the topic to load.
   * @return The {@link Topic} with the specified identifier.
   * @throws NoSuchTopicException if the topic with the specified identifier does not exist.
   */
  @Override
  public Topic loadTopic(NameIdentifier ident) throws NoSuchTopicException {
    NameIdentifier.checkTopic(ident);

    TopicResponse resp =
        restClient.get(
            formatTopicRequestPath(ident.namespace()) + "/" + ident.name(),
            TopicResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.topicErrorHandler());
    resp.validate();

    return resp.getTopic();
  }

  /**
   * Create a new topic with the given identifier, comment, data layout and properties.
   *
   * @param ident A topic identifier.
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
    NameIdentifier.checkTopic(ident);

    TopicCreateRequest req =
        TopicCreateRequest.builder()
            .name(ident.name())
            .comment(comment)
            .properties(properties)
            .build();

    TopicResponse resp =
        restClient.post(
            formatTopicRequestPath(ident.namespace()),
            req,
            TopicResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.topicErrorHandler());
    resp.validate();

    return resp.getTopic();
  }

  /**
   * Alter the topic with the given identifier.
   *
   * @param ident A topic identifier.
   * @param changes The changes to apply to the topic.
   * @return The altered topic object.
   * @throws NoSuchTopicException if the topic with the specified identifier does not exist.
   * @throws IllegalArgumentException if the changes are invalid.
   */
  @Override
  public Topic alterTopic(NameIdentifier ident, TopicChange... changes)
      throws NoSuchTopicException, IllegalArgumentException {
    NameIdentifier.checkTopic(ident);

    List<TopicUpdateRequest> updates =
        Arrays.stream(changes)
            .map(DTOConverters::toTopicUpdateRequest)
            .collect(Collectors.toList());
    TopicUpdatesRequest updatesRequest = new TopicUpdatesRequest(updates);
    updatesRequest.validate();

    TopicResponse resp =
        restClient.put(
            formatTopicRequestPath(ident.namespace()) + "/" + ident.name(),
            updatesRequest,
            TopicResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.topicErrorHandler());
    resp.validate();

    return resp.getTopic();
  }

  /**
   * Drop the topic with the given identifier.
   *
   * @param ident A topic identifier.
   * @return True if the topic is dropped successfully, false the topic does not exist.
   */
  @Override
  public boolean dropTopic(NameIdentifier ident) {
    NameIdentifier.checkTopic(ident);

    DropResponse resp =
        restClient.delete(
            formatTopicRequestPath(ident.namespace()) + "/" + ident.name(),
            DropResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.topicErrorHandler());
    resp.validate();

    return resp.dropped();
  }

  @VisibleForTesting
  static String formatTopicRequestPath(Namespace ns) {
    Namespace schemaNs = Namespace.of(ns.level(0), ns.level(1));
    return formatSchemaRequestPath(schemaNs) + "/" + ns.level(2) + "/topics";
  }

  static class Builder extends CatalogDTO.Builder<Builder> {
    /** The REST client to send the requests. */
    private RESTClient restClient;

    private Builder() {}

    Builder withRestClient(RESTClient restClient) {
      this.restClient = restClient;
      return this;
    }

    @Override
    public MessagingCatalog build() {
      Preconditions.checkArgument(StringUtils.isNotBlank(name), "name must not be blank");
      Preconditions.checkArgument(type != null, "type must not be null");
      Preconditions.checkArgument(StringUtils.isNotBlank(provider), "provider must not be blank");
      Preconditions.checkArgument(audit != null, "audit must not be null");
      Preconditions.checkArgument(restClient != null, "restClient must be set");

      return new MessagingCatalog(name, type, provider, comment, properties, audit, restClient);
    }
  }
}
