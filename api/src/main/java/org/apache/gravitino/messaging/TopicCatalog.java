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
package org.apache.gravitino.messaging;

import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTopicException;
import org.apache.gravitino.exceptions.TopicAlreadyExistsException;

/**
 * The {@link TopicCatalog} interface defines the public API for managing topic objects in a schema.
 * If the catalog implementation supports topic objects, it should implement this interface.
 */
@Evolving
public interface TopicCatalog {

  /**
   * List the topics in a schema namespace from the catalog.
   *
   * @param namespace A schema namespace.
   * @return An array of topic identifiers in the namespace.
   * @throws NoSuchSchemaException If the schema does not exist.
   */
  NameIdentifier[] listTopics(Namespace namespace) throws NoSuchSchemaException;

  /**
   * Load topic metadata by {@link NameIdentifier} from the catalog.
   *
   * @param ident A topic identifier.
   * @return The topic metadata.
   * @throws NoSuchTopicException If the topic does not exist.
   */
  Topic loadTopic(NameIdentifier ident) throws NoSuchTopicException;

  /**
   * Check if a topic exists using an {@link NameIdentifier} from the catalog.
   *
   * @param ident A topic identifier.
   * @return true If the topic exists, false otherwise.
   */
  default boolean topicExists(NameIdentifier ident) {
    try {
      loadTopic(ident);
      return true;
    } catch (NoSuchTopicException e) {
      return false;
    }
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
  Topic createTopic(
      NameIdentifier ident, String comment, DataLayout dataLayout, Map<String, String> properties)
      throws NoSuchSchemaException, TopicAlreadyExistsException;

  /**
   * Apply the {@link TopicChange changes} to a topic in the catalog.
   *
   * @param ident A topic identifier.
   * @param changes The changes to apply to the topic.
   * @return The altered topic metadata.
   * @throws NoSuchTopicException If the topic does not exist.
   * @throws IllegalArgumentException If the changes is rejected by the implementation.
   */
  Topic alterTopic(NameIdentifier ident, TopicChange... changes)
      throws NoSuchTopicException, IllegalArgumentException;

  /**
   * Drop a topic from the catalog.
   *
   * @param ident A topic identifier.
   * @return true If the topic is dropped, false if the topic does not exist.
   */
  boolean dropTopic(NameIdentifier ident);
}
