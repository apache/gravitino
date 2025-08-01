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

import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.Auditable;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.authorization.SupportsRoles;
import org.apache.gravitino.policy.SupportsPolicies;
import org.apache.gravitino.tag.SupportsTags;

/**
 * An interface representing a topic under a schema {@link Namespace}. A topic is a message queue
 * that is managed by Apache Gravitino. Users can create/drop/alter a topic on the Message Queue
 * system like Apache Kafka, Apache Pulsar, etc.
 *
 * <p>{@link Topic} defines the basic properties of a topic object. A catalog implementation with
 * {@link TopicCatalog} should implement this interface.
 */
@Evolving
public interface Topic extends Auditable {

  /** @return Name of the topic */
  String name();

  /** @return The comment of the topic object. Null is returned if no comment is set. */
  @Nullable
  default String comment() {
    return null;
  }

  /** @return The properties of the topic object. Empty map is returned if no properties are set. */
  default Map<String, String> properties() {
    return Collections.emptyMap();
  }

  /**
   * @return the {@link SupportsTags} if the topic supports tag operations.
   * @throws UnsupportedOperationException if the topic does not support tag operations.
   */
  default SupportsTags supportsTags() {
    throw new UnsupportedOperationException("Topic does not support tag operations.");
  }

  /**
   * @return the {@link SupportsPolicies} if the topic supports policy operations.
   * @throws UnsupportedOperationException if the topic does not support policy operations.
   */
  default SupportsPolicies supportsPolicies() {
    throw new UnsupportedOperationException("Topic does not support policy operations.");
  }

  /**
   * @return the {@link SupportsRoles} if the topic supports role operations.
   * @throws UnsupportedOperationException if the topic does not support role operations.
   */
  default SupportsRoles supportsRoles() {
    throw new UnsupportedOperationException("Topic does not support role operations.");
  }
}
