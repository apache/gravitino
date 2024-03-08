/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.messaging;

import com.datastrato.gravitino.Auditable;
import com.datastrato.gravitino.annotation.Evolving;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * An interface representing a topic under a schema {@link com.datastrato.gravitino.Namespace}. A
 * topic is a message queue that is managed by Gravitino. Users can create/drop/alter a topic on the
 * Message Queue system like Kafka, Pulsar, etc.
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
}
