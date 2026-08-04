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
import org.apache.gravitino.annotation.Evolving;

/**
 * Schema metadata for a single named message payload of a {@link Topic}.
 *
 * <p>A topic may carry multiple named layouts (see {@link Topic#dataLayouts()}). Conventional names
 * are {@link DataLayouts#KEY} (record key) and {@link DataLayouts#VALUE} (record value). One {@link
 * DataLayout} always describes a single payload role — never both key and value together.
 *
 * <p>Fields may describe an external schema-registry reference ({@link #schemaUri()}, {@link
 * #schemaSubject()}, {@link #schemaId()}, {@link #schemaVersion()}), inline schema text ({@link
 * #schemaText()}), a type name ({@link #typeName()}), or a combination. Catalogs such as Kafka do
 * not store layouts in the broker; Gravitino persists them in its entity store.
 */
@Evolving
public interface DataLayout {

  /** Serialization / registry format of the message payload. */
  enum Format {
    /** Format is not specified. */
    UNKNOWN,
    /** Google Protocol Buffers. */
    PROTOBUF,
    /** Apache Avro. */
    AVRO,
    /** JSON Schema / JSON payload. */
    JSON
  }

  /**
   * @return The payload format. Never null; defaults to {@link Format#UNKNOWN} when unspecified.
   */
  default Format format() {
    return Format.UNKNOWN;
  }

  /**
   * @return Fully-qualified type / record name (e.g. protobuf message FQN), or null.
   */
  @Nullable
  default String typeName() {
    return null;
  }

  /**
   * @return Schema Registry (or equivalent) schema id, or null.
   */
  @Nullable
  default String schemaId() {
    return null;
  }

  /**
   * @return Schema version / subject version string, or null.
   */
  @Nullable
  default String schemaVersion() {
    return null;
  }

  /**
   * @return Schema Registry base URL or other schema location URI, or null.
   */
  @Nullable
  default String schemaUri() {
    return null;
  }

  /**
   * @return Schema Registry subject name, or null.
   */
  @Nullable
  default String schemaSubject() {
    return null;
  }

  /**
   * @return Inline schema text (Avro JSON, .proto text, JSON Schema), or null.
   */
  @Nullable
  default String schemaText() {
    return null;
  }

  /**
   * @return Additional vendor-specific properties. Never null; implementations should return an
   *     unmodifiable copy.
   */
  default Map<String, String> properties() {
    return Collections.emptyMap();
  }
}
