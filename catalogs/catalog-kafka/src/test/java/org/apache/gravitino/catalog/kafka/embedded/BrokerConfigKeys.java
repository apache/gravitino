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

package org.apache.gravitino.catalog.kafka.embedded;

/**
 * Broker configuration keys used by the embedded Kafka cluster in tests.
 *
 * <p>Kafka no longer exposes the Scala {@code KafkaConfig} property accessors as a stable API, so
 * the plain configuration keys are centralized here.
 */
final class BrokerConfigKeys {

  static final String ZOOKEEPER_CONNECT = "zookeeper.connect";
  static final String ZOOKEEPER_SESSION_TIMEOUT_MS = "zookeeper.session.timeout.ms";
  static final String ZOOKEEPER_CONNECTION_TIMEOUT_MS = "zookeeper.connection.timeout.ms";
  static final String DELETE_TOPIC_ENABLE = "delete.topic.enable";
  static final String LOG_CLEANER_DEDUPE_BUFFER_SIZE = "log.cleaner.dedupe.buffer.size";
  static final String GROUP_MIN_SESSION_TIMEOUT_MS = "group.min.session.timeout.ms";
  static final String OFFSETS_TOPIC_REPLICATION_FACTOR = "offsets.topic.replication.factor";
  static final String OFFSETS_TOPIC_NUM_PARTITIONS = "offsets.topic.num.partitions";
  static final String AUTO_CREATE_TOPICS_ENABLE = "auto.create.topics.enable";
  static final String LISTENERS = "listeners";
  static final String BROKER_ID = "broker.id";
  static final String NUM_PARTITIONS = "num.partitions";
  static final String MESSAGE_MAX_BYTES = "message.max.bytes";
  static final String CONTROLLED_SHUTDOWN_ENABLE = "controlled.shutdown.enable";
  static final String LOG_DIR = "log.dir";

  private BrokerConfigKeys() {}
}
