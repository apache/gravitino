/*
 * Copyright Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gravitino.catalog.kafka.embedded;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Properties;
import java.util.UUID;
import kafka.server.KafkaConfig;
import kafka.server.KafkaConfig$;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class KafkaClusterEmbedded {
  public static final String TOPIC_1 = "kafka-test-topic-1";
  public static final String TOPIC_2 = "kafka-test-topic-2";
  public static final String TOPIC_3 = "kafka-test-topic-3";

  private static final Logger LOG = LoggerFactory.getLogger(KafkaClusterEmbedded.class);
  private static ZooKeeperEmbedded zookeeper;
  private static KafkaEmbedded broker;

  /** Creates and starts the cluster. */
  @BeforeAll
  public static void start() throws Exception {
    LOG.info("Initiating embedded Kafka cluster startup");
    LOG.info("Starting a ZooKeeper instance...");
    zookeeper = new ZooKeeperEmbedded();
    LOG.info("ZooKeeper instance is running at {}", zookeeper.connectString());

    Properties brokerConfig = initBrokerConfig();
    LOG.info(
        "Starting a Kafka instance on port {} ...",
        brokerConfig.getProperty(KafkaConfig.ListenersProp()));
    broker = new KafkaEmbedded(brokerConfig);
    LOG.info(
        "Kafka instance is running at {}, connected to ZooKeeper at {}",
        broker.brokerList(),
        broker.zookeeperConnect());

    // Create initial topics
    broker.createTopic(TOPIC_1);
    broker.createTopic(TOPIC_2);
    broker.createTopic(TOPIC_3);
  }

  @AfterAll
  public static void stop() throws IOException {
    LOG.info("Stopping embedded Kafka cluster");
    if (broker != null) {
      broker.stop();
    }

    if (zookeeper != null) {
      zookeeper.stop();
    }

    LOG.info("Embedded Kafka cluster stopped");
  }

  protected static String genRandomString() {
    return UUID.randomUUID().toString().replace("-", "");
  }

  public static String brokerList() {
    return broker.brokerList();
  }

  private static Properties initBrokerConfig() {
    Properties configs = new Properties();
    configs.put(KafkaConfig$.MODULE$.ZkConnectProp(), zookeeper.connectString());
    configs.put(KafkaConfig$.MODULE$.ZkSessionTimeoutMsProp(), 30 * 1000);
    configs.put(KafkaConfig$.MODULE$.ZkConnectionTimeoutMsProp(), 60 * 1000);
    configs.put(KafkaConfig$.MODULE$.DeleteTopicEnableProp(), true);
    configs.put(KafkaConfig$.MODULE$.LogCleanerDedupeBufferSizeProp(), 2 * 1024 * 1024L);
    configs.put(KafkaConfig$.MODULE$.GroupMinSessionTimeoutMsProp(), 0);
    configs.put(KafkaConfig$.MODULE$.OffsetsTopicReplicationFactorProp(), (short) 1);
    configs.put(KafkaConfig$.MODULE$.OffsetsTopicPartitionsProp(), 1);
    configs.put(KafkaConfig$.MODULE$.AutoCreateTopicsEnableProp(), true);
    // Find a random port
    try (ServerSocket socket = new ServerSocket(0)) {
      socket.setReuseAddress(true);
      configs.put(
          KafkaConfig.ListenersProp(),
          String.format("PLAINTEXT://127.0.0.1:%s", socket.getLocalPort()));
    } catch (IOException e) {
      throw new RuntimeException("Can't find a port to start embedded Kafka broker", e);
    }
    return configs;
  }
}
