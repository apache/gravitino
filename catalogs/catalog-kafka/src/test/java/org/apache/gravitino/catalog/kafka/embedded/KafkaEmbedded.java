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
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import kafka.cluster.EndPoint;
import kafka.server.KafkaConfig;
import kafka.server.KafkaConfig$;
import kafka.server.KafkaServer;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

public class KafkaEmbedded {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaEmbedded.class);
  private static final String LOG_DIR =
      "/tmp/gravitino_test_embeddedKafka_" + KafkaClusterEmbedded.genRandomString();

  private final Properties effectiveConfig;
  private final KafkaServer kafka;

  public KafkaEmbedded(final Properties config) {
    effectiveConfig = effectiveConfigFrom(config);
    final boolean loggingEnabled = true;

    final KafkaConfig kafkaConfig = new KafkaConfig(effectiveConfig, loggingEnabled);
    LOG.info("Starting embedded Kafka broker (with ZK ensemble at {}) ...", zookeeperConnect());
    kafka = new KafkaServer(kafkaConfig, Time.SYSTEM, Option.apply("embedded-kafka-broker"), false);
    kafka.startup();
    LOG.info(
        "Startup of embedded Kafka broker at {} completed (with ZK ensemble at {}) ...",
        brokerList(),
        zookeeperConnect());
  }

  public void createTopic(String topic) {
    Properties properties = new Properties();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList());

    try (AdminClient adminClient = AdminClient.create(properties)) {
      NewTopic newTopic = new NewTopic(topic, 1, (short) 1);
      adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException("Failed to create topic " + topic, e);
    }
  }

  /**
   * This broker's `metadata.broker.list` value. Example: `127.0.0.1:9092`.
   *
   * <p>You can use this to tell Kafka producers and consumers how to connect to this instance.
   */
  public String brokerList() {
    final EndPoint endPoint = kafka.advertisedListeners().head();
    final String hostname = endPoint.host() == null ? "" : endPoint.host();

    return String.join(
        ":",
        hostname,
        Integer.toString(
            kafka.boundPort(ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))));
  }

  /** The ZooKeeper connection string aka `zookeeper.connect`. */
  public String zookeeperConnect() {
    return effectiveConfig.getProperty("zookeeper.connect");
  }

  /** Stop the broker. */
  public void stop() throws IOException {
    LOG.info(
        "Shutting down embedded Kafka broker at {} (with ZK ensemble at {}) ...",
        brokerList(),
        zookeeperConnect());
    kafka.shutdown();
    kafka.awaitShutdown();
    FileUtils.deleteDirectory(FileUtils.getFile(LOG_DIR));
    LOG.info(
        "Shutdown of embedded Kafka broker at {} completed (with ZK ensemble at {}) ...",
        brokerList(),
        zookeeperConnect());
  }

  private Properties effectiveConfigFrom(final Properties initialConfig) {
    final Properties effectiveConfig = new Properties();
    effectiveConfig.put(KafkaConfig$.MODULE$.BrokerIdProp(), 0);
    effectiveConfig.put(KafkaConfig$.MODULE$.NumPartitionsProp(), 1);
    effectiveConfig.put(KafkaConfig$.MODULE$.AutoCreateTopicsEnableProp(), true);
    effectiveConfig.put(KafkaConfig$.MODULE$.MessageMaxBytesProp(), 1000000);
    effectiveConfig.put(KafkaConfig$.MODULE$.ControlledShutdownEnableProp(), true);

    effectiveConfig.putAll(initialConfig);
    effectiveConfig.setProperty(KafkaConfig$.MODULE$.LogDirProp(), LOG_DIR);
    return effectiveConfig;
  }
}
