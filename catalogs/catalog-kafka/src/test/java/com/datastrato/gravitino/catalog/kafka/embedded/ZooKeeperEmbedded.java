/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.kafka.embedded;

import java.io.IOException;
import org.apache.curator.test.TestingServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooKeeperEmbedded {
  private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperEmbedded.class);
  private final TestingServer server;

  /**
   * Creates and starts a ZooKeeper instance.
   *
   * @throws Exception if an error occurs during ZooKeeper startup
   */
  public ZooKeeperEmbedded() throws Exception {
    LOG.info("Starting embedded ZooKeeper server...");
    this.server = new TestingServer();
    LOG.info(
        "Embedded ZooKeeper server at {} uses the temp directory at {}",
        server.getConnectString(),
        server.getTempDirectory());
  }

  public void stop() throws IOException {
    LOG.info("Shutting down embedded ZooKeeper server at {} ...", server.getConnectString());
    server.close();
    LOG.info("Shutdown of embedded ZooKeeper server at {} completed", server.getConnectString());
  }

  /**
   * The ZooKeeper connection string aka `zookeeper.connect` in `hostnameOrIp:port` format. Example:
   * `127.0.0.1:2181`.
   *
   * <p>You can use this to e.g. tell Kafka brokers how to connect to this instance.
   */
  public String connectString() {
    return server.getConnectString();
  }
}
