/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.catalog.lakehouse.iceberg;

import com.datastrato.graviton.aux.GravitonAuxiliaryService;
import com.datastrato.graviton.catalog.lakehouse.iceberg.ops.IcebergTableOps;
import com.datastrato.graviton.catalog.lakehouse.iceberg.web.IcebergExceptionMapper;
import com.datastrato.graviton.catalog.lakehouse.iceberg.web.IcebergObjectMapperProvider;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.thread.ExecutorThreadPool;
import org.eclipse.jetty.util.thread.ScheduledExecutorScheduler;
import org.eclipse.jetty.util.thread.Scheduler;
import org.eclipse.jetty.util.thread.ThreadPool;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergRESTService implements GravitonAuxiliaryService {

  private static Logger LOG = LoggerFactory.getLogger(IcebergRESTService.class);

  private Server server;

  public static final String SERVICE_NAME = "iceberg-rest";

  private ExecutorThreadPool createThreadPool(
      int coreThreads, int maxThreads, int threadPoolWorkQueueSize) {
    return new ExecutorThreadPool(
        new ThreadPoolExecutor(
            coreThreads,
            maxThreads,
            60,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(threadPoolWorkQueueSize),
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("jetty-Iceberg-REST-%d")
                .setUncaughtExceptionHandler(
                    (thread, throwable) -> {
                      LOG.warn("%s uncaught exception:", thread.getName(), throwable);
                    })
                .build()));
  }

  private ServerConnector createServerConnector(
      Server server, ConnectionFactory[] connectionFactories) {
    Scheduler serverExecutor =
        new ScheduledExecutorScheduler("graviton-Iceberg-REST-JettyScheduler", true);

    return new ServerConnector(server, null, serverExecutor, null, -1, -1, connectionFactories);
  }

  private ServerConnector createHttpServerConnector(Server server, String host, int port) {
    HttpConfiguration httpConfig = new HttpConfiguration();
    httpConfig.setSendServerVersion(true);

    HttpConnectionFactory httpConnectionFactory = new HttpConnectionFactory(httpConfig);
    ServerConnector connector =
        createServerConnector(server, new ConnectionFactory[] {httpConnectionFactory});
    connector.setHost(host);
    connector.setPort(port);
    connector.setReuseAddress(true);

    return connector;
  }

  // todo, use JettyServer when it's moved to graviton common package
  private Server createJettyServer(IcebergRESTConfig restConfig) {
    String host = restConfig.get(IcebergRESTConfig.ICEBERG_REST_SERVER_HOST);
    int port = restConfig.get(IcebergRESTConfig.ICEBERG_REST_SERVER_HTTP_PORT);
    int coreThreads = restConfig.get(IcebergRESTConfig.ICEBERG_REST_SERVER_CORE_THREADS);
    int maxThreads = restConfig.get(IcebergRESTConfig.ICEBERG_REST_SERVER_MAX_THREADS);
    int queueSize =
        restConfig.get(IcebergRESTConfig.ICEBERG_REST_SERVER_THREAD_POOL_WORK_QUEUE_SIZE);
    LOG.info("Iceberg REST service http port:{}", port);
    ThreadPool threadPool = createThreadPool(coreThreads, maxThreads, queueSize);
    Server server = new Server(threadPool);
    ServerConnector connector = createHttpServerConnector(server, host, port);
    server.addConnector(connector);
    return server;
  }

  private Server initServer(IcebergRESTConfig restConfig) {

    Server server = createJettyServer(restConfig);

    ResourceConfig config = new ResourceConfig();
    config.packages("com.datastrato.graviton.catalog.lakehouse.iceberg.web.rest");

    config.register(IcebergObjectMapperProvider.class).register(JacksonFeature.class);
    config.register(IcebergExceptionMapper.class);

    IcebergTableOps icebergTableOps = new IcebergTableOps();
    config.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bind(icebergTableOps).to(IcebergTableOps.class).ranked(1);
          }
        });

    ServletHolder servlet = new ServletHolder(new ServletContainer(config));

    ServletContextHandler context = new ServletContextHandler(server, "/");
    context.addServlet(servlet, "/iceberg/*");
    return server;
  }

  @Override
  public String shortName() {
    return SERVICE_NAME;
  }

  @Override
  public void serviceInit(Map<String, String> properties) {
    IcebergRESTConfig icebergConfig = new IcebergRESTConfig();
    icebergConfig.loadFromMap(properties, k -> true);
    server = initServer(icebergConfig);
    LOG.info("Iceberg REST service inited");
  }

  @Override
  public void serviceStart() {
    if (server != null) {
      try {
        server.start();
        LOG.info("Iceberg REST service started");
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void serviceStop() throws Exception {
    if (server != null) {
      server.stop();
      LOG.info("Iceberg REST service stopped");
    }
  }
}
