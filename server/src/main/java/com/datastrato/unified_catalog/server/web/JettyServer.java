package com.datastrato.unified_catalog.server.web;


import com.datastrato.unified_catalog.Config;
import com.datastrato.unified_catalog.server.ServerConfig;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ErrorHandler;
import org.eclipse.jetty.util.thread.ExecutorThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public final class JettyServer {

  private static final Logger LOG = LoggerFactory.getLogger(JettyServer.class);

  private Server server;


  public JettyServer() {
  }

  public void initialize(Config config) {
    int coreThreads = config.get(ServerConfig.WEBSERVER_CORE_THREADS);
    int maxThreads = config.get(ServerConfig.WEBSERVER_MAX_THREADS);
    ExecutorThreadPool threadPool = createThreadPool(coreThreads, maxThreads);

    // Create and config Jetty Server
    server = new Server(threadPool);
    server.setStopAtShutdown(true);
    server.setStopTimeout(config.get(ServerConfig.WEBSERVER_STOP_IDLE_TIMEOUT));

    // Set error handler for Jetty Server
    ErrorHandler errorHandler = new ErrorHandler();
    errorHandler.setShowStacks(true);
    errorHandler.setServer(server);
    server.addBean(errorHandler);

  }

  private ExecutorThreadPool createThreadPool(int coreThreads, int maxThreads) {
    return new ExecutorThreadPool(
        new ThreadPoolExecutor(
            coreThreads,
            maxThreads,
            60,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("jetty-webserver-%d")
                .build())
    );
  }












}
