/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web;

import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.nio.file.Files;
import java.util.EnumSet;
import java.util.concurrent.LinkedBlockingQueue;
import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import javax.servlet.Servlet;
import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ErrorHandler;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.component.LifeCycle;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.util.thread.ScheduledExecutorScheduler;
import org.eclipse.jetty.util.thread.Scheduler;
import org.eclipse.jetty.util.thread.ThreadPool;
import org.eclipse.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JettyServer {

  private static final Logger LOG = LoggerFactory.getLogger(JettyServer.class);

  private Server server;

  private WebAppContext servletContextHandler;

  private JettyServerConfig serverConfig;

  private String serverName;

  public JettyServer() {}

  public synchronized void initialize(JettyServerConfig serverConfig, String serverName) {
    this.serverConfig = serverConfig;
    this.serverName = serverName;

    ThreadPool threadPool =
        createThreadPool(
            serverConfig.getMinThreads(),
            serverConfig.getMaxThreads(),
            serverConfig.getThreadPoolWorkQueueSize());

    // Create and config Jetty Server
    server = new Server(threadPool);
    server.setStopAtShutdown(true);
    server.setStopTimeout(serverConfig.getStopTimeout());

    // Set error handler for Jetty Server
    ErrorHandler errorHandler = new ErrorHandler();
    errorHandler.setShowStacks(true);
    errorHandler.setServer(server);
    server.addBean(errorHandler);

    // Create and set Http ServerConnector
    ServerConnector httpConnector =
        createHttpServerConnector(
            server,
            serverConfig.getRequestHeaderSize(),
            serverConfig.getResponseHeaderSize(),
            serverConfig.getHost(),
            serverConfig.getHttpPort(),
            serverConfig.getIdleTimeout());
    server.addConnector(httpConnector);

    // TODO. Create and set https connector @jerry

    // Initialize ServletContextHandler
    initializeServletContextHandler(server);
  }

  public synchronized void start() throws RuntimeException {
    try {
      server.start();
    } catch (BindException e) {
      LOG.error(
          "Failed to start {} web server on host {} port {}, which is already in use.",
          serverName,
          serverConfig.getHost(),
          serverConfig.getHttpPort(),
          e);
      throw new RuntimeException("Failed to start " + serverName + " web server.", e);

    } catch (Exception e) {
      LOG.error("Failed to start {} web server.", serverName, e);
      throw new RuntimeException("Failed to start " + serverName + " web server.", e);
    }

    LOG.info(
        "{} web server started on host {} port {}.",
        serverName,
        serverConfig.getHost(),
        serverConfig.getHttpPort());
  }

  public synchronized void join() {
    try {
      server.join();
    } catch (InterruptedException e) {
      LOG.info("Interrupted while {} web server is joining.", serverName);
      Thread.currentThread().interrupt();
    }
  }

  public synchronized void stop() {
    if (server != null) {
      try {
        // Referring from Spark's implementation to avoid the issues.
        ThreadPool threadPool = server.getThreadPool();
        if (threadPool instanceof QueuedThreadPool) {
          ((QueuedThreadPool) threadPool).setStopTimeout(0);
        }

        server.stop();

        if (threadPool instanceof LifeCycle) {
          ((LifeCycle) threadPool).stop();
        }

        LOG.info(
            "{} web server stopped on host {} port {}.",
            serverName,
            serverConfig.getHost(),
            serverConfig.getHttpPort());
      } catch (Exception e) {
        // Swallow the exception.
        LOG.warn("Failed to stop {} web server.", serverName, e);
      }

      server = null;
    }
  }

  public void addServlet(Servlet servlet, String pathSpec) {
    servletContextHandler.addServlet(new ServletHolder(servlet), pathSpec);
  }

  public void addFilter(Filter filter, String pathSpec) {
    servletContextHandler.addFilter(
        new FilterHolder(filter), pathSpec, EnumSet.allOf(DispatcherType.class));
  }

  private void initializeServletContextHandler(Server server) {
    this.servletContextHandler = new WebAppContext();

    // Only Gravitino Server need to set war file.
    if (!serverConfig.getWarFile().isEmpty()) {
      // If development mode, you can set war file in the `GRAVITINO_WAR` environment variable.
      String warPath = System.getenv("GRAVITINO_WAR");
      if (warPath == null) {
        // Default deploy mode, read from `gravitino/gravitino-web-{version}.war`
        warPath =
            String.join(File.separator, System.getenv("GRAVITINO_HOME"), serverConfig.getWarFile());
      }

      File warFile = new File(warPath);
      if (warFile.exists()) {
        if (warFile.isDirectory()) {
          // Development mode, read from FS
          servletContextHandler.setResourceBase(warFile.getPath());
        } else {
          // use packaged WAR
          servletContextHandler.setWar(warFile.getAbsolutePath());
          servletContextHandler.setExtractWAR(false);
          try {
            File warTempDirectory = Files.createTempDirectory("GravitinoWar").toFile();
            warTempDirectory.mkdir();
            LOG.info("Gravitino Webapp path: {}", warTempDirectory.getPath());
            servletContextHandler.setTempDirectory(warTempDirectory);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      } else {
        servletContextHandler.setResourceBase("/");
      }
    } else {
      servletContextHandler.setResourceBase("/");
    }

    servletContextHandler.setContextPath("/");
    servletContextHandler.addServlet(DefaultServlet.class, "/");

    HandlerCollection handlers = new HandlerCollection();
    handlers.addHandler(servletContextHandler);

    server.setHandler(handlers);
  }

  private ServerConnector createHttpServerConnector(
      Server server,
      int reqHeaderSize,
      int respHeaderSize,
      String host,
      int port,
      int idleTimeout) {
    HttpConfiguration httpConfig = new HttpConfiguration();
    httpConfig.setRequestHeaderSize(reqHeaderSize);
    httpConfig.setResponseHeaderSize(respHeaderSize);
    httpConfig.setSendServerVersion(true);
    httpConfig.setIdleTimeout(idleTimeout);

    HttpConnectionFactory httpConnectionFactory = new HttpConnectionFactory(httpConfig);
    ServerConnector connector =
        creatorServerConnector(server, new ConnectionFactory[] {httpConnectionFactory});
    connector.setHost(host);
    connector.setPort(port);
    connector.setReuseAddress(true);

    return connector;
  }

  private ServerConnector creatorServerConnector(
      Server server, ConnectionFactory[] connectionFactories) {
    Scheduler serverExecutor =
        new ScheduledExecutorScheduler(serverName + "-webserver-JettyScheduler", true);

    return new ServerConnector(server, null, serverExecutor, null, -1, -1, connectionFactories);
  }

  private ThreadPool createThreadPool(int minThreads, int maxThreads, int threadPoolWorkQueueSize) {

    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    // Use QueuedThreadPool not ExecutorThreadPool to work around the accidental test failures.
    // see https://github.com/datastrato/gravitino/issues/546
    QueuedThreadPool threadPool =
        new QueuedThreadPool(
            maxThreads, minThreads, 60000, new LinkedBlockingQueue(threadPoolWorkQueueSize)) {

          @Override
          public Thread newThread(Runnable runnable) {
            return PrivilegedThreadFactory.newThread(
                () -> {
                  Thread thread = new Thread(runnable);
                  thread.setDaemon(true);
                  thread.setPriority(getThreadsPriority());
                  thread.setName(getName() + "-" + thread.getId());
                  thread.setUncaughtExceptionHandler(
                      (t, throwable) -> {
                        LOG.error("{} uncaught exception:", t.getName(), throwable);
                      });
                  // JettyServer maybe used by Gravitino server and Iceberg REST server with
                  // different classloaders, we use the classloader of current thread to
                  // replace the classloader of QueuedThreadPool.class.
                  // thread.setContextClassLoader(getClass().getClassLoader()) is removed
                  thread.setContextClassLoader(classLoader);
                  return thread;
                });
          }
        };
    threadPool.setName(serverName);
    return threadPool;
  }
}
