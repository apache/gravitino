/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web;

import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
import org.eclipse.jetty.servlet.ServletContextHandler;
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

  private ServletContextHandler servletContextHandler;

  private JettyServerConfig serverConfig;

  private String serverName;

  public JettyServer() {}

  public synchronized void initialize(
      JettyServerConfig serverConfig, String serverName, boolean shouldEnableUI) {
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

    // Initialize ServletContextHandler or WebAppContext
    if (shouldEnableUI) {
      initializeWebAppContext();
    } else {
      initializeServletContextHandler();
    }
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

  private void initializeServletContextHandler() {
    servletContextHandler = new ServletContextHandler();
    servletContextHandler.setContextPath("/");
    servletContextHandler.addServlet(DefaultServlet.class, "/");

    HandlerCollection handlers = new HandlerCollection();
    handlers.addHandler(servletContextHandler);

    server.setHandler(handlers);
  }

  private void initializeWebAppContext() {
    servletContextHandler = new WebAppContext();

    boolean isUnitTest = System.getenv("GRAVITINO_TEST") != null;

    // If in development/test mode, you can set `war` file or `web/dist` directory in the
    // `GRAVITINO_WAR` environment variable.
    String warPath = System.getenv("GRAVITINO_WAR") != null ? System.getenv("GRAVITINO_WAR") : "";
    if (warPath.isEmpty()) {
      // Default deploy mode, read from `gravitino-${version}/web/gravitino-web.war`
      String webPath = String.join(File.separator, System.getenv("GRAVITINO_HOME"), "web");

      try (DirectoryStream<Path> paths =
          Files.newDirectoryStream(Paths.get(webPath), "gravitino-web-*.war")) {
        int warCount = 0;
        for (Path path : paths) {
          warPath = path.toString();
          warCount++;
        }
        if (warCount != 1 && !isUnitTest) {
          throw new RuntimeException("Found multiple or no war files in the web path : " + webPath);
        }
      } catch (IOException e) {
        throw new RuntimeException("Failed to find war file in the web path : " + webPath, e);
      }
    }

    File warFile = new File(warPath);
    if (!warFile.exists()) {
      // Check war file if exists
      if (isUnitTest) {
        // In development/test mode, We don't have web files in the unit test, so only RESTful API
        // are supported
        servletContextHandler.setResourceBase("/");
      } else {
        // In deployment mode, war files must be available or an exception is thrown
        throw new RuntimeException("Gravitino web path not found in " + warPath);
      }
    }

    if (warFile.isDirectory()) {
      // Development mode, read from FS
      servletContextHandler.setResourceBase(warFile.getPath());
      servletContextHandler.setContextPath("/");
    } else {
      // use packaged WAR
      ((WebAppContext) servletContextHandler).setWar(warFile.getAbsolutePath());
      ((WebAppContext) servletContextHandler).setExtractWAR(false);
      try {
        File warTempDirectory = Files.createTempDirectory("GravitinoWar").toFile();
        LOG.info("Gravitino Webapp path: {}", warTempDirectory.getPath());
        ((WebAppContext) servletContextHandler).setTempDirectory(warTempDirectory);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

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
