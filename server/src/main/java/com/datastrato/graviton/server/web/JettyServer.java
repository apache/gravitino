package com.datastrato.graviton.server.web;

import com.datastrato.graviton.Config;
import com.datastrato.graviton.server.GravitonServerException;
import com.datastrato.graviton.server.ServerConfig;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.net.BindException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.servlet.Servlet;
import org.eclipse.jetty.server.*;
import org.eclipse.jetty.server.handler.ErrorHandler;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.component.LifeCycle;
import org.eclipse.jetty.util.thread.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JettyServer {

  private static final Logger LOG = LoggerFactory.getLogger(JettyServer.class);

  private Server server;

  private String host;

  private int httpPort;

  private ServletContextHandler servletContextHandler;

  public JettyServer() {}

  public synchronized void initialize(Config config) {
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

    // Create and set Http ServerConnector
    int reqHeaderSize = config.get(ServerConfig.WEBSERVER_REQUEST_HEADER_SIZE);
    int respHeaderSize = config.get(ServerConfig.WEBSERVER_RESPONSE_HEADER_SIZE);
    host = config.get(ServerConfig.WEBSERVER_HOST);
    httpPort = config.get(ServerConfig.WEBSERVER_HTTP_PORT);
    ServerConnector httpConnector =
        createHttpServerConnector(server, reqHeaderSize, respHeaderSize, host, httpPort);
    server.addConnector(httpConnector);

    // TODO. Create and set https connector @jerry

    // Initialize ServletContextHandler
    initializeServletContextHandler(server);
  }

  public synchronized void start() throws GravitonServerException {
    try {
      server.start();
    } catch (BindException e) {
      LOG.error(
          "Failed to start web server on host {} port {}, which is already in use.",
          host,
          httpPort,
          e);
      throw new GravitonServerException("Failed to start web server.", e);

    } catch (Exception e) {
      LOG.error("Failed to start web server.", e);
      throw new GravitonServerException("Failed to start web server.", e);
    }

    LOG.info("Graviton web server started on host {} port {}.", host, httpPort);
  }

  public synchronized void join() {
    try {
      server.join();
    } catch (InterruptedException e) {
      LOG.info("Interrupted while web server is joining.");
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

        LOG.info("Graviton web server stopped on host {} port {}.", host, httpPort);
      } catch (Exception e) {
        // Swallow the exception.
        LOG.warn("Failed to stop web server.", e);
      }

      server = null;
    }
  }

  public void addServlet(Servlet servlet, String pathSpec) {
    servletContextHandler.addServlet(new ServletHolder(servlet), pathSpec);
  }

  private void initializeServletContextHandler(Server server) {
    this.servletContextHandler = new ServletContextHandler();
    servletContextHandler.setContextPath("/");
    servletContextHandler.addServlet(DefaultServlet.class, "/");

    HandlerCollection handlers = new HandlerCollection();
    handlers.addHandler(servletContextHandler);

    server.setHandler(handlers);
  }

  private ServerConnector createHttpServerConnector(
      Server server, int reqHeaderSize, int respHeaderSize, String host, int port) {
    HttpConfiguration httpConfig = new HttpConfiguration();
    httpConfig.setRequestHeaderSize(reqHeaderSize);
    httpConfig.setResponseHeaderSize(respHeaderSize);
    httpConfig.setSendServerVersion(true);

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
        new ScheduledExecutorScheduler("graviton-webserver-JettyScheduler", true);

    return new ServerConnector(server, null, serverExecutor, null, -1, -1, connectionFactories);
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
                .build()));
  }
}
