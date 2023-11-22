/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.metrics;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Reporter;
import com.codahale.metrics.jmx.JmxReporter;
import com.datastrato.gravitino.metrics.source.MetricsSource;
import com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MetricsSystem manages the lifecycle of MetricsSources and MetricsReporters. MetricsReporter will
 * report metrics from MetricsSources registered to MetricsSystem.
 */
public class MetricsSystem implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsSystem.class);
  private final String name;
  private final MetricRegistry metricRegistry;
  private HashMap<String, MetricsSource> metricSources = new HashMap<>();
  private List<Reporter> metricsReporters = new LinkedList<>();

  public MetricsSystem() {
    this("");
  }

  public MetricsSystem(String name) {
    this.name = name;
    this.metricRegistry = new MetricRegistry();
  }

  /**
   * Register a metricsSource to MetricsSystem, all metrics belonging to the metricsSource are added
   * to the MetricsSystem. Ideally, dynamic created metrics source like HiveCatalogMetricsSource
   * unregister the old one before register a new one, but the unregister process is async in
   * Caffeine.removalListener, such race condition may happen: 1. HiveCatalogMetricsSource1 is
   * created and registered. 2. HiveCatalog expired and is removed from cache. 3. At the same time,
   * HiveCatalog is recreated triggered by a new request, and a new HiveCatalogMetricsSource, we
   * call it HiveCatalogMetricsSource2, is created and registered. 5. HiveCatalogMetricsSource1 is
   * unregistered.
   *
   * @param metricsSource metricsSource object to unregistered
   */
  public synchronized void register(MetricsSource metricsSource) {
    LOG.info("Register {} to metrics system {}", metricsSource.getMetricsSourceName(), name);
    if (metricSources.containsKey(metricsSource.getMetricsSourceName())) {
      unregister(metricSources.get(metricsSource.getMetricsSourceName()));
    }
    this.metricSources.put(metricsSource.getMetricsSourceName(), metricsSource);
    metricRegistry.register(
        metricsSource.getMetricsSourceName(), metricsSource.getMetricRegistry());
  }

  /**
   * Unregister a metricsSource from MetricsSystem, all metrics belonging to the metricsSource are
   * removed from MetricsSystem
   *
   * @param metricsSource metricsSource object to unregistered
   */
  public synchronized void unregister(MetricsSource metricsSource) {
    MetricsSource oldMetricsSource = metricSources.get(metricsSource.getMetricsSourceName());
    if (oldMetricsSource == null) {
      LOG.info(
          "Unregister {} metrics source failed, it's not registered",
          metricsSource.getMetricsSourceName());
      return;
    }
    if (!oldMetricsSource.equals(metricsSource)) {
      LOG.info(
          "Unregister {} metrics source failed, it's not the same object that registered",
          metricsSource.getMetricsSourceName());
      return;
    }
    this.metricSources.remove(metricsSource.getMetricsSourceName());
    metricRegistry.removeMatching(
        MetricFilter.startsWith(metricsSource.getMetricsSourceName() + "."));
    LOG.info("Unregistered {} from metrics system {}", metricsSource.getMetricsSourceName(), name);
  }

  // We support JMX reporter for now, todo: support more reporters
  private void initAndStartMetricsReporter() {
    JmxReporter jmxReporter = JmxReporter.forRegistry(metricRegistry).build();
    jmxReporter.start();
    metricsReporters.add(jmxReporter);
  }

  public void start() {
    initAndStartMetricsReporter();
  }

  @VisibleForTesting
  public MetricRegistry getMetricRegistry() {
    return metricRegistry;
  }

  @Override
  public void close() {
    this.metricsReporters.forEach(
        reporter -> {
          try {
            reporter.close();
          } catch (IOException exception) {
            LOG.warn("Close metrics reporter failed,", exception);
          }
        });
  }
}
