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
import com.google.common.collect.ImmutableMap;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import io.prometheus.client.dropwizard.samplebuilder.CustomMappingSampleBuilder;
import io.prometheus.client.dropwizard.samplebuilder.MapperConfig;
import io.prometheus.client.exporter.MetricsServlet;
import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
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
  private CollectorRegistry prometheusRegistry;

  public MetricsSystem() {
    this("");
  }

  public MetricsSystem(String name) {
    this.name = name;
    this.metricRegistry = new MetricRegistry();
    this.prometheusRegistry = new CollectorRegistry();
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
    registerMetricsToPrometheusRegistry();
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

  /*
   * Extract a metric name and label from dropwizard metrics for Prometheus
   *
   * All extract rules must register with the prometheus registry before start prometheus servlet.
   * At this time, some MetricsSource may not register with MetricsSystem, such as
   * HiveCatalogMetricsSource, so we place all rules in MetricsSystem not
   * spread in separate MetricsSources.
   *
   * If a metric name can't apply any rules, it will be constructed to Prometheus metrics
   * name format, "ab.c-a.d" is transformed to "ab_c_a_d"
   *
   * MapperConfig is used to extract Prometheus metricsName and labels with signature:
   * MapperConfig(final String match, final String name, final Map<String, String> labels)
   *
   * Match is regex used to match incoming metric name. It uses a simplified glob syntax where
   * only '*' are allowed. Name is New metric name. Can contain placeholders to be replaced with
   * actual values from the incoming metric name. Placeholders are in the ${n} format where n
   * is the zero based index of the group to extract from the original metric name. Labels are the
   * label to be extracted from, they should contain placeholders too.
   * E.g.:
   * Match: gravitino.dispatcher.*.*
   * Name: dispatcher_events_total_${0}
   * Labels: label1: ${1}_t
   * A metric "gravitino.dispatcher.sp1.yay" will be converted in a new metric with name
   * "dispatcher_events_total_sp1" with label {label1: yay_t}
   * Label names have to match the regex ^[a-zA-Z_][a-zA-Z0-9_]+$
   */
  @VisibleForTesting
  static List<MapperConfig> getMetricNameAndLabelRules() {
    return Arrays.asList(
        new MapperConfig(
            MetricsSource.ICEBERG_REST_SERVER_METRIC_NAME + ".*.*",
            MetricsSource.ICEBERG_REST_SERVER_METRIC_NAME + "_${1}",
            ImmutableMap.of("operation", "${0}")),
        new MapperConfig(
            MetricsSource.GRAVITINO_SERVER_METRIC_NAME + ".*.*",
            MetricsSource.GRAVITINO_SERVER_METRIC_NAME + "_${1}",
            ImmutableMap.of("operation", "${0}")));
  }

  private void registerMetricsToPrometheusRegistry() {
    CustomMappingSampleBuilder sampleBuilder =
        new CustomMappingSampleBuilder(getMetricNameAndLabelRules());
    DropwizardExports dropwizardExports = new DropwizardExports(metricRegistry, sampleBuilder);
    dropwizardExports.register(prometheusRegistry);
  }

  public MetricsServlet getPrometheusServlet() {
    return new MetricsServlet(prometheusRegistry);
  }
}
