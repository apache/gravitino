/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.server.web;

import com.codahale.metrics.Clock;
import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import com.codahale.metrics.jersey2.InstrumentedResourceMethodApplicationListener;
import com.datastrato.gravitino.metrics.MetricNames;
import com.datastrato.gravitino.metrics.source.MetricsSource;
import java.util.concurrent.TimeUnit;
import org.glassfish.jersey.server.ResourceConfig;

public class HttpServerMetricsSource extends MetricsSource {
  public HttpServerMetricsSource(String name, ResourceConfig resourceConfig, JettyServer server) {
    super(name);
    resourceConfig.register(
        new InstrumentedResourceMethodApplicationListener(
            getMetricRegistry(),
            Clock.defaultClock(),
            false,
            () ->
                new SlidingTimeWindowArrayReservoir(
                    getTimeSlidingWindowSeconds(), TimeUnit.SECONDS)));
    registerGauge(
        MetricNames.SERVER_IDLE_THREAD_NUM, () -> server.getThreadPool().getIdleThreads());
  }
}
