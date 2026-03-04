/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.server.web;

import com.codahale.metrics.Clock;
import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import com.codahale.metrics.jersey2.InstrumentedResourceMethodApplicationListener;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.metrics.source.MetricsSource;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.util.thread.ThreadPool;
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

    // Register QueuedThreadPool specific metrics with instance checks
    ThreadPool threadPool = server.getThreadPool();
    registerGauge(MetricNames.SERVER_IDLE_THREAD_NUM, () -> threadPool.getIdleThreads());
    registerGauge(MetricNames.SERVER_TOTAL_THREAD_NUM, () -> threadPool.getThreads());

    if (threadPool instanceof QueuedThreadPool) {
      QueuedThreadPool queuedThreadPool = (QueuedThreadPool) threadPool;
      registerGauge(MetricNames.SERVER_BUSY_THREAD_NUM, () -> queuedThreadPool.getBusyThreads());
      registerGauge(MetricNames.SERVER_QUEUED_REQUEST_NUM, () -> queuedThreadPool.getQueueSize());
      registerGauge(MetricNames.SERVER_MIN_THREAD_NUM, () -> queuedThreadPool.getMinThreads());
      registerGauge(MetricNames.SERVER_MAX_THREAD_NUM, () -> queuedThreadPool.getMaxThreads());
    }
  }
}
