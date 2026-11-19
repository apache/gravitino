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
package org.apache.gravitino.metrics;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import javax.annotation.Nullable;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.metrics.source.MetricsSource;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Aspect
public class MethodMonitorAspect {

  private static final Logger LOG = LoggerFactory.getLogger(MethodMonitorAspect.class);
  private volatile MetricsSystem metricsSystem;

  public MethodMonitorAspect() {
    this.metricsSystem = GravitinoEnv.getInstance().metricsSystem();
    // Metrics System could be null in UT.
    if (metricsSystem != null) {
      LOG.info("MethodMonitorAspect initialized.");
    } else {
      LOG.warn("MetricsSystem is not initialized, MethodMonitorAspect is disabled.");
    }
  }

  /**
   * Replaces the metrics system used by this aspect. The aspect is a singleton that reads the
   * metrics system only once, when it is created, so tests use this to install their own. Do not
   * call it from production code.
   *
   * @param metricsSystem the metrics system to use, or null to disable monitoring
   * @return the metrics system used before the call, which may be null, so the caller can restore
   *     it
   */
  @VisibleForTesting
  @Nullable
  public MetricsSystem replaceMetricsSystem(@Nullable MetricsSystem metricsSystem) {
    MetricsSystem previous = this.metricsSystem;
    this.metricsSystem = metricsSystem;
    return previous;
  }

  @Pointcut("execution(@org.apache.gravitino.metrics.Monitored * *(..))")
  public void monitoredMethods() {}

  @Around("monitoredMethods() && @annotation(monitored)")
  public Object monitorMethod(ProceedingJoinPoint pjp, Monitored monitored) throws Throwable {
    MetricsSystem currentMetricsSystem = metricsSystem;
    if (currentMetricsSystem == null) {
      return pjp.proceed();
    }

    MetricsSource metricsSource = currentMetricsSystem.getMetricsSource(monitored.metricsSource());
    if (metricsSource == null) {
      LOG.warn(
          "MetricsSource {} is not registered in MetricsSystem, skip monitoring for method {}",
          monitored.metricsSource(),
          pjp.getSignature().toShortString());
      return pjp.proceed();
    }

    String baseMetricName = monitored.baseMetricName();
    Timer timer = metricsSource.getTimer(MetricRegistry.name(baseMetricName, "total"));
    Meter successMeter = metricsSource.getMeter(MetricRegistry.name(baseMetricName, "success"));
    Meter failureMeter = metricsSource.getMeter(MetricRegistry.name(baseMetricName, "failure"));

    try (Timer.Context ignore = timer.time()) {
      Object result = pjp.proceed();
      successMeter.mark();
      return result;
    } catch (Throwable t) {
      failureMeter.mark();
      throw t;
    }
  }
}
