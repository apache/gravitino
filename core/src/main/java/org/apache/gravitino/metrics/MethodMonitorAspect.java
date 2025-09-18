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
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.GravitinoEnv;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Aspect
public class MethodMonitorAspect {

  private static final Logger LOG = LoggerFactory.getLogger(MethodMonitorAspect.class);
  private MetricRegistry metricRegistry;

  public MethodMonitorAspect() {
    MetricsSystem metricsSystem = GravitinoEnv.getInstance().metricsSystem();
    // Metrics System could be null in UT.
    if (metricsSystem != null) {
      this.metricRegistry = metricsSystem.getMetricRegistry();
      LOG.info("MethodMonitorAspect initialized.");
    } else {
      LOG.warn("MetricsSystem is not initialized, MethodMonitorAspect is disabled.");
    }
  }

  @Pointcut("execution(@org.apache.gravitino.metrics.Monitored * *(..))")
  public void monitoredMethods() {}

  @Around("monitoredMethods() && @annotation(monitored)")
  public Object monitorMethod(ProceedingJoinPoint pjp, Monitored monitored) throws Throwable {
    if (metricRegistry == null) {
      return pjp.proceed();
    }

    String baseName = buildBaseName(pjp, monitored);

    Timer timer = metricRegistry.timer(MetricRegistry.name(baseName, "total"));
    Meter successMeter = metricRegistry.meter(MetricRegistry.name(baseName, "success"));
    Meter failureMeter = metricRegistry.meter(MetricRegistry.name(baseName, "failure"));

    try (Timer.Context ignore = timer.time()) {
      Object result = pjp.proceed();
      successMeter.mark();
      return result;
    } catch (Throwable t) {
      failureMeter.mark();
      throw t;
    }
  }

  private String buildBaseName(ProceedingJoinPoint pjp, Monitored monitored) {
    String prefix = monitored.prefix();
    if (StringUtils.isNotBlank(prefix)) {
      return prefix;
    }
    return MetricRegistry.name(pjp.getSignature().getDeclaringType(), pjp.getSignature().getName());
  }
}
