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

package org.apache.gravitino.maintenance.optimizer.common.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
import java.util.List;
import java.util.ServiceLoader;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.maintenance.optimizer.api.monitor.MetricsEvaluator;
import org.apache.gravitino.maintenance.optimizer.api.updater.StatisticsCalculator;

public class InstanceLoaderUtils {

  public static <T extends StatisticsCalculator> T createStatisticsCalculatorInstance(
      String calculatorName) {
    return createInstanceByName(
        StatisticsCalculator.class, calculatorName, StatisticsCalculator::name);
  }

  public static <T extends MetricsEvaluator> T createMetricsEvaluatorInstance(
      String evaluatorName) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(evaluatorName), "metrics evaluator name must not be null or blank");
    return createInstanceByName(MetricsEvaluator.class, evaluatorName, MetricsEvaluator::name);
  }

  private static <B, T extends B> T createInstanceByName(
      Class<B> typeClass, String instanceName, Function<B, String> nameFunction) {
    ServiceLoader<B> loader = ServiceLoader.load(typeClass);
    List<Class<? extends T>> providers =
        Streams.stream(loader.iterator())
            .filter(p -> nameFunction.apply(p).equalsIgnoreCase(instanceName))
            .map(p -> (Class<? extends T>) p.getClass())
            .collect(Collectors.toList());

    if (providers.isEmpty()) {
      throw new IllegalArgumentException(
          "No " + typeClass.getSimpleName() + " class found for: " + instanceName);
    } else if (providers.size() > 1) {
      throw new IllegalArgumentException(
          "Multiple " + typeClass.getSimpleName() + " found for: " + instanceName);
    } else {
      Class<? extends T> providerClz = Iterables.getOnlyElement(providers);
      try {
        return providerClz.getDeclaredConstructor().newInstance();
      } catch (Exception e) {
        throw new RuntimeException(
            "Failed to instantiate "
                + typeClass.getSimpleName()
                + ": "
                + instanceName
                + ", class: "
                + providerClz.getName(),
            e);
      }
    }
  }
}
