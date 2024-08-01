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
package org.apache.gravitino.flink.connector.utils;

import static org.apache.flink.table.factories.FactoryUtil.validateFactoryOptions;
import static org.apache.flink.table.factories.FactoryUtil.validateWatermarkOptions;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.factories.FactoryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FactoryUtils {

  private FactoryUtils() {}

  private static final Logger LOG = LoggerFactory.getLogger(FactoryUtils.class);

  /**
   * Utility for working with {@link Factory}s. The {@link GravitinoCatalogFactoryHelper} override
   * the {@link FactoryUtil.CatalogFactoryHelper#validate()} method to validate the options. For the
   * unconsumed option keys, it logs a warning instead of throwing an exception.
   */
  public static class GravitinoCatalogFactoryHelper extends FactoryUtil.CatalogFactoryHelper {

    private GravitinoCatalogFactoryHelper(
        CatalogFactory catalogFactory, CatalogFactory.Context context) {
      super(catalogFactory, context);
    }

    @Override
    public void validate() {
      validateFactoryOptions(factory, allOptions);
      ignoreUnconsumedKeys(
          factory.factoryIdentifier(),
          allOptions.keySet(),
          consumedOptionKeys,
          deprecatedOptionKeys);
      validateWatermarkOptions(factory.factoryIdentifier(), allOptions);
    }
  }

  /**
   * Validates unconsumed option keys. Logs a warning for each unconsumed option key instead of
   * throwing an exception.
   */
  private static void ignoreUnconsumedKeys(
      String factoryIdentifier,
      Set<String> allOptionKeys,
      Set<String> consumedOptionKeys,
      Set<String> deprecatedOptionKeys) {
    final Set<String> remainingOptionKeys = new HashSet<>(allOptionKeys);
    remainingOptionKeys.removeAll(consumedOptionKeys);
    if (!remainingOptionKeys.isEmpty()) {
      LOG.warn(
          "Unsupported options found for '{}'.\n\n"
              + "Unsupported options that will be ignored:\n\n"
              + "{}\n\n"
              + "Supported options:\n\n"
              + "{}",
          factoryIdentifier,
          remainingOptionKeys.stream().sorted().collect(Collectors.joining("\n")),
          consumedOptionKeys.stream()
              .map(
                  k -> {
                    if (deprecatedOptionKeys.contains(k)) {
                      return String.format("%s (deprecated)", k);
                    }
                    return k;
                  })
              .sorted()
              .collect(Collectors.joining("\n")));
    }
  }

  /**
   * Creates a utility that helps to validate options for a {@link CatalogFactory}.
   *
   * <p>Note: This utility checks for left-over options in the final step.
   */
  public static FactoryUtil.CatalogFactoryHelper createCatalogFactoryHelper(
      CatalogFactory factory, CatalogFactory.Context context) {
    return new FactoryUtils.GravitinoCatalogFactoryHelper(factory, context);
  }
}
