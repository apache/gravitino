/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.gravitino.catalog.lakehouse.generic;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LakehouseTableDelegatorFactory {
  private static final Logger LOG = LoggerFactory.getLogger(LakehouseTableDelegatorFactory.class);

  private static ImmutableMap<String, LakehouseTableDelegator> delegators;

  private LakehouseTableDelegatorFactory() {}

  private static synchronized void createTableDelegators() {
    if (delegators != null) {
      return;
    }

    ClassLoader cl =
        Optional.ofNullable(Thread.currentThread().getContextClassLoader())
            .orElse(LakehouseTableDelegator.class.getClassLoader());
    ServiceLoader<LakehouseTableDelegator> loader =
        ServiceLoader.load(LakehouseTableDelegator.class, cl);
    delegators =
        ImmutableMap.copyOf(
            loader.stream()
                .collect(
                    Collectors.toMap(
                        provider -> provider.get().tableFormat(), ServiceLoader.Provider::get)));
    Preconditions.checkArgument(
        !delegators.isEmpty(),
        "No LakehouseTableDelegator implementation found via ServiceLoader, this is unexpected, "
            + "please check the code to see what is going on.");

    LOG.info("Loaded LakehouseTableDelegators: {}", delegators.keySet());
  }

  public static ImmutableMap<String, LakehouseTableDelegator> tableDelegators() {
    // Initialize delegators if not yet done
    createTableDelegators();
    return delegators;
  }
}
