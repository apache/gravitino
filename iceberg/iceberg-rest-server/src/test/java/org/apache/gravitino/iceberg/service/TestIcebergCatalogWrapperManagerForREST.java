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
package org.apache.gravitino.iceberg.service;

import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.iceberg.common.ops.IcebergCatalogWrapper;
import org.apache.gravitino.iceberg.service.provider.IcebergConfigProvider;
import org.apache.gravitino.iceberg.service.provider.IcebergConfigProviderFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestIcebergCatalogWrapperManagerForREST {

  private static final String DEFAULT_CATALOG = "memory";

  @ParameterizedTest
  @ValueSource(strings = {"", "hello/", "\\\n\t\\\'/", "\u0024/", "\100/", "[_~/"})
  public void testValidGetOps(String rawPrefix) {
    String prefix = rawPrefix;
    if (!StringUtils.isBlank(rawPrefix)) {
      prefix = rawPrefix.substring(0, rawPrefix.length() - 1);
    }
    Map<String, String> config = Maps.newHashMap();
    config.put(String.format("catalog.%s.catalog-backend-name", prefix), prefix);
    IcebergConfigProvider configProvider = IcebergConfigProviderFactory.create(config);
    configProvider.initialize(config);
    IcebergCatalogWrapperManager manager = new IcebergCatalogWrapperManager(config, configProvider);

    IcebergCatalogWrapper ops = manager.getOps(rawPrefix);

    if (StringUtils.isBlank(prefix)) {
      Assertions.assertEquals(ops.getCatalog().name(), DEFAULT_CATALOG);
    } else {
      Assertions.assertEquals(ops.getCatalog().name(), prefix);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"hello", "\\\n\t\\\'", "\u0024", "\100", "[_~"})
  public void testInvalidGetOps(String rawPrefix) {
    Map<String, String> config = Maps.newHashMap();
    IcebergConfigProvider configProvider = IcebergConfigProviderFactory.create(config);
    configProvider.initialize(config);
    IcebergCatalogWrapperManager manager = new IcebergCatalogWrapperManager(config, configProvider);

    Assertions.assertThrowsExactly(IllegalArgumentException.class, () -> manager.getOps(rawPrefix));
  }
}
