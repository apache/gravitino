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
package org.apache.gravitino.iceberg.common.ops;

import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestConfigIcebergTableOpsProvider {

  @ParameterizedTest
  @ValueSource(strings = {"", "hello", "\\\n\t\\\'", "\u0024", "\100", "[_~"})
  public void testValidIcebergTableOps(String prefix) {
    Map<String, String> config = Maps.newHashMap();
    config.put(String.format("catalog.%s.catalog-backend-name", prefix), prefix);
    ConfigIcebergTableOpsProvider provider = new ConfigIcebergTableOpsProvider();
    provider.initialize(config);

    IcebergTableOps ops = provider.getIcebergTableOps(prefix);

    if (StringUtils.isBlank(prefix)) {
      Assertions.assertEquals(ops.catalog.name(), "memory");
    } else {
      Assertions.assertEquals(ops.catalog.name(), prefix);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"hello", "\\\n\t\\\'", "\u0024", "\100", "[_~"})
  public void testInvalidIcebergTableOps(String prefix) {
    ConfigIcebergTableOpsProvider provider = new ConfigIcebergTableOpsProvider();
    provider.initialize(Maps.newHashMap());

    Assertions.assertThrowsExactly(
        RuntimeException.class, () -> provider.getIcebergTableOps(prefix));
  }
}
