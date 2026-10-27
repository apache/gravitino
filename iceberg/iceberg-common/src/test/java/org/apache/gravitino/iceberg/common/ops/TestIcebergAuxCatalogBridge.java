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

import java.util.Optional;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergAuxCatalogBridge {

  @AfterEach
  void tearDown() {
    IcebergAuxCatalogBridge.clear();
  }

  @Test
  void resolveEmptyWhenUnregistered() {
    Assertions.assertFalse(IcebergAuxCatalogBridge.isRegistered());
    Assertions.assertTrue(IcebergAuxCatalogBridge.resolve("c1").isEmpty());
  }

  @Test
  void resolveReturnsRegisteredWrapper() {
    IcebergCatalogWrapper wrapper = new IcebergCatalogWrapper(new IcebergConfig());
    IcebergAuxCatalogBridge.register(
        name -> {
          if ("c1".equals(name)) {
            return wrapper;
          }
          throw new NoSuchCatalogException("missing %s", name);
        });

    Assertions.assertTrue(IcebergAuxCatalogBridge.isRegistered());
    Optional<IcebergCatalogWrapper> resolved = IcebergAuxCatalogBridge.resolve("c1");
    Assertions.assertTrue(resolved.isPresent());
    Assertions.assertSame(wrapper, resolved.get());
  }

  @Test
  void resolveEmptyOnNoSuchCatalog() {
    IcebergAuxCatalogBridge.register(
        name -> {
          throw new NoSuchCatalogException("missing %s", name);
        });
    Assertions.assertTrue(IcebergAuxCatalogBridge.resolve("unknown").isEmpty());
  }

  @Test
  void clearUnregisters() {
    IcebergAuxCatalogBridge.register(name -> new IcebergCatalogWrapper(new IcebergConfig()));
    IcebergAuxCatalogBridge.clear();
    Assertions.assertFalse(IcebergAuxCatalogBridge.isRegistered());
    Assertions.assertTrue(IcebergAuxCatalogBridge.resolve("c1").isEmpty());
  }
}
