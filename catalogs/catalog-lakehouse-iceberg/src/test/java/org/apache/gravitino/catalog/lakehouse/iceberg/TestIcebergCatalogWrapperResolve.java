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
package org.apache.gravitino.catalog.lakehouse.iceberg;

import java.lang.reflect.Field;
import java.util.Arrays;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.ops.CatalogWrapperManager;
import org.apache.gravitino.iceberg.common.ops.IcebergCatalogWrapper;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.iceberg.service.provider.IcebergConfigProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestIcebergCatalogWrapperResolve {

  @AfterEach
  void tearDown() throws Exception {
    resetIcebergRESTServerContext();
  }

  @Test
  void catalogWrapperUsesIrcManagerWhenAux() throws Exception {
    IcebergCatalogWrapper shared = new IcebergCatalogWrapper(new IcebergConfig());
    CatalogWrapperManager manager = Mockito.mock(CatalogWrapperManager.class);
    Mockito.when(manager.getCatalogWrapper("c1")).thenReturn(shared);

    IcebergConfigProvider configProvider = Mockito.mock(IcebergConfigProvider.class);
    Mockito.when(configProvider.getMetalakeName()).thenReturn("ml");
    Mockito.when(configProvider.getDefaultCatalogName()).thenReturn("c1");
    IcebergRESTServerContext.create(configProvider, false, true, false, manager);

    IcebergCatalogOperations ops = new IcebergCatalogOperations();
    setField(ops, "catalogName", "c1");
    setField(ops, "useIrcCatalogWrapper", true);

    Assertions.assertSame(shared, ops.catalogWrapper());
    Mockito.verify(manager).getCatalogWrapper("c1");
  }

  @Test
  void catalogWrapperUsesLocalWhenNotAux() throws Exception {
    IcebergCatalogWrapper local = new IcebergCatalogWrapper(new IcebergConfig());
    IcebergCatalogOperations ops = new IcebergCatalogOperations();
    ops.icebergCatalogWrapper = local;
    setField(ops, "useIrcCatalogWrapper", false);

    Assertions.assertSame(local, ops.catalogWrapper());
  }

  private static void setField(Object target, String name, Object value) throws Exception {
    Field field = IcebergCatalogOperations.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(target, value);
  }

  private static void resetIcebergRESTServerContext() throws Exception {
    Class<?> holder =
        Arrays.stream(IcebergRESTServerContext.class.getDeclaredClasses())
            .filter(c -> c.getSimpleName().equals("InstanceHolder"))
            .findFirst()
            .orElseThrow();
    Field instance = holder.getDeclaredField("INSTANCE");
    instance.setAccessible(true);
    instance.set(null, null);
  }
}
