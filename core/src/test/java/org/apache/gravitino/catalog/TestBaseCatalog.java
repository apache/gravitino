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

package org.apache.gravitino.catalog;

import com.google.common.collect.ImmutableMap;
import org.apache.gravitino.TestCatalog;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.TestCatalogOperations;
import org.apache.gravitino.meta.CatalogEntity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestBaseCatalog {

  @Test
  void testCustomCatalogOperations() {
    CatalogEntity entity = Mockito.mock(CatalogEntity.class);

    TestCatalog catalog =
        new TestCatalog().withCatalogConf(ImmutableMap.of()).withCatalogEntity(entity);
    CatalogOperations testCatalogOperations = catalog.ops();
    Assertions.assertTrue(testCatalogOperations instanceof TestCatalogOperations);

    TestCatalog catalog2 =
        new TestCatalog()
            .withCatalogConf(
                ImmutableMap.of(
                    BaseCatalog.CATALOG_OPERATION_IMPL, DummyCatalogOperations.class.getName()))
            .withCatalogEntity(entity);
    CatalogOperations dummyCatalogOperations = catalog2.ops();
    Assertions.assertTrue(dummyCatalogOperations instanceof DummyCatalogOperations);
  }
}
