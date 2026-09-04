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

import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.utils.ThrowableFunction;
import org.mockito.Mockito;

/** Test-only helpers for catalog internals that are package-private in production code. */
public final class CatalogTestUtils {

  private CatalogTestUtils() {}

  /**
   * Stubs a mocked manager so {@link CatalogManager#doWithCatalog} invokes its callback with the
   * supplied catalog.
   *
   * @param catalogManager the mocked catalog manager.
   * @param catalog the live catalog to pass to callbacks.
   */
  @SuppressWarnings("unchecked")
  public static void mockDoWithCatalog(CatalogManager catalogManager, BaseCatalog<?> catalog) {
    Mockito.doAnswer(
            invocation -> {
              ThrowableFunction<BaseCatalog, Object> operation = invocation.getArgument(1);
              return operation.apply(catalog);
            })
        .when(catalogManager)
        .doWithCatalog(Mockito.any(), Mockito.any());
  }
}
