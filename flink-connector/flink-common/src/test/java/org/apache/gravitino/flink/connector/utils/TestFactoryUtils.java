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

import com.google.common.collect.ImmutableList;
import java.util.Iterator;
import java.util.ServiceConfigurationError;
import org.apache.flink.table.factories.Factory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestFactoryUtils {

  @Test
  void testBuiltInTypesAreGravitinoManaged() {
    Assertions.assertTrue(FactoryUtils.isGravitinoManagedCatalogType("gravitino-hive"));
    Assertions.assertTrue(FactoryUtils.isGravitinoManagedCatalogType("gravitino-jdbc-mysql"));
  }

  @Test
  void testExternalCatalogFactoryTypeIsGravitinoManaged() {
    // Discovered through META-INF/services on the test classpath. Matching follows Flink's own
    // factory discovery and is case-sensitive.
    Assertions.assertTrue(
        FactoryUtils.isGravitinoManagedCatalogType(FakeExternalCatalogFactory.IDENTIFIER));
    Assertions.assertFalse(
        FactoryUtils.isGravitinoManagedCatalogType(
            FakeExternalCatalogFactory.IDENTIFIER.toUpperCase()));
  }

  @Test
  void testTypeWithoutCatalogFactoryIsNotGravitinoManaged() {
    // No jar on this classpath registers a factory for these types.
    Assertions.assertFalse(FactoryUtils.isGravitinoManagedCatalogType("gravitino-jdbc-oracle"));
    Assertions.assertFalse(FactoryUtils.isGravitinoManagedCatalogType("gravitino-jdbc-custom"));
    Assertions.assertFalse(FactoryUtils.isGravitinoManagedCatalogType(null));
  }

  @Test
  void testFlinkNativeCatalogTypesAreNotGravitinoManaged() {
    // These factories are on the test classpath but are not BaseCatalogFactory.
    Assertions.assertFalse(FactoryUtils.isGravitinoManagedCatalogType("generic_in_memory"));
    Assertions.assertFalse(FactoryUtils.isGravitinoManagedCatalogType("hive"));
  }

  @Test
  void testBrokenServiceEntriesAreSkipped() {
    Iterator<Factory> factories =
        new Iterator<Factory>() {
          private final Iterator<Object> steps =
              ImmutableList.of(
                      new ServiceConfigurationError("missing provider"),
                      new NoClassDefFoundError("missing/Dependency"),
                      new IllegalStateException("factory init failed"),
                      new FakeExternalCatalogFactory())
                  .iterator();

          @Override
          public boolean hasNext() {
            return steps.hasNext();
          }

          @Override
          public Factory next() {
            Object step = steps.next();
            if (step instanceof Error) {
              throw (Error) step;
            }
            if (step instanceof RuntimeException) {
              throw (RuntimeException) step;
            }
            return (Factory) step;
          }
        };
    Assertions.assertTrue(
        FactoryUtils.isProvidedByCatalogFactory(factories, FakeExternalCatalogFactory.IDENTIFIER));
  }

  @Test
  void testBrokenServiceEntryOnHasNextIsSkipped() {
    Iterator<Factory> factories =
        new Iterator<Factory>() {
          private boolean failed = false;

          @Override
          public boolean hasNext() {
            if (!failed) {
              failed = true;
              throw new ServiceConfigurationError("unreadable service file");
            }
            return false;
          }

          @Override
          public Factory next() {
            throw new IllegalStateException();
          }
        };
    Assertions.assertFalse(FactoryUtils.isProvidedByCatalogFactory(factories, "any"));
  }
}
