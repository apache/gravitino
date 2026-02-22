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
package org.apache.gravitino.server.web.filter;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.Set;
import org.apache.gravitino.iceberg.service.rest.IcebergNamespaceOperations;
import org.apache.gravitino.iceberg.service.rest.IcebergTableOperations;
import org.apache.gravitino.iceberg.service.rest.IcebergTableRenameOperations;
import org.apache.gravitino.iceberg.service.rest.IcebergViewOperations;
import org.apache.gravitino.iceberg.service.rest.IcebergViewRenameOperations;
import org.glassfish.hk2.api.Filter;
import org.junit.jupiter.api.Test;

/**
 * Verifies that {@link IcebergRESTAuthInterceptionService} registers every Iceberg REST resource
 * class that carries {@code @AuthorizationExpression} annotations.
 *
 * <p>If a class is missing from the HK2 {@link BaseInterceptionService.ClassListFilter} its
 * annotations are silently ignored at runtime — auth is never enforced for any method in that
 * class.
 */
public class TestIcebergRESTAuthInterceptionService {

  /**
   * All Iceberg REST resource classes that carry {@code @AuthorizationExpression} annotations and
   * therefore MUST appear in the descriptor filter. Add new classes here when new resource classes
   * are introduced.
   */
  private static final Class<?>[] EXPECTED_CLASSES = {
    IcebergTableOperations.class,
    IcebergTableRenameOperations.class,
    IcebergNamespaceOperations.class,
    IcebergViewOperations.class,
    IcebergViewRenameOperations.class,
  };

  @Test
  @SuppressWarnings("unchecked")
  public void testAllAnnotatedResourceClassesAreInFilter() throws Exception {
    IcebergRESTAuthInterceptionService service = new IcebergRESTAuthInterceptionService();
    Filter filter = service.getDescriptorFilter();

    // Read ClassListFilter.targetClasses directly — avoids needing HK2 descriptor factories on
    // the test classpath and gives a clear class-name assertion on failure.
    Field field = BaseInterceptionService.ClassListFilter.class.getDeclaredField("targetClasses");
    field.setAccessible(true);
    Set<String> registeredNames = (Set<String>) field.get(filter);

    for (Class<?> clazz : EXPECTED_CLASSES) {
      assertTrue(
          registeredNames.contains(clazz.getName()),
          clazz.getSimpleName()
              + " has @AuthorizationExpression methods but is NOT in"
              + " IcebergRESTAuthInterceptionService.getDescriptorFilter()."
              + " All auth annotations on this class are silently ignored at runtime.");
    }
  }
}
