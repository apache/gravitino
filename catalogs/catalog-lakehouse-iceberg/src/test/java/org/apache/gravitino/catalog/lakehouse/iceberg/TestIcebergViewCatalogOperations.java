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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NoSuchViewException;
import org.apache.gravitino.iceberg.common.ops.IcebergCatalogWrapper;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ServiceFailureException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestIcebergViewCatalogOperations {

  @Test
  public void testLoadViewTranslatesOnlyNoSuchViewException() {
    IcebergCatalogWrapper wrapper = Mockito.mock(IcebergCatalogWrapper.class);
    IcebergViewCatalogOperations operations = new IcebergViewCatalogOperations(wrapper);

    NameIdentifier ident = NameIdentifier.of("schema1", "view1");
    when(wrapper.loadView(any(TableIdentifier.class)))
        .thenThrow(new org.apache.iceberg.exceptions.NoSuchViewException("view missing"));

    Assertions.assertThrows(NoSuchViewException.class, () -> operations.loadView(ident));
  }

  @Test
  public void testLoadViewPropagatesNonNoSuchViewException() {
    IcebergCatalogWrapper wrapper = Mockito.mock(IcebergCatalogWrapper.class);
    IcebergViewCatalogOperations operations = new IcebergViewCatalogOperations(wrapper);

    NameIdentifier ident = NameIdentifier.of("schema1", "view1");
    when(wrapper.loadView(any(TableIdentifier.class)))
        .thenThrow(new ServiceFailureException("backend unavailable"));

    Assertions.assertThrows(ServiceFailureException.class, () -> operations.loadView(ident));
  }

  @Test
  public void testViewExistsDelegatesToWrapper() {
    IcebergCatalogWrapper wrapper = Mockito.mock(IcebergCatalogWrapper.class);
    IcebergViewCatalogOperations operations = new IcebergViewCatalogOperations(wrapper);

    NameIdentifier ident = NameIdentifier.of("schema1", "view1");
    when(wrapper.viewExists(any(TableIdentifier.class))).thenReturn(true);

    Assertions.assertTrue(operations.viewExists(ident));
    verify(wrapper).viewExists(any(TableIdentifier.class));
  }
}
