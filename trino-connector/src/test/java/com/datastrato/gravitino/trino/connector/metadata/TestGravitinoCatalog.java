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
package com.datastrato.gravitino.trino.connector.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastrato.gravitino.Audit;
import com.datastrato.gravitino.Catalog;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class TestGravitinoCatalog {

  @Test
  public void testGravitinoCatalog() {
    String catalogName = "mock";
    String provider = "hive";
    Catalog mockCatalog =
        mockCatalog(
            catalogName, provider, "test catalog", Catalog.Type.RELATIONAL, Collections.emptyMap());
    GravitinoCatalog catalog = new GravitinoCatalog("test", mockCatalog);
    assertEquals(catalogName, catalog.getName());
    assertEquals(provider, catalog.getProvider());
  }

  public static Catalog mockCatalog(
      String name,
      String provider,
      String comments,
      Catalog.Type type,
      Map<String, String> properties) {
    Catalog mockCatalog = mock(Catalog.class);
    when(mockCatalog.name()).thenReturn(name);
    when(mockCatalog.provider()).thenReturn(provider);
    when(mockCatalog.comment()).thenReturn(comments);
    when(mockCatalog.type()).thenReturn(type);
    when(mockCatalog.properties()).thenReturn(properties);

    Audit mockAudit = mock(Audit.class);
    when(mockAudit.creator()).thenReturn("gravitino");
    when(mockAudit.createTime()).thenReturn(Instant.now());
    when(mockCatalog.auditInfo()).thenReturn(mockAudit);
    return mockCatalog;
  }
}
