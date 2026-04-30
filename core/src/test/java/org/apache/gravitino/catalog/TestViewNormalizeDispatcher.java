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
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.SQLRepresentation;
import org.apache.gravitino.rel.View;
import org.apache.gravitino.rel.ViewChange;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestViewNormalizeDispatcher extends TestOperationDispatcher {
  private static ViewNormalizeDispatcher viewNormalizeDispatcher;
  private static SchemaNormalizeDispatcher schemaNormalizeDispatcher;

  @BeforeAll
  public static void initialize() throws IOException, IllegalAccessException {
    TestViewOperationDispatcher.initialize();
    viewNormalizeDispatcher =
        new ViewNormalizeDispatcher(
            TestViewOperationDispatcher.viewOperationDispatcher, catalogManager);
    schemaNormalizeDispatcher =
        new SchemaNormalizeDispatcher(
            TestViewOperationDispatcher.schemaOperationDispatcher, catalogManager);
  }

  @Test
  public void testNameCaseInsensitive() {
    Namespace viewNs = Namespace.of(metalake, catalog, "schemaVIEW_NORMALIZE_1");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    schemaNormalizeDispatcher.createSchema(NameIdentifier.of(viewNs.levels()), "comment", props);

    NameIdentifier viewIdent = NameIdentifier.of(viewNs, "viewNAME");
    Representation[] representations = {
      SQLRepresentation.builder().withDialect("spark").withSql("SELECT 1").build()
    };

    View created =
        viewNormalizeDispatcher.createView(
            viewIdent, "comment", new Column[0], representations, null, null, props);
    Assertions.assertEquals(viewIdent.name().toLowerCase(), created.name());

    // Loading with any case should work and return lower-case.
    View loaded =
        viewNormalizeDispatcher.loadView(NameIdentifier.of(viewNs, viewIdent.name().toUpperCase()));
    Assertions.assertEquals(viewIdent.name().toLowerCase(), loaded.name());

    // Listing returns lowercase names.
    NameIdentifier[] idents = viewNormalizeDispatcher.listViews(viewNs);
    Arrays.stream(idents).forEach(i -> Assertions.assertEquals(i.name().toLowerCase(), i.name()));

    // Altering with mixed case should work.
    View altered =
        viewNormalizeDispatcher.alterView(
            NameIdentifier.of(viewNs, viewIdent.name().toUpperCase()),
            ViewChange.setProperty("k2", "v2"));
    Assertions.assertEquals(viewIdent.name().toLowerCase(), altered.name());

    Assertions.assertTrue(
        viewNormalizeDispatcher.viewExists(
            NameIdentifier.of(viewNs, viewIdent.name().toUpperCase())));

    // Dropping with mixed case should work.
    Assertions.assertTrue(
        viewNormalizeDispatcher.dropView(
            NameIdentifier.of(viewNs, viewIdent.name().toUpperCase())));
  }
}
