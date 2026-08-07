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
package org.apache.gravitino.rel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.rel.metric.Dataset;
import org.apache.gravitino.rel.metric.SemanticModel;
import org.junit.jupiter.api.Test;

public class TestViewCatalog {

  @Test
  public void testCreateMetricViewDelegatesToCreateView() {
    CapturingViewCatalog catalog = new CapturingViewCatalog();
    MetricRepresentation representation = testMetricRepresentation();
    NameIdentifier ident = NameIdentifier.of("mart", "sales_metrics");

    catalog.createMetricView(
        ident, "Sales metrics", representation, Collections.singletonMap("owner", "finance"));

    assertEquals(ident, catalog.ident);
    assertEquals(0, catalog.columns.length);
    assertEquals(representation, catalog.representations[0]);
    assertNull(catalog.defaultCatalog);
    assertNull(catalog.defaultSchema);
  }

  @Test
  public void testCreateMetricViewRejectsNullRepresentation() {
    CapturingViewCatalog catalog = new CapturingViewCatalog();

    assertThrows(
        IllegalArgumentException.class,
        () ->
            catalog.createMetricView(
                NameIdentifier.of("mart", "sales_metrics"), null, null, Collections.emptyMap()));
  }

  private static MetricRepresentation testMetricRepresentation() {
    Dataset dataset =
        Dataset.builder()
            .withName("orders")
            .withSource(NameIdentifier.of("sales", "mart", "orders"))
            .build();
    SemanticModel model =
        SemanticModel.builder()
            .withName("sales")
            .withDatasets(Collections.singletonList(dataset))
            .build();
    return MetricRepresentation.builder().withSemanticModel(model).build();
  }

  private static final class CapturingViewCatalog implements ViewCatalog {
    private NameIdentifier ident;
    private Column[] columns;
    private Representation[] representations;
    private String defaultCatalog;
    private String defaultSchema;

    @Override
    public View loadView(NameIdentifier ident) {
      return null;
    }

    @Override
    public View createView(
        NameIdentifier ident,
        String comment,
        Column[] columns,
        Representation[] representations,
        String defaultCatalog,
        String defaultSchema,
        Map<String, String> properties) {
      this.ident = ident;
      this.columns = columns;
      this.representations = representations;
      this.defaultCatalog = defaultCatalog;
      this.defaultSchema = defaultSchema;
      return null;
    }
  }
}
