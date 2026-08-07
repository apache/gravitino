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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.rel.metric.Dataset;
import org.apache.gravitino.rel.metric.SemanticModel;
import org.junit.jupiter.api.Test;

public class TestMetricRepresentation {

  @Test
  public void testBuildMetricRepresentation() {
    SemanticModel model = testModel("sales_semantic_model");
    MetricRepresentation representation =
        MetricRepresentation.builder().withSemanticModel(model).build();

    assertEquals(Representation.TYPE_METRIC, representation.type());
    assertEquals(model, representation.semanticModel());
  }

  @Test
  public void testMetricRepresentationEquality() {
    MetricRepresentation first =
        MetricRepresentation.builder().withSemanticModel(testModel("sales")).build();
    MetricRepresentation second =
        MetricRepresentation.builder().withSemanticModel(testModel("sales")).build();
    MetricRepresentation different =
        MetricRepresentation.builder().withSemanticModel(testModel("inventory")).build();

    assertEquals(first, second);
    assertEquals(first.hashCode(), second.hashCode());
    assertNotEquals(first, different);
  }

  @Test
  public void testRejectMissingMetricRepresentationFields() {
    assertThrows(IllegalArgumentException.class, () -> MetricRepresentation.builder().build());
  }

  private static SemanticModel testModel(String name) {
    Dataset dataset =
        Dataset.builder()
            .withName("orders")
            .withSource(NameIdentifier.of("sales", "mart", "orders"))
            .build();
    return SemanticModel.builder()
        .withName(name)
        .withDatasets(Collections.singletonList(dataset))
        .build();
  }
}
