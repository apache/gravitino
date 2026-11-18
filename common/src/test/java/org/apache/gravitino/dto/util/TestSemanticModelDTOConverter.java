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
package org.apache.gravitino.dto.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.Audit;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.semantic.SemanticModelDTO;
import org.apache.gravitino.semantic.AIContext;
import org.apache.gravitino.semantic.CustomExtension;
import org.apache.gravitino.semantic.Dataset;
import org.apache.gravitino.semantic.DialectExpression;
import org.apache.gravitino.semantic.Dialects;
import org.apache.gravitino.semantic.Expression;
import org.apache.gravitino.semantic.Metric;
import org.apache.gravitino.semantic.Relationship;
import org.apache.gravitino.semantic.SemanticModel;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.junit.jupiter.api.Test;

public class TestSemanticModelDTOConverter {

  @Test
  public void testConvertSemanticModelToDTO() {
    Dataset orders = dataset("orders");
    Dataset customers = dataset("customers");
    Relationship relationship =
        Relationship.builder()
            .withName("orders_to_customers")
            .withFrom("orders")
            .withTo("customers")
            .withFromColumns(new String[] {"customer_id"})
            .withToColumns(new String[] {"id"})
            .build();
    Metric metric =
        Metric.builder()
            .withName("revenue")
            .withExpression(expression("SUM(orders.amount)"))
            .build();
    CustomExtension extension =
        CustomExtension.builder().withVendorName("example").withData("{}").build();
    SemanticModelDefinition definition =
        SemanticModelDefinition.builder()
            .withAIContext(AIContext.of("Use governed metrics"))
            .withDatasets(new Dataset[] {orders, customers})
            .withRelationships(new Relationship[] {relationship})
            .withMetrics(new Metric[] {metric})
            .withCustomExtensions(new CustomExtension[] {extension})
            .build();
    AuditDTO audit = audit();

    SemanticModel semanticModel =
        new SemanticModel() {
          @Override
          public String name() {
            return "sales_model";
          }

          @Override
          @Nullable
          public String comment() {
            return "Governed sales definitions";
          }

          @Override
          public SemanticModelDefinition definition() {
            return definition;
          }

          @Override
          public Map<String, String> properties() {
            return Map.of("owner", "finance");
          }

          @Override
          public Audit auditInfo() {
            return audit;
          }
        };

    SemanticModelDTO dto = DTOConverters.toDTO(semanticModel);

    assertEquals("sales_model", dto.name());
    assertEquals("Governed sales definitions", dto.comment());
    assertEquals(definition, dto.definition());
    assertEquals(Map.of("owner", "finance"), dto.properties());
    assertEquals(audit, dto.auditInfo());
  }

  private static Dataset dataset(String name) {
    return Dataset.builder()
        .withName(name)
        .withSource(NameIdentifier.of("sales", "mart", name))
        .build();
  }

  private static Expression expression(String value) {
    return Expression.builder()
        .withDialects(
            new DialectExpression[] {
              DialectExpression.builder()
                  .withDialect(Dialects.ANSI_SQL)
                  .withExpression(value)
                  .build()
            })
        .build();
  }

  private static AuditDTO audit() {
    return AuditDTO.builder()
        .withCreator("tester")
        .withCreateTime(Instant.parse("2026-08-11T00:00:00Z"))
        .build();
  }
}
