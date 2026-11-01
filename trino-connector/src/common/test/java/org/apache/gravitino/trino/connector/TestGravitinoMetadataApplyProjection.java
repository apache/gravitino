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
package org.apache.gravitino.trino.connector;

import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorMetadata;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import org.junit.jupiter.api.Test;

/**
 * Tests that {@link GravitinoMetadata#applyProjection} hands back the types the engine assigned to
 * the projected columns, rather than the types of the internal connector's column handles.
 */
public class TestGravitinoMetadataApplyProjection {

  private static final Type UNBOUNDED_VARCHAR = createUnboundedVarcharType();
  private static final Type BOUNDED_VARCHAR = createVarcharType(255);

  @Test
  public void testEngineTypeIsRestoredForProjectedColumn() {
    ColumnHandle internalColumnHandle = mock(ColumnHandle.class);
    Fixture fixture = new Fixture();
    fixture.declareColumn("col_tinytext", internalColumnHandle);
    fixture.internalReturns(
        List.of(new Variable("col_tinytext", UNBOUNDED_VARCHAR)),
        List.of(new Assignment("col_tinytext", internalColumnHandle, BOUNDED_VARCHAR)));

    List<Assignment> assignments =
        fixture.applyProjection(List.of(new Variable("col_tinytext", UNBOUNDED_VARCHAR)));

    assertEquals(1, assignments.size());
    assertEquals(UNBOUNDED_VARCHAR, assignments.get(0).getType());
    assertEquals("col_tinytext", assignments.get(0).getVariable());
  }

  @Test
  public void testEngineTypeIsRestoredForRenamedColumn() {
    ColumnHandle internalColumnHandle = mock(ColumnHandle.class);
    Fixture fixture = new Fixture();
    fixture.declareColumn("col_tinytext", internalColumnHandle);
    // The internal connector renames the variable (e.g. to avoid a name collision) but keeps the
    // same underlying column handle, matching the "col_tinytext_7" symbol from issue #12518.
    fixture.internalReturns(
        List.of(new Variable("col_tinytext_7", BOUNDED_VARCHAR)),
        List.of(new Assignment("col_tinytext_7", internalColumnHandle, BOUNDED_VARCHAR)));

    List<Assignment> assignments =
        fixture.applyProjection(List.of(new Variable("col_tinytext", UNBOUNDED_VARCHAR)));

    assertEquals(1, assignments.size());
    assertEquals(UNBOUNDED_VARCHAR, assignments.get(0).getType());
    assertEquals("col_tinytext_7", assignments.get(0).getVariable());
  }

  @Test
  public void testEngineTypeIsRestoredForColumnNestedInExpression() {
    ColumnHandle internalColumnHandle = mock(ColumnHandle.class);
    Fixture fixture = new Fixture();
    fixture.declareColumn("col_text", internalColumnHandle);
    fixture.internalReturns(
        List.of(new Variable("col_text", UNBOUNDED_VARCHAR)),
        List.of(new Assignment("col_text", internalColumnHandle, createVarcharType(65535))));

    // The column only appears as an argument of a function call, never on its own.
    ConnectorExpression call =
        new Call(
            UNBOUNDED_VARCHAR,
            new FunctionName("lower"),
            List.of(new Variable("col_text", UNBOUNDED_VARCHAR)));

    List<Assignment> assignments = fixture.applyProjection(List.of(call));

    assertEquals(1, assignments.size());
    assertEquals(UNBOUNDED_VARCHAR, assignments.get(0).getType());
  }

  @Test
  public void testSyntheticColumnKeepsInternalType() {
    ColumnHandle internalColumnHandle = mock(ColumnHandle.class);
    Fixture fixture = new Fixture();
    // A column synthesized by the internal connector has no counterpart among the engine variables.
    fixture.declareColumn("expr_1", internalColumnHandle);
    fixture.internalReturns(
        List.of(new Variable("expr_1", BOUNDED_VARCHAR)),
        List.of(new Assignment("expr_1", internalColumnHandle, BOUNDED_VARCHAR)));

    List<Assignment> assignments =
        fixture.applyProjection(List.of(new Variable("col_tinytext", UNBOUNDED_VARCHAR)));

    assertEquals(1, assignments.size());
    assertEquals(BOUNDED_VARCHAR, assignments.get(0).getType());
  }

  @Test
  public void testSynthesizedColumnReusingAnEngineNameKeepsInternalType() {
    ColumnHandle inputHandle = mock(ColumnHandle.class);
    ColumnHandle synthesizedHandle = mock(ColumnHandle.class);
    Fixture fixture = new Fixture();
    fixture.declareColumn("expr_1", inputHandle);
    fixture.declareColumn("expr_1_synthesized", synthesizedHandle);
    // The internal connector reuses the engine variable name for a column it synthesized itself.
    fixture.internalReturns(
        List.of(new Variable("expr_1", BOUNDED_VARCHAR)),
        List.of(new Assignment("expr_1", synthesizedHandle, BOUNDED_VARCHAR)));

    List<Assignment> assignments =
        fixture.applyProjection(List.of(new Variable("expr_1", UNBOUNDED_VARCHAR)));

    assertEquals(1, assignments.size());
    assertEquals(BOUNDED_VARCHAR, assignments.get(0).getType());
  }

  @Test
  public void testEmptyResultIsPassedThrough() {
    Fixture fixture = new Fixture();
    when(fixture.internalMetadata.applyProjection(
            any(ConnectorSession.class), any(ConnectorTableHandle.class), anyList(), anyMap()))
        .thenReturn(Optional.empty());

    assertFalse(
        fixture
            .metadata
            .applyProjection(
                fixture.session,
                fixture.tableHandle,
                List.of(new Variable("col_tinytext", UNBOUNDED_VARCHAR)),
                Map.of())
            .isPresent());
  }

  private static final class Fixture {
    private final ConnectorMetadata internalMetadata = mock(ConnectorMetadata.class);
    private final ConnectorSession session = mock(ConnectorSession.class);
    private final ConnectorTableHandle internalTableHandle = mock(ConnectorTableHandle.class);
    private final GravitinoTableHandle tableHandle;
    private final GravitinoMetadata metadata;
    private final Map<String, ColumnHandle> assignments = new HashMap<>();

    private Fixture() {
      tableHandle = new GravitinoTableHandle("test_schema", "test_table", internalTableHandle);
      metadata =
          new GravitinoMetadata(
              mock(CatalogConnectorMetadata.class),
              mock(CatalogConnectorMetadataAdapter.class),
              internalMetadata) {};
    }

    private void declareColumn(String columnName, ColumnHandle internalColumnHandle) {
      when(internalMetadata.getColumnMetadata(session, internalTableHandle, internalColumnHandle))
          .thenReturn(new ColumnMetadata(columnName, UNBOUNDED_VARCHAR));
      assignments.put(columnName, new GravitinoColumnHandle(columnName, internalColumnHandle));
    }

    private void internalReturns(List<ConnectorExpression> projections, List<Assignment> results) {
      when(internalMetadata.applyProjection(
              any(ConnectorSession.class), any(ConnectorTableHandle.class), anyList(), anyMap()))
          .thenReturn(
              Optional.of(
                  new ProjectionApplicationResult<>(
                      internalTableHandle, projections, results, false)));
    }

    private List<Assignment> applyProjection(List<ConnectorExpression> projections) {
      Optional<ProjectionApplicationResult<ConnectorTableHandle>> result =
          metadata.applyProjection(session, tableHandle, projections, assignments);
      assertTrue(result.isPresent());
      return result.get().getAssignments();
    }
  }
}
