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
package org.apache.gravitino.trino.connector.system.storedprocedure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;

import io.trino.spi.procedure.Procedure;
import java.util.List;
import java.util.Set;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorManager;
import org.junit.jupiter.api.Test;

public class TestGravitinoStoredProcedureFactory {

  @Test
  public void testProceduresDeclareOptionalMetalakeArgument() {
    // Trino validates a procedure's arguments against its method handle when the catalog loads,
    // so a mismatch surfaces as a failure to load the entry catalog rather than in a call.
    for (String metalake : new String[] {"test", null}) {
      GravitinoStoredProcedureFactory factory =
          new GravitinoStoredProcedureFactory(mock(CatalogConnectorManager.class), metalake);
      Set<Procedure> procedures = factory.getStoredProcedures();

      assertEquals(
          Set.of("create_catalog", "drop_catalog", "alter_catalog"),
          Set.of(procedures.stream().map(Procedure::getName).toArray(String[]::new)));
      for (Procedure procedure : procedures) {
        List<Procedure.Argument> arguments = procedure.getArguments();
        Procedure.Argument last = arguments.get(arguments.size() - 1);
        assertEquals("METALAKE", last.getName(), procedure.getName());
        assertFalse(last.isRequired(), procedure.getName());
        assertNull(last.getDefaultValue(), procedure.getName());
      }
    }
  }
}
