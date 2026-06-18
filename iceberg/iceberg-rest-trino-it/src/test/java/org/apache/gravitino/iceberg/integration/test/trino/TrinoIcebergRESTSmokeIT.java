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
package org.apache.gravitino.iceberg.integration.test.trino;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("gravitino-docker-test")
public class TrinoIcebergRESTSmokeIT extends TrinoIcebergRESTAuthorizationITBase {

  // Use a schema name distinct from the other IT classes: they share one PostgreSQL-backed Iceberg
  // catalog (the shared ContainerSuite is intentionally not reset between classes), so a common
  // namespace would collide across classes.
  private static final String SCHEMA = "smoke_db";

  @BeforeAll
  public void setupTrino() throws Exception {
    catalogAsSuper.asSchemas().createSchema(SCHEMA, "", new HashMap<>());
    startTrino();
  }

  @Test
  public void testCreateInsertSelectAsSuperUser() {
    sql(SUPER_CATALOG, "CREATE TABLE " + SCHEMA + ".t1 (id integer, name varchar)");
    sql(SUPER_CATALOG, "INSERT INTO " + SCHEMA + ".t1 VALUES (1, 'a'), (2, 'b')");
    assertEquals(2L, sql(SUPER_CATALOG, "SELECT count(*) FROM " + SCHEMA + ".t1").getOnlyValue());
    sql(SUPER_CATALOG, "DROP TABLE " + SCHEMA + ".t1");
  }
}
