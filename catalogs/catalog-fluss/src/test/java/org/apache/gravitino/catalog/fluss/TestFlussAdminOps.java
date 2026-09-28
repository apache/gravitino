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

package org.apache.gravitino.catalog.fluss;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.exception.DatabaseNotExistException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.junit.jupiter.api.Test;

class TestFlussAdminOps {

  @Test
  void testDoAsAdminReturnsFutureValue() {
    Admin admin = mock(Admin.class);
    when(admin.listDatabases()).thenReturn(CompletableFuture.completedFuture(List.of("db")));

    FlussAdminOps adminOps = new FlussAdminOps(admin);

    assertEquals(List.of("db"), adminOps.doAsAdmin(Admin::listDatabases));
  }

  @Test
  void testDoAsAdminMapsFutureException() {
    Admin admin = mock(Admin.class);
    CompletableFuture<List<String>> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(new DatabaseNotExistException("missing"));
    when(admin.listDatabases()).thenReturn(failedFuture);

    FlussAdminOps adminOps = new FlussAdminOps(admin);

    assertThrows(
        NoSuchSchemaException.class,
        () ->
            adminOps.doAsAdmin(
                Admin::listDatabases,
                e -> new NoSuchSchemaException(e, "Failed to list databases")));
  }

  @Test
  void testDoAsAdminMapsSynchronousException() {
    Admin admin = mock(Admin.class);
    FlussAdminOps adminOps = new FlussAdminOps(admin);

    assertThrows(
        NoSuchSchemaException.class,
        () ->
            adminOps.doAsAdmin(
                ignored -> {
                  throw new DatabaseNotExistException("missing");
                },
                FlussExceptionConverter.forSchema("db", "Failed to load database")));
  }
}
