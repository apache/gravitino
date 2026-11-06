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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Schema;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.hook.SchemaHookDispatcher;
import org.apache.gravitino.hook.TableHookDispatcher;
import org.apache.gravitino.hook.ViewHookDispatcher;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.SchemaEventDispatcher;
import org.apache.gravitino.listener.TableEventDispatcher;
import org.apache.gravitino.listener.ViewEventDispatcher;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.View;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests that internal imports preserve audit information through dispatcher decorators. */
public class TestImportDispatcherDelegation {

  private static final NameIdentifier IDENTIFIER =
      NameIdentifier.of("metalake", "catalog", "schema", "object");
  private static final AuditInfo AUDIT = AuditInfo.builder().withCreator("creator").build();

  /** Internal table imports do not invent load/create events or change ownership. */
  @Test
  public void testImportTablePreservesAuditWithoutExtraOperations() {
    TableDispatcher delegate = mock(TableDispatcher.class);
    Table imported = mock(Table.class);
    EventBus events = mock(EventBus.class);
    OwnerDispatcher owner = mock(OwnerDispatcher.class);
    when(delegate.importTable(IDENTIFIER, AUDIT)).thenReturn(imported);
    TableDispatcher dispatcher =
        new TableHookDispatcher(new TableEventDispatcher(events, delegate), () -> owner);

    Assertions.assertSame(imported, dispatcher.importTable(IDENTIFIER, AUDIT));

    verify(delegate).importTable(IDENTIFIER, AUDIT);
    verifyNoInteractions(events, owner);
  }

  /** Internal schema imports do not invent load/create events or change ownership. */
  @Test
  public void testImportSchemaPreservesAuditWithoutExtraOperations() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", "schema");
    SchemaDispatcher delegate = mock(SchemaDispatcher.class);
    Schema imported = mock(Schema.class);
    EventBus events = mock(EventBus.class);
    OwnerDispatcher owner = mock(OwnerDispatcher.class);
    when(delegate.importSchema(identifier, AUDIT)).thenReturn(imported);
    SchemaDispatcher dispatcher =
        new SchemaHookDispatcher(new SchemaEventDispatcher(events, delegate));

    Assertions.assertSame(imported, dispatcher.importSchema(identifier, AUDIT));

    verify(delegate).importSchema(identifier, AUDIT);
    verifyNoInteractions(events, owner);
  }

  /** Internal view imports do not invent load/create events or change ownership. */
  @Test
  public void testImportViewPreservesAuditWithoutExtraOperations() {
    ViewDispatcher delegate = mock(ViewDispatcher.class);
    View imported = mock(View.class);
    EventBus events = mock(EventBus.class);
    OwnerDispatcher owner = mock(OwnerDispatcher.class);
    when(delegate.importView(IDENTIFIER, AUDIT)).thenReturn(imported);
    ViewDispatcher dispatcher =
        new ViewHookDispatcher(new ViewEventDispatcher(events, delegate), () -> owner);

    Assertions.assertSame(imported, dispatcher.importView(IDENTIFIER, AUDIT));

    verify(delegate).importView(IDENTIFIER, AUDIT);
    verifyNoInteractions(events, owner);
  }
}
