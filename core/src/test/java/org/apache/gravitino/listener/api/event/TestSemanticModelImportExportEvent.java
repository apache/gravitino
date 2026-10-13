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
package org.apache.gravitino.listener.api.event;

import static org.apache.gravitino.semantic.SemanticModel.PROPERTY_OSSIE_VERSION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.audit.AuditLog;
import org.apache.gravitino.audit.v2.SimpleAuditLogV2;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.catalog.CatalogTestUtils;
import org.apache.gravitino.catalog.SemanticModelDispatcher;
import org.apache.gravitino.catalog.SemanticModelNormalizeDispatcher;
import org.apache.gravitino.catalog.SemanticModelOperationDispatcher;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.IllegalSemanticModelException;
import org.apache.gravitino.exceptions.NoSuchSemanticModelException;
import org.apache.gravitino.exceptions.SemanticModelAlreadyExistsException;
import org.apache.gravitino.hook.SemanticModelHookDispatcher;
import org.apache.gravitino.listener.DummyEventListener;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.SemanticModelEventDispatcher;
import org.apache.gravitino.listener.api.event.semantic.ExportSemanticModelEvent;
import org.apache.gravitino.listener.api.event.semantic.ExportSemanticModelFailureEvent;
import org.apache.gravitino.listener.api.event.semantic.ExportSemanticModelPreEvent;
import org.apache.gravitino.listener.api.event.semantic.ImportSemanticModelEvent;
import org.apache.gravitino.listener.api.event.semantic.ImportSemanticModelFailureEvent;
import org.apache.gravitino.listener.api.event.semantic.ImportSemanticModelPreEvent;
import org.apache.gravitino.semantic.Dataset;
import org.apache.gravitino.semantic.OssieDocument;
import org.apache.gravitino.semantic.OssieDocumentConverter;
import org.apache.gravitino.semantic.OssieFormat;
import org.apache.gravitino.semantic.SemanticModel;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.apache.gravitino.utils.PrincipalUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

class TestSemanticModelImportExportEvent {
  private static final Namespace NAMESPACE = Namespace.of("metalake", "catalog", "MixedSchema");
  private static final NameIdentifier IDENT = NameIdentifier.of(NAMESPACE, "SalesModel");
  private static final NameIdentifier NORMALIZED_IDENT =
      NameIdentifier.of("metalake", "catalog", "mixedschema", "SalesModel");

  private SemanticModelDispatcher operation;
  private SemanticModelNormalizeDispatcher normalizer;
  private SemanticModelHookDispatcher hook;
  private SemanticModelEventDispatcher dispatcher;
  private OwnerDispatcher owners;
  private DummyEventListener listener;
  private SemanticModel model;
  private boolean authorizationEnabled;

  @BeforeEach
  void setUp() {
    authorizationEnabled = true;
    owners = mock(OwnerDispatcher.class);
    operation = mock(SemanticModelOperationDispatcher.class);
    doCallRealMethod().when(operation).importSemanticModel(any(), any(), any(), any());
    doCallRealMethod().when(operation).exportOssieDocument(any(), any());
    model = mock(SemanticModel.class);
    when(model.name()).thenReturn(IDENT.name());
    when(model.definition()).thenReturn(definition());
    when(model.properties()).thenReturn(Map.of());
    when(operation.createSemanticModel(any(), any(), any(), any())).thenReturn(model);
    when(operation.loadSemanticModel(NORMALIZED_IDENT)).thenReturn(model);
    CatalogManager catalogs = mock(CatalogManager.class);
    BaseCatalog<?> catalog = mock(BaseCatalog.class);
    when(catalog.capability()).thenReturn(new ParentNormalizingCapability());
    CatalogTestUtils.mockDoWithCatalog(catalogs, catalog);
    hook =
        spy(new SemanticModelHookDispatcher(operation, () -> authorizationEnabled ? owners : null));
    normalizer = spy(new SemanticModelNormalizeDispatcher(hook, catalogs));
    listener = new DummyEventListener();
    dispatcher = new SemanticModelEventDispatcher(new EventBus(List.of(listener)), normalizer);
  }

  @ParameterizedTest
  @EnumSource(OssieFormat.class)
  void testImportParsesOnceAndSetsOwnerWithoutCreateEventsOrHooks(OssieFormat format) {
    OssieDocument document = OssieDocumentConverter.exportDocument(model, format);
    AtomicInteger validations = new AtomicInteger();
    try (MockedStatic<OssieDocumentConverter> converter =
        mockStatic(OssieDocumentConverter.class, CALLS_REAL_METHODS)) {
      assertSame(
          model,
          dispatcher.importOssieDocument(
              NAMESPACE,
              document,
              definition -> {
                assertEquals(definition(), definition);
                verify(operation, never()).createSemanticModel(any(), any(), any(), any());
                verifyNoInteractions(owners);
                validations.incrementAndGet();
              }));
      converter.verify(() -> OssieDocumentConverter.importDocument(document));
    }
    assertEquals(1, validations.get());
    verify(operation).importSemanticModel(eq(NORMALIZED_IDENT), any(), eq(definition()), any());
    verify(operation).createSemanticModel(eq(NORMALIZED_IDENT), any(), eq(definition()), any());
    ArgumentCaptor<MetadataObject> object = ArgumentCaptor.forClass(MetadataObject.class);
    verify(owners).setOwner(eq("metalake"), object.capture(), any(), eq(Owner.Type.USER));
    assertEquals("catalog.mixedschema.SalesModel", object.getValue().fullName());
    assertEquals(MetadataObject.Type.SEMANTIC_MODEL, object.getValue().type());
    verifyNoMoreInteractions(owners);

    ImportSemanticModelEvent event =
        assertEvents(ImportSemanticModelPreEvent.class, ImportSemanticModelEvent.class);
    ImportSemanticModelPreEvent pre =
        (ImportSemanticModelPreEvent) listener.getPreEvents().getFirst();
    assertEquals(NAMESPACE, pre.namespace());
    assertEquals(NameIdentifier.of(NAMESPACE.levels()), pre.identifier());
    assertEquals(format, pre.format());
    assertEquals(IDENT, event.identifier());
    assertEquals(format, event.format());
    assertEquals(model.definition(), event.importedSemanticModelInfo().definition());
    assertAudit(event, AuditLog.Operation.IMPORT_SEMANTIC_MODEL, OperationStatus.SUCCESS);
  }

  @Test
  void testDirectParsedImportIsRejectedWithoutEventsOrSideEffects() {
    UnsupportedOperationException failure =
        assertThrows(
            UnsupportedOperationException.class,
            () -> dispatcher.importSemanticModel(IDENT, null, definition(), Map.of()));

    assertTrue(failure.getMessage().contains("importOssieDocument"));
    verifyNoInteractions(normalizer, hook, operation, owners);
    assertEquals(0, listener.getPreEvents().size());
    assertEquals(0, listener.getPostEvents().size());
  }

  @Test
  void testImportStillValidatesSourcesWhenOwnershipIsDisabled() {
    authorizationEnabled = false;
    AtomicInteger validations = new AtomicInteger();
    dispatcher.importOssieDocument(
        NAMESPACE, document(), definition -> validations.incrementAndGet());
    assertEquals(1, validations.get());
    assertEvents(ImportSemanticModelPreEvent.class, ImportSemanticModelEvent.class);
    verifyNoInteractions(owners);
  }

  @ParameterizedTest
  @ValueSource(strings = {"parse", "source", "normalize", "create", "owner"})
  void testImportFailuresEmitOnlyImportFailure(String stage) {
    OssieDocument document = document();
    RuntimeException expected = new ForbiddenException("source denied");
    if (stage.equals("parse")) {
      document = OssieDocument.json("{invalid json");
    } else if (stage.equals("normalize")) {
      document = OssieDocument.yaml(document.content().replace("SalesModel", "invalid model"));
    } else if (stage.equals("create")) {
      expected = new SemanticModelAlreadyExistsException("already exists");
      doThrow(expected).when(operation).createSemanticModel(any(), any(), any(), any());
    } else if (stage.equals("owner")) {
      expected = new IllegalStateException("owner assignment failed");
      doThrow(expected).when(owners).setOwner(any(), any(), any(), any());
    }
    OssieDocument request = document;
    RuntimeException failure = expected;
    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () ->
                dispatcher.importOssieDocument(
                    NAMESPACE,
                    request,
                    definition -> {
                      if (stage.equals("source")) throw failure;
                    }));
    ImportSemanticModelFailureEvent event =
        assertEvents(ImportSemanticModelPreEvent.class, ImportSemanticModelFailureEvent.class);
    assertSame(thrown, event.exception());
    assertEquals(NAMESPACE, event.namespace());
    assertEquals(NameIdentifier.of(NAMESPACE.levels()), event.identifier());
    assertEquals(document.format(), event.format());
    assertAudit(event, AuditLog.Operation.IMPORT_SEMANTIC_MODEL, OperationStatus.FAILURE);
    if (!stage.equals("owner")) verifyNoInteractions(owners);
    if (List.of("parse", "source", "normalize").contains(stage)) {
      verify(operation, never()).createSemanticModel(any(), any(), any(), any());
    } else {
      assertSame(expected, thrown);
    }
  }

  @Test
  void testImportRejectsMissingValidatorBeforeWriting() {
    assertThrows(
        NullPointerException.class,
        () -> dispatcher.importOssieDocument(NAMESPACE, document(), null));
    assertEvents(ImportSemanticModelPreEvent.class, ImportSemanticModelFailureEvent.class);
    verifyNoInteractions(operation, owners);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testPreEventRejectionEmitsFailureWithoutInvokingOperation(boolean importing) {
    DummyEventListener rejecting = spy(listener);
    ForbiddenException denied = new ForbiddenException("listener denied");
    doThrow(denied).when(rejecting).onPreEvent(any());
    SemanticModelDispatcher rejectedDispatcher =
        new SemanticModelEventDispatcher(new EventBus(List.of(rejecting)), normalizer);
    Runnable action =
        importing
            ? () -> rejectedDispatcher.importOssieDocument(NAMESPACE, document(), definition -> {})
            : () -> rejectedDispatcher.exportOssieDocument(IDENT, OssieFormat.YAML);
    assertSame(denied, assertThrows(ForbiddenException.class, action::run));
    verifyNoInteractions(normalizer, operation, owners);
    assertEquals(1, rejecting.getPostEvents().size());
    FailureEvent failure =
        assertInstanceOf(FailureEvent.class, rejecting.getPostEvents().getFirst());
    assertEquals(
        importing ? OperationType.IMPORT_SEMANTIC_MODEL : OperationType.EXPORT_SEMANTIC_MODEL,
        failure.operationType());
    assertSame(denied, failure.exception());
  }

  @Test
  void testNullImportDocumentStillEmitsFailure() {
    assertThrows(
        NullPointerException.class,
        () -> dispatcher.importOssieDocument(NAMESPACE, null, definition -> {}));
    ImportSemanticModelFailureEvent event =
        assertEvents(ImportSemanticModelPreEvent.class, ImportSemanticModelFailureEvent.class);
    assertNull(event.format());
    verifyNoInteractions(operation, owners);
  }

  @ParameterizedTest
  @EnumSource(OssieFormat.class)
  void testExportDoesNotEmitLoadEventsOrRunLoadHooks(OssieFormat format) {
    OssieDocument document = dispatcher.exportOssieDocument(IDENT, format);
    assertEquals(format, document.format());
    assertEquals(IDENT.name(), OssieDocumentConverter.importDocument(document).name());
    verify(operation).loadSemanticModel(NORMALIZED_IDENT);
    verifyNoInteractions(owners);
    ExportSemanticModelEvent event =
        assertEvents(ExportSemanticModelPreEvent.class, ExportSemanticModelEvent.class);
    ExportSemanticModelPreEvent pre =
        (ExportSemanticModelPreEvent) listener.getPreEvents().getFirst();
    assertEquals(IDENT, pre.identifier());
    assertEquals(format, pre.format());
    assertEquals(IDENT, event.identifier());
    assertEquals(format, event.format());
    assertAudit(event, AuditLog.Operation.EXPORT_SEMANTIC_MODEL, OperationStatus.SUCCESS);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testExportLoadAndConversionFailures(boolean conversionFailure) {
    if (conversionFailure) {
      when(model.properties()).thenReturn(Map.of(PROPERTY_OSSIE_VERSION, " "));
    } else {
      when(operation.loadSemanticModel(NORMALIZED_IDENT))
          .thenThrow(new NoSuchSemanticModelException("missing model"));
    }
    RuntimeException thrown =
        assertThrows(
            RuntimeException.class, () -> dispatcher.exportOssieDocument(IDENT, OssieFormat.YAML));
    assertEquals(
        conversionFailure
            ? IllegalSemanticModelException.class
            : NoSuchSemanticModelException.class,
        thrown.getClass());
    ExportSemanticModelFailureEvent event =
        assertEvents(ExportSemanticModelPreEvent.class, ExportSemanticModelFailureEvent.class);
    assertSame(thrown, event.exception());
    assertEquals(IDENT, event.identifier());
    assertEquals(OssieFormat.YAML, event.format());
    assertAudit(event, AuditLog.Operation.EXPORT_SEMANTIC_MODEL, OperationStatus.FAILURE);
    verifyNoInteractions(owners);
  }

  @Test
  void testNullExportFormatStillEmitsFailure() {
    assertThrows(NullPointerException.class, () -> dispatcher.exportOssieDocument(IDENT, null));
    ExportSemanticModelFailureEvent event =
        assertEvents(ExportSemanticModelPreEvent.class, ExportSemanticModelFailureEvent.class);
    assertNull(event.format());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testImportAndExportRetainInitiatingUser(boolean fail) {
    authorizationEnabled = false;
    if (fail) {
      doThrow(new IllegalArgumentException("rejected"))
          .when(operation)
          .createSemanticModel(any(), any(), any(), any());
      doThrow(new IllegalArgumentException("rejected")).when(operation).loadSemanticModel(any());
    }
    for (boolean importing : new boolean[] {true, false}) {
      listener.clear();
      try (MockedStatic<PrincipalUtils> principal = mockStatic(PrincipalUtils.class)) {
        principal.when(PrincipalUtils::getCurrentUserName).thenReturn("initiator", "changed");
        Runnable action =
            importing
                ? () -> dispatcher.importOssieDocument(NAMESPACE, document(), definition -> {})
                : () -> dispatcher.exportOssieDocument(IDENT, OssieFormat.JSON);
        if (fail) assertThrows(IllegalArgumentException.class, action::run);
        else action.run();
        assertEquals("initiator", listener.getPreEvents().getFirst().user());
        assertEquals("initiator", listener.getPostEvents().getFirst().user());
        principal.verify(PrincipalUtils::getCurrentUserName);
      }
    }
  }

  private <T extends Event> T assertEvents(Class<? extends PreEvent> pre, Class<T> post) {
    assertEquals(1, listener.getPreEvents().size());
    assertEquals(1, listener.getPostEvents().size());
    PreEvent preEvent = assertInstanceOf(pre, listener.getPreEvents().getFirst());
    T postEvent = assertInstanceOf(post, listener.getPostEvents().getFirst());
    assertEquals(preEvent.operationType(), postEvent.operationType());
    assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
    verify(normalizer, never()).createSemanticModel(any(), any(), any(), any());
    verify(normalizer, never()).loadSemanticModel(any());
    verify(hook, never()).createSemanticModel(any(), any(), any(), any());
    verify(hook, never()).loadSemanticModel(any());
    return postEvent;
  }

  @SuppressWarnings("deprecation")
  private void assertAudit(Event event, AuditLog.Operation operation, OperationStatus status) {
    assertEquals(operation, AuditLog.Operation.fromEvent(event));
    SimpleAuditLogV2 audit = new SimpleAuditLogV2(event);
    assertEquals(operation, audit.operation());
    assertEquals(OperationType.valueOf(operation.name()), audit.operationType());
    assertEquals(status, audit.operationStatus());
    assertEquals(event.identifier().toString(), audit.identifier());
    assertEquals(event.user(), audit.user());
  }

  private static OssieDocument document() {
    return OssieDocument.yaml(
        "version: 0.2.0.dev0\nname: SalesModel\ndatasets:\n"
            + "  - name: orders\n    source: sales.mart.orders\n");
  }

  private static SemanticModelDefinition definition() {
    return SemanticModelDefinition.builder()
        .withDatasets(
            new Dataset[] {
              Dataset.builder()
                  .withName("orders")
                  .withSource(NameIdentifier.of("sales", "mart", "orders"))
                  .build()
            })
        .build();
  }

  private static class ParentNormalizingCapability implements Capability {
    @Override
    public CapabilityResult caseSensitiveOnName(Scope scope) {
      return CapabilityResult.unsupported("Case-insensitive schema");
    }

    @Override
    public String normalizeName(Scope scope, String name) {
      return scope == Scope.SCHEMA ? name.toLowerCase(Locale.ROOT) : name;
    }
  }
}
