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

import static org.apache.gravitino.file.Fileset.LOCATION_NAME_UNKNOWN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.audit.CallerContext;
import org.apache.gravitino.audit.FilesetAuditConstants;
import org.apache.gravitino.audit.FilesetDataOperation;
import org.apache.gravitino.audit.InternalClientType;
import org.apache.gravitino.catalog.FilesetDispatcher;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.file.FilesetChange;
import org.apache.gravitino.listener.DummyEventListener;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.FilesetEventDispatcher;
import org.apache.gravitino.listener.api.info.FilesetInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
public class TestFilesetEvent {
  private FilesetEventDispatcher dispatcher;
  private FilesetEventDispatcher failureDispatcher;
  private DummyEventListener dummyEventListener;
  private Fileset fileset;

  @BeforeAll
  void init() {
    this.fileset = mockFileset();
    this.dummyEventListener = new DummyEventListener();
    EventBus eventBus = new EventBus(Arrays.asList(dummyEventListener));
    FilesetDispatcher filesetDispatcher = mockFilesetDispatcher();
    this.dispatcher = new FilesetEventDispatcher(eventBus, filesetDispatcher);
    FilesetDispatcher filesetExceptionDispatcher = mockExceptionFilesetDispatcher();
    this.failureDispatcher = new FilesetEventDispatcher(eventBus, filesetExceptionDispatcher);
  }

  @Test
  void testCreateFilesetEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", fileset.name());
    dispatcher.createFileset(
        identifier,
        fileset.comment(),
        fileset.type(),
        fileset.storageLocation(),
        fileset.properties());
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(CreateFilesetEvent.class, event.getClass());
    FilesetInfo filesetInfo = ((CreateFilesetEvent) event).createdFilesetInfo();
    checkFilesetInfo(filesetInfo, fileset);
    Assertions.assertEquals(OperationType.CREATE_FILESET, event.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());

    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(identifier, preEvent.identifier());
    Assertions.assertEquals(CreateFilesetPreEvent.class, preEvent.getClass());
    filesetInfo = ((CreateFilesetPreEvent) preEvent).createFilesetRequest();
    checkFilesetInfo(filesetInfo, fileset);
    Assertions.assertEquals(OperationType.CREATE_FILESET, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
  }

  @Test
  void testLoadFilesetEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", fileset.name());
    dispatcher.loadFileset(identifier);
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(LoadFilesetEvent.class, event.getClass());
    FilesetInfo filesetInfo = ((LoadFilesetEvent) event).loadedFilesetInfo();
    checkFilesetInfo(filesetInfo, fileset);
    Assertions.assertEquals(OperationType.LOAD_FILESET, event.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());

    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(identifier, preEvent.identifier());
    Assertions.assertEquals(LoadFilesetPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.LOAD_FILESET, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
  }

  @Test
  void testAlterFilesetEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", fileset.name());
    FilesetChange change = FilesetChange.setProperty("a", "b");
    dispatcher.alterFileset(identifier, change);
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(AlterFilesetEvent.class, event.getClass());
    FilesetInfo filesetInfo = ((AlterFilesetEvent) event).updatedFilesetInfo();
    checkFilesetInfo(filesetInfo, fileset);
    Assertions.assertEquals(1, ((AlterFilesetEvent) event).filesetChanges().length);
    Assertions.assertEquals(change, ((AlterFilesetEvent) event).filesetChanges()[0]);
    Assertions.assertEquals(OperationType.ALTER_FILESET, event.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());

    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(identifier, preEvent.identifier());
    Assertions.assertEquals(AlterFilesetPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(1, ((AlterFilesetPreEvent) preEvent).filesetChanges().length);
    Assertions.assertEquals(change, ((AlterFilesetPreEvent) preEvent).filesetChanges()[0]);
    Assertions.assertEquals(OperationType.ALTER_FILESET, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
  }

  @Test
  void testDropFilesetEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", fileset.name());
    dispatcher.dropFileset(identifier);
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(DropFilesetEvent.class, event.getClass());
    Assertions.assertTrue(((DropFilesetEvent) event).isExists());
    Assertions.assertEquals(OperationType.DROP_FILESET, event.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());

    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(identifier, preEvent.identifier());
    Assertions.assertEquals(DropFilesetPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.DROP_FILESET, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
  }

  @Test
  void testListFilesetEvent() {
    Namespace namespace = Namespace.of("metalake", "catalog");
    dispatcher.listFilesets(namespace);
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(namespace.toString(), event.identifier().toString());
    Assertions.assertEquals(ListFilesetEvent.class, event.getClass());
    Assertions.assertEquals(namespace, ((ListFilesetEvent) event).namespace());
    Assertions.assertEquals(OperationType.LIST_FILESET, event.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());

    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(namespace.toString(), preEvent.identifier().toString());
    Assertions.assertEquals(ListFilesetPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(namespace, ((ListFilesetPreEvent) preEvent).namespace());
    Assertions.assertEquals(OperationType.LIST_FILESET, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
  }

  @Test
  void testListFilesetFilesEvent() throws IOException {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", fileset.name());
    String locationName = "default";
    String subPath = "/";
    dispatcher.listFiles(identifier, locationName, subPath);
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(ListFilesEvent.class, event.getClass());
    Assertions.assertEquals(locationName, ((ListFilesEvent) event).locationName());
    Assertions.assertEquals(subPath, ((ListFilesEvent) event).subPath());
    Assertions.assertEquals(OperationType.LIST_FILESET_FILES, event.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());

    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(ListFilesPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(identifier, preEvent.identifier());
    Assertions.assertEquals(locationName, ((ListFilesPreEvent) preEvent).locationName());
    Assertions.assertEquals(subPath, ((ListFilesPreEvent) preEvent).subPath());
    Assertions.assertEquals(OperationType.LIST_FILESET_FILES, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
  }

  @Test
  void testGetFileLocationEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", fileset.name());
    dispatcher.createFileset(
        identifier,
        fileset.comment(),
        fileset.type(),
        fileset.storageLocation(),
        fileset.properties());
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(CreateFilesetEvent.class, event.getClass());
    FilesetInfo filesetInfo = ((CreateFilesetEvent) event).createdFilesetInfo();
    checkFilesetInfo(filesetInfo, fileset);

    Map<String, String> contextMap = Maps.newHashMap();
    contextMap.put(
        FilesetAuditConstants.HTTP_HEADER_INTERNAL_CLIENT_TYPE,
        InternalClientType.HADOOP_GVFS.name());
    contextMap.put(
        FilesetAuditConstants.HTTP_HEADER_FILESET_DATA_OPERATION,
        FilesetDataOperation.GET_FILE_STATUS.name());
    CallerContext callerContext = CallerContext.builder().withContext(contextMap).build();
    CallerContext.CallerContextHolder.set(callerContext);
    String fileLocation = dispatcher.getFileLocation(identifier, "test");
    Event event1 = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event1.identifier());
    Assertions.assertEquals(GetFileLocationEvent.class, event1.getClass());
    String actualFileLocation = ((GetFileLocationEvent) event1).actualFileLocation();
    Assertions.assertEquals(actualFileLocation, fileLocation);
    Map<String, String> actualContext = ((GetFileLocationEvent) event1).context();
    assertEquals(2, actualContext.size());
    Assertions.assertEquals(
        InternalClientType.HADOOP_GVFS.name(),
        actualContext.get(FilesetAuditConstants.HTTP_HEADER_INTERNAL_CLIENT_TYPE));
    Assertions.assertEquals(
        FilesetDataOperation.GET_FILE_STATUS.name(),
        actualContext.get(FilesetAuditConstants.HTTP_HEADER_FILESET_DATA_OPERATION));
    Assertions.assertEquals("test", ((GetFileLocationEvent) event1).subPath());
    Assertions.assertEquals(OperationType.GET_FILESET_LOCATION, event1.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, event1.operationStatus());

    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(GetFileLocationPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(identifier, preEvent.identifier());
    Assertions.assertEquals(OperationType.GET_FILESET_LOCATION, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
  }

  @Test
  void testCreateFilesetFailureEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", "fileset");
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () ->
            failureDispatcher.createFileset(
                identifier,
                fileset.comment(),
                fileset.type(),
                fileset.storageLocation(),
                fileset.properties()));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(CreateFilesetFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((CreateFilesetFailureEvent) event).exception().getClass());
    checkFilesetInfo(((CreateFilesetFailureEvent) event).createFilesetRequest(), fileset);
    Assertions.assertEquals(OperationType.CREATE_FILESET, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testLoadFilesetFailureEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", "fileset");
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.loadFileset(identifier));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(LoadFilesetFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((LoadFilesetFailureEvent) event).exception().getClass());
    Assertions.assertEquals(OperationType.LOAD_FILESET, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testAlterFilesetFailureEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", "fileset");
    FilesetChange change = FilesetChange.setProperty("a", "b");
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.alterFileset(identifier, change));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(AlterFilesetFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((AlterFilesetFailureEvent) event).exception().getClass());
    Assertions.assertEquals(1, ((AlterFilesetFailureEvent) event).filesetChanges().length);
    Assertions.assertEquals(change, ((AlterFilesetFailureEvent) event).filesetChanges()[0]);
    Assertions.assertEquals(OperationType.ALTER_FILESET, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testDropFilesetFailureEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", "fileset");
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.dropFileset(identifier));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(DropFilesetFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((DropFilesetFailureEvent) event).exception().getClass());
    Assertions.assertEquals(OperationType.DROP_FILESET, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testListFilesetFailureEvent() {
    Namespace namespace = Namespace.of("metalake", "catalog");
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.listFilesets(namespace));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(namespace.toString(), event.identifier().toString());
    Assertions.assertEquals(ListFilesetFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((ListFilesetFailureEvent) event).exception().getClass());
    Assertions.assertEquals(namespace, ((ListFilesetFailureEvent) event).namespace());
    Assertions.assertEquals(OperationType.LIST_FILESET, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testListFilesetFilesFailureEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", "fileset");
    String locationName = "default";
    String subPath = "/";
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.listFiles(identifier, locationName, subPath));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(ListFilesFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((ListFilesFailureEvent) event).exception().getClass());
    Assertions.assertEquals(locationName, ((ListFilesFailureEvent) event).locationName());
    Assertions.assertEquals(subPath, ((ListFilesFailureEvent) event).subPath());
    Assertions.assertEquals(OperationType.LIST_FILESET_FILES, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testGetFileLocationFailureEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", "fileset");
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.getFileLocation(identifier, "/test"));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(GetFileLocationFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((GetFileLocationFailureEvent) event).exception().getClass());
    Assertions.assertEquals(OperationType.GET_FILESET_LOCATION, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  private void checkFilesetInfo(FilesetInfo filesetInfo, Fileset fileset) {
    Assertions.assertEquals(fileset.name(), filesetInfo.name());
    Assertions.assertEquals(fileset.type(), filesetInfo.type());
    Assertions.assertEquals(fileset.storageLocation(), filesetInfo.storageLocation());
    Assertions.assertEquals(fileset.properties(), filesetInfo.properties());
    Assertions.assertEquals(fileset.comment(), filesetInfo.comment());
  }

  private Fileset mockFileset() {
    Fileset fileset = mock(Fileset.class);
    when(fileset.comment()).thenReturn("comment");
    when(fileset.type()).thenReturn(Fileset.Type.MANAGED);
    when(fileset.properties()).thenReturn(ImmutableMap.of("a", "b"));
    when(fileset.name()).thenReturn("fileset");
    when(fileset.auditInfo()).thenReturn(null);
    when(fileset.storageLocation()).thenCallRealMethod();
    when(fileset.storageLocations()).thenReturn(ImmutableMap.of(LOCATION_NAME_UNKNOWN, "location"));
    return fileset;
  }

  private FilesetDispatcher mockFilesetDispatcher() {
    FilesetDispatcher dispatcher = mock(FilesetDispatcher.class);
    when(dispatcher.createMultipleLocationFileset(
            any(NameIdentifier.class),
            any(String.class),
            any(Fileset.Type.class),
            any(Map.class),
            any(Map.class)))
        .thenReturn(fileset);
    when(dispatcher.loadFileset(any(NameIdentifier.class))).thenReturn(fileset);
    when(dispatcher.dropFileset(any(NameIdentifier.class))).thenReturn(true);
    when(dispatcher.listFilesets(any(Namespace.class))).thenReturn(null);
    when(dispatcher.alterFileset(any(NameIdentifier.class), any(FilesetChange.class)))
        .thenReturn(fileset);
    when(dispatcher.getFileLocation(any(NameIdentifier.class), any()))
        .thenReturn("file:/test/xxx.parquet");
    return dispatcher;
  }

  private FilesetDispatcher mockExceptionFilesetDispatcher() {
    FilesetDispatcher dispatcher =
        mock(
            FilesetDispatcher.class,
            invocation -> {
              throw new GravitinoRuntimeException("Exception for all methods");
            });
    return dispatcher;
  }
}
