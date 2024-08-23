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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.Arrays;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.audit.FilesetAuditConstants;
import org.apache.gravitino.catalog.FilesetDispatcher;
import org.apache.gravitino.context.CallerContext;
import org.apache.gravitino.enums.FilesetDataOperation;
import org.apache.gravitino.enums.InternalClientType;
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
    Event event = dummyEventListener.popEvent();
    assertEquals(identifier, event.identifier());
    assertEquals(CreateFilesetEvent.class, event.getClass());
    FilesetInfo filesetInfo = ((CreateFilesetEvent) event).createdFilesetInfo();
    checkFilesetInfo(filesetInfo, fileset);
  }

  @Test
  void testLoadFilesetEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", fileset.name());
    dispatcher.loadFileset(identifier);
    Event event = dummyEventListener.popEvent();
    assertEquals(identifier, event.identifier());
    assertEquals(LoadFilesetEvent.class, event.getClass());
    FilesetInfo filesetInfo = ((LoadFilesetEvent) event).loadedFilesetInfo();
    checkFilesetInfo(filesetInfo, fileset);
  }

  @Test
  void testAlterFilesetEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", fileset.name());
    FilesetChange change = FilesetChange.setProperty("a", "b");
    dispatcher.alterFileset(identifier, change);
    Event event = dummyEventListener.popEvent();
    assertEquals(identifier, event.identifier());
    assertEquals(AlterFilesetEvent.class, event.getClass());
    FilesetInfo filesetInfo = ((AlterFilesetEvent) event).updatedFilesetInfo();
    checkFilesetInfo(filesetInfo, fileset);
    assertEquals(1, ((AlterFilesetEvent) event).filesetChanges().length);
    assertEquals(change, ((AlterFilesetEvent) event).filesetChanges()[0]);
  }

  @Test
  void testDropFilesetEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", fileset.name());
    dispatcher.dropFileset(identifier);
    Event event = dummyEventListener.popEvent();
    assertEquals(identifier, event.identifier());
    assertEquals(DropFilesetEvent.class, event.getClass());
    Assertions.assertTrue(((DropFilesetEvent) event).isExists());
  }

  @Test
  void testListFilesetEvent() {
    Namespace namespace = Namespace.of("metalake", "catalog");
    dispatcher.listFilesets(namespace);
    Event event = dummyEventListener.popEvent();
    assertEquals(namespace.toString(), event.identifier().toString());
    assertEquals(ListFilesetEvent.class, event.getClass());
    assertEquals(namespace, ((ListFilesetEvent) event).namespace());
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
    Event event = dummyEventListener.popEvent();
    assertEquals(identifier, event.identifier());
    assertEquals(CreateFilesetEvent.class, event.getClass());
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
    Event event1 = dummyEventListener.popEvent();
    assertEquals(identifier, event1.identifier());
    assertEquals(GetFileLocationEvent.class, event1.getClass());
    String actualFileLocation = ((GetFileLocationEvent) event1).actualFileLocation();
    assertEquals(actualFileLocation, fileLocation);
    CallerContext actualCallerContext = ((GetFileLocationEvent) event1).callerContext();
    assertEquals(2, actualCallerContext.context().size());
    assertEquals(
        InternalClientType.HADOOP_GVFS.name(),
        actualCallerContext.context().get(FilesetAuditConstants.HTTP_HEADER_INTERNAL_CLIENT_TYPE));
    assertEquals(
        FilesetDataOperation.GET_FILE_STATUS.name(),
        actualCallerContext
            .context()
            .get(FilesetAuditConstants.HTTP_HEADER_FILESET_DATA_OPERATION));
  }

  @Test
  void testCreateSchemaFailureEvent() {
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
    Event event = dummyEventListener.popEvent();
    assertEquals(identifier, event.identifier());
    assertEquals(CreateFilesetFailureEvent.class, event.getClass());
    assertEquals(
        GravitinoRuntimeException.class,
        ((CreateFilesetFailureEvent) event).exception().getClass());
    checkFilesetInfo(((CreateFilesetFailureEvent) event).createFilesetRequest(), fileset);
  }

  @Test
  void testLoadFilesetFailureEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", "fileset");
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.loadFileset(identifier));
    Event event = dummyEventListener.popEvent();
    assertEquals(identifier, event.identifier());
    assertEquals(LoadFilesetFailureEvent.class, event.getClass());
    assertEquals(
        GravitinoRuntimeException.class, ((LoadFilesetFailureEvent) event).exception().getClass());
  }

  @Test
  void testAlterFilesetFailureEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", "fileset");
    FilesetChange change = FilesetChange.setProperty("a", "b");
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.alterFileset(identifier, change));
    Event event = dummyEventListener.popEvent();
    assertEquals(identifier, event.identifier());
    assertEquals(AlterFilesetFailureEvent.class, event.getClass());
    assertEquals(
        GravitinoRuntimeException.class, ((AlterFilesetFailureEvent) event).exception().getClass());
    assertEquals(1, ((AlterFilesetFailureEvent) event).filesetChanges().length);
    assertEquals(change, ((AlterFilesetFailureEvent) event).filesetChanges()[0]);
  }

  @Test
  void testDropFilesetFailureEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", "fileset");
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.dropFileset(identifier));
    Event event = dummyEventListener.popEvent();
    assertEquals(identifier, event.identifier());
    assertEquals(DropFilesetFailureEvent.class, event.getClass());
    assertEquals(
        GravitinoRuntimeException.class, ((DropFilesetFailureEvent) event).exception().getClass());
  }

  @Test
  void testListFilesetFailureEvent() {
    Namespace namespace = Namespace.of("metalake", "catalog");
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.listFilesets(namespace));
    Event event = dummyEventListener.popEvent();
    assertEquals(namespace.toString(), event.identifier().toString());
    assertEquals(ListFilesetFailureEvent.class, event.getClass());
    assertEquals(
        GravitinoRuntimeException.class, ((ListFilesetFailureEvent) event).exception().getClass());
    assertEquals(namespace, ((ListFilesetFailureEvent) event).namespace());
  }

  @Test
  void testGetFileLocationFailureEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", "fileset");
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.getFileLocation(identifier, "/test"));
    Event event = dummyEventListener.popEvent();
    assertEquals(identifier, event.identifier());
    assertEquals(GetFileLocationFailureEvent.class, event.getClass());
    assertEquals(
        GravitinoRuntimeException.class,
        ((GetFileLocationFailureEvent) event).exception().getClass());
  }

  private void checkFilesetInfo(FilesetInfo filesetInfo, Fileset fileset) {
    assertEquals(fileset.name(), filesetInfo.name());
    assertEquals(fileset.type(), filesetInfo.type());
    assertEquals(fileset.storageLocation(), filesetInfo.storageLocation());
    assertEquals(fileset.properties(), filesetInfo.properties());
    assertEquals(fileset.comment(), filesetInfo.comment());
  }

  private Fileset mockFileset() {
    Fileset fileset = mock(Fileset.class);
    when(fileset.comment()).thenReturn("comment");
    when(fileset.type()).thenReturn(Fileset.Type.MANAGED);
    when(fileset.properties()).thenReturn(ImmutableMap.of("a", "b"));
    when(fileset.name()).thenReturn("fileset");
    when(fileset.auditInfo()).thenReturn(null);
    when(fileset.storageLocation()).thenReturn("location");
    return fileset;
  }

  private FilesetDispatcher mockFilesetDispatcher() {
    FilesetDispatcher dispatcher = mock(FilesetDispatcher.class);
    when(dispatcher.createFileset(
            any(NameIdentifier.class),
            any(String.class),
            any(Fileset.Type.class),
            any(String.class),
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
