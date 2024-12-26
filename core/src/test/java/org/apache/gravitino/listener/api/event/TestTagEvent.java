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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.listener.DummyEventListener;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.TagEventDispatcher;
import org.apache.gravitino.tag.Tag;
import org.apache.gravitino.tag.TagChange;
import org.apache.gravitino.tag.TagDispatcher;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
public class TestTagEvent {
  private TagEventDispatcher failureDispatcher;
  private DummyEventListener dummyEventListener;
  private Tag tag;

  @BeforeAll
  void init() {
    this.tag = mockTag();
    this.dummyEventListener = new DummyEventListener();
    EventBus eventBus = new EventBus(Arrays.asList(dummyEventListener));
    TagDispatcher tagExceptionDispatcher = mockExceptionTagDispatcher();
    this.failureDispatcher = new TagEventDispatcher(eventBus, tagExceptionDispatcher);
  }

  @Test
  void testCreateTagFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.createTag("metalake", tag.name(), tag.comment(), tag.properties()));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(CreateTagFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((CreateTagFailureEvent) event).exception().getClass());
    Assertions.assertEquals("metalake", ((CreateTagFailureEvent) event).metalake());
    Assertions.assertEquals(tag.name(), ((CreateTagFailureEvent) event).tagInfo().name());
    Assertions.assertEquals(tag.comment(), ((CreateTagFailureEvent) event).tagInfo().comment());
    Assertions.assertEquals(
        tag.properties(), ((CreateTagFailureEvent) event).tagInfo().properties());
    Assertions.assertEquals(OperationType.CREATE_TAG, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testGetTagFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.getTag("metalake", tag.name()));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(GetTagFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((GetTagFailureEvent) event).exception().getClass());
    Assertions.assertEquals("metalake", ((GetTagFailureEvent) event).metalake());
    Assertions.assertEquals(tag.name(), ((GetTagFailureEvent) event).name());
    Assertions.assertEquals(OperationType.GET_TAG, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testGetTagForMetadataObjectFailureEvent() {
    MetadataObject metadataObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofCatalog("metalake", "catalog_for_test"),
            Entity.EntityType.CATALOG);

    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.getTagForMetadataObject("metalake", metadataObject, tag.name()));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(GetTagForMetadataObjectFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((GetTagForMetadataObjectFailureEvent) event).exception().getClass());
    Assertions.assertEquals("metalake", ((GetTagForMetadataObjectFailureEvent) event).metalake());
    Assertions.assertEquals(tag.name(), ((GetTagForMetadataObjectFailureEvent) event).name());
    Assertions.assertEquals(
        metadataObject, ((GetTagForMetadataObjectFailureEvent) event).metadataObject());
    Assertions.assertEquals(OperationType.GET_TAG_FOR_METADATA_OBJECT, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testDeleteTagFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.deleteTag("metalake", tag.name()));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(DeleteTagFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((DeleteTagFailureEvent) event).exception().getClass());
    Assertions.assertEquals("metalake", ((DeleteTagFailureEvent) event).metalake());
    Assertions.assertEquals(tag.name(), ((DeleteTagFailureEvent) event).name());
    Assertions.assertEquals(OperationType.DELETE_TAG, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testAlterTagFailureEvent() {
    TagChange change1 = TagChange.rename("newName");
    TagChange change2 = TagChange.updateComment("new comment");
    TagChange change3 = TagChange.setProperty("key", "value");
    TagChange change4 = TagChange.removeProperty("oldKey");
    TagChange[] changes = new TagChange[] {change1, change2, change3, change4};
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.alterTag("metalake", tag.name(), changes));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(AlterTagFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((AlterTagFailureEvent) event).exception().getClass());
    Assertions.assertEquals("metalake", ((AlterTagFailureEvent) event).metalake());
    Assertions.assertEquals(tag.name(), ((AlterTagFailureEvent) event).name());
    Assertions.assertEquals(changes, ((AlterTagFailureEvent) event).changes());
    Assertions.assertEquals(OperationType.ALTER_TAG, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testListTagFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.listTags("metalake"));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(ListTagFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((ListTagFailureEvent) event).exception().getClass());
    Assertions.assertEquals("metalake", ((ListTagFailureEvent) event).metalake());
    Assertions.assertEquals(OperationType.LIST_TAG, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testListTagsForMetadataObjectFailureEvent() {
    MetadataObject metadataObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofCatalog("metalake", "catalog_for_test"),
            Entity.EntityType.CATALOG);

    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.listTagsForMetadataObject("metalake", metadataObject));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(ListTagsForMetadataObjectFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((ListTagsForMetadataObjectFailureEvent) event).exception().getClass());
    Assertions.assertEquals("metalake", ((ListTagsForMetadataObjectFailureEvent) event).metalake());
    Assertions.assertEquals(
        metadataObject, ((ListTagsForMetadataObjectFailureEvent) event).metadataObject());
    Assertions.assertEquals(OperationType.LIST_TAGS_FOR_METADATA_OBJECT, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testListTagsInfoFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.listTagsInfo("metalake"));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(ListTagsInfoFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((ListTagsInfoFailureEvent) event).exception().getClass());
    Assertions.assertEquals("metalake", ((ListTagsInfoFailureEvent) event).metalake());
    Assertions.assertEquals(OperationType.LIST_TAGS_INFO, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testListTagsInfoForMetadataObjectFailureEvent() {
    MetadataObject metadataObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofCatalog("metalake", "catalog_for_test"),
            Entity.EntityType.CATALOG);

    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.listTagsInfoForMetadataObject("metalake", metadataObject));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(ListTagsInfoForMetadataObjectFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((ListTagsInfoForMetadataObjectFailureEvent) event).exception().getClass());
    Assertions.assertEquals(
        "metalake", ((ListTagsInfoForMetadataObjectFailureEvent) event).metalake());
    Assertions.assertEquals(
        metadataObject, ((ListTagsInfoForMetadataObjectFailureEvent) event).metadataObject());
    Assertions.assertEquals(
        OperationType.LIST_TAGS_INFO_FOR_METADATA_OBJECT, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testAssociateTagsForMetadataObjectFailureEvent() {
    MetadataObject metadataObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofCatalog("metalake", "catalog_for_test"),
            Entity.EntityType.CATALOG);

    String[] tagsToAssociate = new String[] {"tag1", "tag2"};
    String[] tagsToDisassociate = new String[] {"tag3", "tag4"};

    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () ->
            failureDispatcher.associateTagsForMetadataObject(
                "metalake", metadataObject, tagsToAssociate, tagsToDisassociate));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(AssociateTagsForMetadataObjectFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((AssociateTagsForMetadataObjectFailureEvent) event).exception().getClass());
    Assertions.assertEquals(
        "metalake", ((AssociateTagsForMetadataObjectFailureEvent) event).metalake());
    Assertions.assertEquals(
        metadataObject, ((AssociateTagsForMetadataObjectFailureEvent) event).metadataObject());
    Assertions.assertEquals(
        tagsToAssociate, ((AssociateTagsForMetadataObjectFailureEvent) event).tagsToAdd());
    Assertions.assertEquals(
        tagsToDisassociate, ((AssociateTagsForMetadataObjectFailureEvent) event).tagsToRemove());
    Assertions.assertEquals(
        OperationType.ASSOCIATE_TAGS_FOR_METADATA_OBJECT, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  private Tag mockTag() {
    Tag tag = mock(Tag.class);
    when(tag.name()).thenReturn("tag");
    when(tag.comment()).thenReturn("comment");
    when(tag.properties()).thenReturn(ImmutableMap.of("color", "#FFFFFF"));
    return tag;
  }

  private TagDispatcher mockExceptionTagDispatcher() {
    return mock(
        TagDispatcher.class,
        invocation -> {
          throw new GravitinoRuntimeException("Exception for all methods");
        });
  }
}
