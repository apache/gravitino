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
package org.apache.gravitino.listener;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.hook.TagHookDispatcher;
import org.apache.gravitino.listener.api.event.ListTagsInfoForMetadataObjectEvent;
import org.apache.gravitino.listener.api.event.ListTagsInfoForMetadataObjectFailureEvent;
import org.apache.gravitino.listener.api.event.ListTagsInfoForMetadataObjectPreEvent;
import org.apache.gravitino.tag.Tag;
import org.apache.gravitino.tag.TagDispatcher;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTagEventDispatcher {
  private static final String METALAKE = "metalake";

  @Test
  void testDisplayTagsPreserveDispatcherEvents() {
    TagDispatcher delegate = mock(TagDispatcher.class);
    DummyEventListener listener = new DummyEventListener();
    TagDispatcher dispatcher =
        new TagHookDispatcher(
            new TagEventDispatcher(new EventBus(Collections.singletonList(listener)), delegate));
    MetadataObject object = MetadataObjects.parse("catalog.schema", MetadataObject.Type.SCHEMA);
    List<MetadataObject> objects = List.of(object);
    Map<MetadataObject, Tag[]> expected = Collections.singletonMap(object, new Tag[0]);
    when(delegate.listEffectiveTagsForDisplay(METALAKE, objects)).thenReturn(expected);

    Assertions.assertSame(expected, dispatcher.listEffectiveTagsForDisplay(METALAKE, objects));
    Assertions.assertInstanceOf(
        ListTagsInfoForMetadataObjectPreEvent.class, listener.getPreEvents().get(0));
    Assertions.assertInstanceOf(
        ListTagsInfoForMetadataObjectEvent.class, listener.getPostEvents().get(0));
    RuntimeException failure = new RuntimeException("store unavailable");
    when(delegate.listEffectiveTagsForDisplay(METALAKE, objects)).thenThrow(failure);
    Assertions.assertSame(
        failure,
        Assertions.assertThrows(
            RuntimeException.class,
            () -> dispatcher.listEffectiveTagsForDisplay(METALAKE, objects)));
    Assertions.assertInstanceOf(
        ListTagsInfoForMetadataObjectFailureEvent.class, listener.getPostEvents().get(1));
    verify(delegate, times(2)).listEffectiveTagsForDisplay(METALAKE, objects);
  }
}
