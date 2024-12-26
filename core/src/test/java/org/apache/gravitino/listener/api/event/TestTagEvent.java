package org.apache.gravitino.listener.api.event;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.listener.DummyEventListener;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.TagEventDispatcher;

import org.apache.gravitino.listener.api.info.TagInfo;
import org.apache.gravitino.tag.Tag;
import org.apache.gravitino.tag.TagDispatcher;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.util.Arrays;
import java.util.Map;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestTagEvent {
    private TagEventDispatcher dispatcher;
    private TagEventDispatcher failureDispatcher;
    private DummyEventListener dummyEventListener;
    private Tag tag;

    @BeforeAll
    void init() {
        this.tag = mockTag();
        this.dummyEventListener = new DummyEventListener();
        EventBus eventBus = new EventBus(Arrays.asList(dummyEventListener));
        TagDispatcher tagDispatcher = mockTagDispatcher();
        this.dispatcher = new TagEventDispatcher(eventBus, tagDispatcher);
    }

    @Test
    void testCreateTagEvent() {
        String metalake = "metalake";
        NameIdentifier identifier = NameIdentifier.of(metalake);
        TagInfo tagInfo = new TagInfo("tagName", "test comment", ImmutableMap.of("key", "value"));

        dispatcher.createTag(metalake, tagInfo.name(), tagInfo.comment(), tagInfo.properties());
        Event event = dummyEventListener.popPostEvent();

        Assertions.assertEquals(identifier, event.identifier());
        Assertions.assertEquals(CreateTagEvent.class, event.getClass());
        Assertions.assertEquals(tagInfo.name(), ((CreateTagEvent) event).createdTagInfo().name());
        Assertions.assertEquals(tagInfo.comment(), ((CreateTagEvent) event).createdTagInfo().comment());
        Assertions.assertEquals(tagInfo.properties(), ((CreateTagEvent) event).createdTagInfo().properties());
        Assertions.assertEquals(OperationType.CREATE_TAG, event.operationType());
        Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());
    }

    @Test
    void testListTagsEvent() {
        String metalake = "metalake";
        NameIdentifier identifier = NameIdentifier.of(metalake);
        String[] tagNames = new String[] {"tag1", "tag2"};

        when(dispatcher.listTags(metalake)).thenReturn(tagNames);

        String[] result = dispatcher.listTags(metalake);
        Event event = dummyEventListener.popPostEvent();

        Assertions.assertEquals(identifier, event.identifier());
        Assertions.assertEquals(ListTagEvent.class, event.getClass());
        Assertions.assertEquals(metalake, ((ListTagEvent)event).metalake());
        Assertions.assertEquals(OperationType.LIST_TAG, event.operationType());
        Assertions.assertArrayEquals(tagNames, result);
    }



    private Tag mockTag() {
        Tag tag = mock(Tag.class);
        when(tag.name()).thenReturn("tagName");
        when(tag.comment()).thenReturn("test comment");
        when(tag.properties()).thenReturn(ImmutableMap.of("key", "value"));
        return tag;
    }

    private TagDispatcher mockTagDispatcher() {
        TagDispatcher dispatcher = mock(TagDispatcher.class);
        when(dispatcher.createTag(
                any(String.class), any(String.class), any(String.class), any(Map.class)))
                .thenReturn(tag);
        return dispatcher;
    }

}
