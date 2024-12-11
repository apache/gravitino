package org.apache.gravitino.listener;

import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NoSuchTagException;
import org.apache.gravitino.tag.Tag;
import org.apache.gravitino.tag.TagDispatcher;

import java.io.IOException;
import java.util.List;


public class TagEventDispatcher implements TagDispatcher {
    private final EventBus eventBus;
    private final TagDispatcher dispatcher;

    public TagEventDispatcher(EventBus eventBus, TagDispatcher dispatcher) {
        this.eventBus = eventBus;
        this.dispatcher = dispatcher;
    }


    @Override
    public String[] listTags(String metalake) {
        return new String[0];
    }

    @Override
    public Tag[] listTagsInfo(String metalake) {
        return new Tag[0];
    }

    @Override
    public Tag getTag(String metalake, String name) throws NoSuchTagException {
        return null;
    }
}
