package org.apache.gravitino.listener.api.event;

import org.apache.gravitino.NameIdentifier;

public class ListTagsInfoFailureEvent extends FailureEvent {
    private final String metalake;

    public ListTagsInfoFailureEvent(String user, String metalake, Exception exception) {
        super(user, NameIdentifier.of(metalake), exception);
        this.metalake = metalake;
    }

    public String metalake() {
        return metalake;
    }

    @Override
    public OperationType operationType() {
        return OperationType.LIST_TAGS_INFO;
    }

}