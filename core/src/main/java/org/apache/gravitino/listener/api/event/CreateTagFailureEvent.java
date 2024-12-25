package org.apache.gravitino.listener.api.event;

import org.apache.gravitino.listener.api.info.TagInfo;
import org.apache.gravitino.NameIdentifier;
public class CreateTagFailureEvent extends TagFailureEvent {
    private final String metalake;
    private final TagInfo tagInfo;
    public CreateTagFailureEvent(String user, String metalake, TagInfo tagInfo, Exception exception) {
        super(user, NameIdentifier.of(metalake), exception);
        this.metalake = metalake;
        this.tagInfo = tagInfo;
    }

    public TagInfo tagInfo() {
        return tagInfo;
    }

    public String metalake() {
        return metalake;
    }

    @Override
    public OperationType operationType() {
        return OperationType.CREATE_TAG;
    }
}
