package org.apache.gravitino.listener.api.event;

import org.apache.gravitino.tag.TagManager;
public class DeleteTagFailureEvent extends TagFailureEvent {
    private final String metalake;
    private final String name;
    public DeleteTagFailureEvent(String user, String metalake, String name, Exception exception) {
        super(user, TagManager.ofTagIdent(metalake, name), exception);
        this.metalake = metalake;
        this.name = name;
    }

    public String metalake() {
        return metalake;
    }

    public String name() {
        return name;
    }

    @Override
    public OperationType operationType() {
        return OperationType.DELETE_TAG;
    }
}