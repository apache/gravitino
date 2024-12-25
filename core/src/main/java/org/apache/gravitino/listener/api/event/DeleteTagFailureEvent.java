package org.apache.gravitino.listener.api.event;

import org.apache.gravitino.NameIdentifier;
public class DeleteTagFailureEvent extends TagFailureEvent {
    private final String metalake;
    private final String name;
    public DeleteTagFailureEvent(String user, String metalake, String name, Exception exception) {
        super(user, NameIdentifier.of(metalake), exception);
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