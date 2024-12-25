package org.apache.gravitino.listener.api.event;

import org.apache.gravitino.NameIdentifier;

public class GetTagFailureEvent extends TagFailureEvent {
    private final String metalake;
    private final String name;
    public GetTagFailureEvent(String user, String metalake, String name, Exception exception) {
        super(user, NameIdentifier.of(metalake), exception);
        this.name = name;
        this.metalake = metalake;
    }

    public String metalake() {
        return metalake;
    }

    public String name() {
        return name;
    }

    @Override
    public OperationType operationType() {
        return OperationType.GET_TAG;
    }
}

