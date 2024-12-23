package org.apache.gravitino.listener.api.event;


public class ListTagFailureEvent extends TagFailureEvent {
    private final String metalake;
    public ListTagFailureEvent(String user, String metalake, Exception exception) {
        super(user, exception);
        this.metalake = metalake;
    }

    public String metalake() {
        return metalake;
    }

    @Override
    public OperationType operationType() {
        return OperationType.LIST_TAG;
    }
}
