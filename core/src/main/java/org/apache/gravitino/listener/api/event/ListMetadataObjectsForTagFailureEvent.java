package org.apache.gravitino.listener.api.event;

public class ListMetadataObjectsForTagFailureEvent extends TagFailureEvent {
    private final String metalake;
    private final String name;
    public ListMetadataObjectsForTagFailureEvent(String user, String metalake, String name, Exception exception) {
        super(user, null, exception);
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
        return OperationType.LIST_METADATA_OBJECTS_FOR_TAG;
    }
}
