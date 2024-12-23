package org.apache.gravitino.listener.api.event;

import org.apache.gravitino.MetadataObject;

public class GetTagForMetadataObjectFailureEvent extends TagFailureEvent {
    private final String metalake;
    private final MetadataObject metadataObject;
    private final String name;
    public GetTagForMetadataObjectFailureEvent(String user, String metalake, MetadataObject metadataObject, String name, Exception exception) {
        super(user, null, exception);
        this.metalake = metalake;
        this.metadataObject = metadataObject;
        this.name = name;
    }

    public String metalake() {
        return metalake;
    }

    public MetadataObject metadataObject() {
        return metadataObject;
    }

    public String name() {
        return name;
    }

    @Override
    public OperationType operationType() {
        return OperationType.GET_TAG_FOR_METADATA_OBJECT;
    }
}
