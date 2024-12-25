package org.apache.gravitino.listener.api.event;

import org.apache.gravitino.MetadataObject;

import org.apache.gravitino.utils.MetadataObjectUtil;
public class ListTagsForMetadataObjectFailureEvent extends TagFailureEvent {
    private final String metalake;
    private final MetadataObject metadataObject;
    public ListTagsForMetadataObjectFailureEvent(String user, String metalake, MetadataObject metadataObject, Exception exception) {
        super(user, MetadataObjectUtil.toEntityIdent(metalake, metadataObject), exception);
        this.metalake = metalake;
        this.metadataObject = metadataObject;
    }

    public String metalake() {
        return metalake;
    }

    public MetadataObject metadataObject() {
        return metadataObject;
    }

    @Override
    public OperationType operationType() {
        return OperationType.LIST_TAGS_FOR_METADATA_OBJECT;
    }
}
