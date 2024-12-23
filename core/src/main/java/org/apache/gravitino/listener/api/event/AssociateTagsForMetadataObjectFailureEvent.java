package org.apache.gravitino.listener.api.event;

import org.apache.gravitino.MetadataObject;

public class AssociateTagsForMetadataObjectFailureEvent extends TagFailureEvent {
    private final String metalake;
    private final MetadataObject metadataObject;
    private final String[] tagsToAdd;
    private final String[] tagsToRemove;
    public AssociateTagsForMetadataObjectFailureEvent(String user, String metalake, MetadataObject metadataObject, String[] tagsToAdd, String[] tagsToRemove, Exception exception) {
        super(user, null, exception);
        this.metalake = metalake;
        this.metadataObject = metadataObject;
        this.tagsToAdd = tagsToAdd;
        this.tagsToRemove = tagsToRemove;
    }

    public String metalake() {
        return metalake;
    }

    public MetadataObject metadataObject() {
        return metadataObject;
    }

    public String[] tagsToAdd() {
        return tagsToAdd;
    }

    public String[] tagsToRemove() {
        return tagsToRemove;
    }

    @Override
    public OperationType operationType() {
        return OperationType.ASSOCIATE_TAGS_FOR_METADATA_OBJECT;
    }
}
