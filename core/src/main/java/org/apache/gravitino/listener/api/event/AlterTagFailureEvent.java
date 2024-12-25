package org.apache.gravitino.listener.api.event;

import org.apache.gravitino.tag.TagManager;
import org.apache.gravitino.tag.TagChange;

public class AlterTagFailureEvent extends TagFailureEvent {
    private final String metalake;
    private final String name;
    private final TagChange[] changes;
    public AlterTagFailureEvent(String user, String metalake, String name, TagChange[] changes, Exception exception) {
        super(user, TagManager.ofTagIdent(metalake, name), exception);
        this.name = name;
        this.metalake = metalake;
        this.changes = changes;
    }

    public String name() {
        return name;
    }

    public TagChange[] changes() {
        return changes;
    }

    public String metalake() {
        return metalake;
    }

    @Override
    public OperationType operationType() {
        return OperationType.ALTER_TAG;
    }
}
