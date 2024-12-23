package org.apache.gravitino.listener.api.event;
import org.apache.gravitino.NameIdentifier;

public class TagFailureEvent extends FailureEvent {
    public TagFailureEvent(String user, NameIdentifier identifier, Exception exception) {
        super(user, identifier, exception);
    }
}
