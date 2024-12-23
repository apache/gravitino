package org.apache.gravitino.listener.api.event;

public class TagFailureEvent extends FailureEvent {
    public TagFailureEvent(String user, Exception exception) {
        super(user, null, exception);
    }
}
