package org.apache.gravitino.listener.api.event;

import org.apache.gravitino.listener.api.info.MetalakeInfo;

public class CreateTagFailureEvent extends TagFailureEvent {
    private final String metalake;
    private final MetalakeInfo metalakeInfo;
    public CreateTagFailureEvent(String user, String metalake, MetalakeInfo metalakeInfo, Exception exception) {
        super(user, exception);
        this.metalake = metalake;
        this.metalakeInfo = metalakeInfo;
    }

    public MetalakeInfo metalakeInfo() {
        return metalakeInfo;
    }

    public String metalake() {
        return metalake;
    }

    @Override
    public OperationType operationType() {
        return OperationType.CREATE_TAG;
    }
}
