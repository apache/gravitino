package com.datastrato.gravitino.dto;

import com.datastrato.gravitino.Audit;
import com.datastrato.gravitino.User;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.Map;

public class UserDTO implements User {

    @JsonProperty("name")
    private String name;

    @Nullable
    @JsonProperty("properties")
    private Map<String, String> properties;

    @JsonProperty("audit")
    private AuditDTO audit;

    /**
     * Default constructor for Jackson deserialization.
     */
    protected UserDTO() {
    }

    protected UserDTO(
            String name, Map<String, String> properties, AuditDTO audit) {
        this.name = name;
        this.properties = properties;
        this.audit = audit;
    }

    /** @return The name of the User DTO. */
    @Override
    public String name() {
        return name;
    }

    /** @return The properties of the User DTO. */
    @Override
    public Map<String, String> properties() {
        return properties;
    }

    /** @return The audit information of the User DTO. */
    @Override
    public Audit auditInfo() {
        return audit;
    }
}
