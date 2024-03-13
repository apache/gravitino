package com.datastrato.gravitino.dto.requests;

import com.datastrato.gravitino.rest.RESTRequest;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.jackson.Jacksonized;

import javax.annotation.Nullable;
import java.util.Map;

@Getter
@EqualsAndHashCode
@ToString
@Builder
@Jacksonized
public class UserCreateRequest implements RESTRequest {

    @JsonProperty("name")
    private final String name;

    @Nullable
    @JsonProperty("properties")
    private final Map<String, String> properties;

    /**
     * Validates the {@link UserCreateRequest} request.
     *
     * @throws IllegalArgumentException If the request is invalid, this exception is thrown.
     */
    @Override
    public void validate() throws IllegalArgumentException {

    }
}
