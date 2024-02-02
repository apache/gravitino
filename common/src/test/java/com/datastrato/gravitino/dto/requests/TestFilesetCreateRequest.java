/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.requests;

import com.datastrato.gravitino.file.Fileset;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestFilesetCreateRequest {
  @Test
  public void testValidateFilesetCreateRequest() {
    FilesetCreateRequest req =
        FilesetCreateRequest.builder()
            .name(null)
            .type(Fileset.Type.MANAGED)
            .storageLocation(null)
            .comment(null)
            .properties(null)
            .build();
    Assertions.assertThrows(
        IllegalArgumentException.class,
        req::validate,
        "\"name\" field is required and cannot be empty");

    FilesetCreateRequest req1 =
        FilesetCreateRequest.builder()
            .name("name")
            .type(null)
            .storageLocation(null)
            .comment(null)
            .properties(null)
            .build();
    Assertions.assertEquals(Fileset.Type.MANAGED, req1.getType());
    req1.validate();
  }
}
