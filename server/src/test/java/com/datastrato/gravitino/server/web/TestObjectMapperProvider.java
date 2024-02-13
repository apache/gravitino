/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

public class TestObjectMapperProvider {

  @Test
  void testGetContext() {
    ObjectMapperProvider provider = new ObjectMapperProvider();
    Class<Object> someClass = Object.class;

    ObjectMapper objectMapper = provider.getContext(someClass);

    assertNotNull(objectMapper);
    assertEquals(
        JsonInclude.Include.NON_NULL,
        objectMapper.getSerializationConfig().getDefaultPropertyInclusion().getValueInclusion());
  }
}
