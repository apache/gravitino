package com.datastrato.graviton.server.web;

import com.datastrato.graviton.json.JsonUtils;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;

@Provider
public class ObjectMapperProvider implements ContextResolver<ObjectMapper> {

  @Override
  public ObjectMapper getContext(Class<?> type) {
    ObjectMapper mapper =
        JsonUtils.objectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);

    return mapper;
  }
}
