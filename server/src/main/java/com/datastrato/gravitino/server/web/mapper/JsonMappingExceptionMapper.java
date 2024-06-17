/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.mapper;

import com.datastrato.gravitino.server.web.Utils;
import com.fasterxml.jackson.databind.JsonMappingException;
import javax.annotation.Priority;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

/**
 * JsonMappingExceptionMapper is used for returning consistent format of response when an illegal
 * Json request is sent. This class overrides the built-in JsonMappingExceptionMapper defined in
 * Jersey.
 */
@Priority(1)
public class JsonMappingExceptionMapper implements ExceptionMapper<JsonMappingException> {
  @Override
  public Response toResponse(JsonMappingException e) {
    String errorMsg = "Malformed json request";
    return Utils.illegalArguments(errorMsg, e);
  }
}
