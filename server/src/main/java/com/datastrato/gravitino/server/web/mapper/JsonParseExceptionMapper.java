/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.mapper;

import com.datastrato.gravitino.server.web.Utils;
import com.fasterxml.jackson.core.JsonParseException;
import javax.annotation.Priority;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

/**
 * JsonParseExceptionMapper is used for returning consistent format of response when an illegal Json
 * request is sent. This class overrides the built-in JsonParseExceptionMapper defined in Jersey.
 */
@Priority(1)
public class JsonParseExceptionMapper implements ExceptionMapper<JsonParseException> {
  @Override
  public Response toResponse(JsonParseException e) {
    String errorMsg = "Malformed json request";
    return Utils.illegalArguments(errorMsg, e);
  }
}
