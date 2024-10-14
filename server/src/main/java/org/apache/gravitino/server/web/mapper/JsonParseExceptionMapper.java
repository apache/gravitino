/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.server.web.mapper;

import com.fasterxml.jackson.core.JsonParseException;
import javax.annotation.Priority;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import org.apache.gravitino.server.web.Utils;

/**
 * JsonParseExceptionMapper is used for returning consistent format of response when an illegal Json
 * request is sent. This class overrides the built-in JsonParseExceptionMapper defined in Jersey.
 */
@Priority(1)
public class JsonParseExceptionMapper implements ExceptionMapper<JsonParseException> {
  @Override
  public Response toResponse(JsonParseException e) {
    String errorMsg = "Malformed json request";
    return Utils.illegalArguments(IllegalArgumentException.class.getSimpleName(), errorMsg, e);
  }
}
