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

package org.apache.gravitino.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.function.Consumer;
import org.apache.gravitino.dto.responses.ErrorResponse;

/**
 * The ErrorHandler class is an abstract class specialized for handling ErrorResponse objects.
 * Subclasses of ErrorHandler must implement the parseResponse method to provide custom parsing
 * logic for different types of errors.
 */
public abstract class ErrorHandler implements Consumer<ErrorResponse> {

  /**
   * Parses the response and creates an ErrorResponse object based on the response code and the JSON
   * data using the provided ObjectMapper.
   *
   * @param code The response code indicating the error status.
   * @param json The JSON data representing the error response.
   * @param mapper The ObjectMapper used to deserialize the JSON data.
   * @return The ErrorResponse object representing the parsed error response.
   */
  public abstract ErrorResponse parseResponse(int code, String json, ObjectMapper mapper);
}
