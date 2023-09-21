/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.client;

import com.datastrato.graviton.dto.responses.ErrorResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.function.Consumer;

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
