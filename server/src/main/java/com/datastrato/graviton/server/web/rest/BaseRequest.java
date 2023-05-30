package com.datastrato.graviton.server.web.rest;

public abstract class BaseRequest {

  /**
   * Validates the arguments of the request.
   *
   * @throws IllegalArgumentException if the validation is failed.
   */
  public abstract void validate() throws IllegalArgumentException;
}
