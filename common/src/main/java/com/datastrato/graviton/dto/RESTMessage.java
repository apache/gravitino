package com.datastrato.graviton.dto;

public interface RESTMessage {

  /**
   * Ensures that a constructed instance of a REST message is valid according to the REST spec.
   *
   * <p>This is needed when parsing data that comes from external sources and the object might have
   * been constructed without all the required fields present.
   */
  void validate() throws IllegalArgumentException;
}
