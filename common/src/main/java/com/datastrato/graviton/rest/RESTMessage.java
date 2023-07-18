/*·Copyright·2023·Datastrato.·This·software·is·licensed·under·the·Apache·License·version·2.·*/
package com.datastrato.graviton.rest;

public interface RESTMessage {

  /**
   * Ensures that a constructed instance of a REST message is valid according to the REST spec.
   *
   * <p>This is needed when parsing data that comes from external sources and the object might have
   * been constructed without all the required fields present.
   */
  void validate() throws IllegalArgumentException;
}
