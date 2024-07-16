/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.rest;

// Referred from Apache Iceberg's RESTMessage implementation
// core/src/main/java/org/apache/iceberg/rest/RESTMessage.java

/**
 * Interface for REST messages.
 *
 * <p>REST messages are objects that are sent to and received from REST endpoints. They are
 * typically used to represent the request and response bodies of REST API calls.
 */
public interface RESTMessage {

  /**
   * Ensures that a constructed instance of a REST message is valid according to the REST spec.
   *
   * <p>This is needed when parsing data that comes from external sources and the object might have
   * been constructed without all the required fields present.
   */
  void validate() throws IllegalArgumentException;
}
