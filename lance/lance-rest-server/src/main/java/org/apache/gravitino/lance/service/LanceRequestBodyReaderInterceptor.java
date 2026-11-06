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
package org.apache.gravitino.lance.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.ext.Provider;
import javax.ws.rs.ext.ReaderInterceptor;
import javax.ws.rs.ext.ReaderInterceptorContext;

/** Keeps JSON request decoding failures in the Lance error-response format. */
@Provider
public class LanceRequestBodyReaderInterceptor implements ReaderInterceptor {

  /**
   * Converts JSON decoding failures before a resource method is invoked.
   *
   * @param context the request entity reader context
   * @return the decoded request entity
   * @throws IOException if a non-JSON input failure occurs
   * @throws WebApplicationException if the JSON request cannot be decoded
   */
  @Override
  public Object aroundReadFrom(ReaderInterceptorContext context)
      throws IOException, WebApplicationException {
    try {
      return context.proceed();
    } catch (JsonProcessingException invalidJson) {
      // Keep the cause for diagnostics without echoing request contents in the HTTP error body.
      throw new BadRequestException("Invalid JSON request", invalidJson);
    }
  }
}
