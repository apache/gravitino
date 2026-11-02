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
package org.apache.gravitino.iceberg.service;

import java.io.IOException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.ext.ReaderInterceptor;
import javax.ws.rs.ext.ReaderInterceptorContext;
import org.apache.iceberg.exceptions.BadRequestException;

/**
 * Reports a request body that Iceberg's parsers reject as a 400 Bad Request.
 *
 * <p>Iceberg's JSON parsers reject invalid input, such as an unknown metadata update action or
 * requirement type, with {@link IllegalArgumentException}, {@link UnsupportedOperationException} or
 * {@link IllegalStateException} instead of a Jackson exception. Raised while the body is read,
 * these would otherwise be reported as server errors or as 406 Not Acceptable.
 */
public class IcebergRequestBodyReaderInterceptor implements ReaderInterceptor {

  @Override
  public Object aroundReadFrom(ReaderInterceptorContext context) throws IOException {
    try {
      return context.proceed();
    } catch (WebApplicationException e) {
      throw e;
    } catch (IllegalArgumentException
        | UnsupportedOperationException
        | IllegalStateException
        | NullPointerException e) {
      throw new BadRequestException(
          "Invalid request body: %s",
          e.getMessage() == null ? "a required field is missing or invalid" : e.getMessage());
    }
  }
}
