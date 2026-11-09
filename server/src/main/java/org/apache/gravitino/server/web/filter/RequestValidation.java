/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.server.web.filter;

import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.gravitino.exceptions.BadRequestException;

/** Converts failures from explicit client-input validation into request errors. */
public final class RequestValidation {

  private RequestValidation() {}

  /**
   * Validates client input without including authorization or backend operations.
   *
   * @param validation the client-input validation to perform
   * @throws BadRequestException if validation rejects the input
   */
  public static void validate(Runnable validation) {
    parse(
        () -> {
          validation.run();
          return null;
        });
  }

  /**
   * Parses client input and reports invalid arguments as a bad request.
   *
   * @param parser the client-input parser, excluding configuration and authorization operations
   * @param <T> the parsed result type
   * @return the parsed result
   * @throws BadRequestException if parsing rejects the input
   */
  public static <T> T parse(Supplier<T> parser) {
    return parse(parser, e -> new BadRequestException(e, "%s", e.getMessage()));
  }

  /**
   * Parses client input while preserving the caller's request-error type and message.
   *
   * <p>Only explicit client-input parsing belongs in this scope. Authorizer construction,
   * configuration loading, expression evaluation, and backend calls must remain outside it.
   *
   * @param parser the client-input parser
   * @param errorMapper converts an input validation failure into the appropriate request error
   * @param <T> the parsed result type
   * @return the parsed result
   */
  public static <T> T parse(
      Supplier<T> parser,
      Function<IllegalArgumentException, ? extends RuntimeException> errorMapper) {
    try {
      return parser.get();
    } catch (IllegalArgumentException e) {
      throw errorMapper.apply(e);
    }
  }
}
