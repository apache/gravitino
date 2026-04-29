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
package org.apache.gravitino.catalog.glue;

import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import software.amazon.awssdk.services.glue.model.AlreadyExistsException;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GlueException;
import software.amazon.awssdk.services.glue.model.InvalidInputException;

/** Converts AWS Glue SDK exceptions to Gravitino exceptions. */
final class GlueExceptionConverter {

  private GlueExceptionConverter() {}

  /**
   * Converts a {@link GlueException} to the appropriate Gravitino schema exception.
   *
   * @param e the Glue exception to convert
   * @param context description of the operation context for error messages
   * @return a Gravitino or standard Java runtime exception
   */
  static RuntimeException toSchemaException(GlueException e, String context) {
    if (e instanceof EntityNotFoundException) {
      return new NoSuchSchemaException(e, "%s does not exist", context);
    }
    if (e instanceof AlreadyExistsException) {
      return new SchemaAlreadyExistsException(e, "%s already exists", context);
    }
    if (e instanceof InvalidInputException) {
      return new IllegalArgumentException(context + ": " + e.getMessage(), e);
    }
    return new RuntimeException("Glue error: " + context, e);
  }

  /**
   * Converts a {@link GlueException} to the appropriate Gravitino table exception.
   *
   * @param e the Glue exception to convert
   * @param context description of the operation context for error messages
   * @return a Gravitino or standard Java runtime exception
   */
  static RuntimeException toTableException(GlueException e, String context) {
    if (e instanceof EntityNotFoundException) {
      return new NoSuchTableException(e, "%s does not exist", context);
    }
    if (e instanceof AlreadyExistsException) {
      return new TableAlreadyExistsException(e, "%s already exists", context);
    }
    if (e instanceof InvalidInputException) {
      return new IllegalArgumentException(context + ": " + e.getMessage(), e);
    }
    return new RuntimeException("Glue error: " + context, e);
  }
}
