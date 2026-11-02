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
package org.apache.gravitino.trino.connector.system.storedprocedure;

import io.trino.spi.TrinoException;
import io.trino.spi.procedure.Procedure;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.trino.connector.GravitinoErrorCode;

/** Gravitino System stored procedure interfaces */
public abstract class GravitinoStoredProcedure {

  /** The name of the optional argument selecting the metalake a procedure operates on. */
  protected static final String METALAKE_ARGUMENT = "METALAKE";

  /**
   * Return the definition of the stored procedure.
   *
   * @return the {@link Procedure} instance
   * @throws Exception if creation of the procedure fails
   */
  public abstract Procedure createStoredProcedure() throws Exception;

  /**
   * Resolves the metalake a procedure call operates on: the METALAKE argument when given, otherwise
   * the metalake the connector is configured with.
   *
   * @param configuredMetalake the metalake from {@code gravitino.metalake}, null when unset
   * @param argument the METALAKE argument of the call, null when not passed
   * @return the metalake name
   * @throws TrinoException if neither is available
   */
  protected static String resolveMetalake(
      @Nullable String configuredMetalake, @Nullable String argument) {
    if (StringUtils.isNotBlank(argument)) {
      return argument.trim();
    }
    if (configuredMetalake != null) {
      return configuredMetalake;
    }
    throw new TrinoException(
        GravitinoErrorCode.GRAVITINO_MISSING_CONFIG,
        "No metalake specified: pass the METALAKE argument or set gravitino.metalake");
  }
}
