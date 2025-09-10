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

package org.apache.gravitino.cli;

import com.google.common.base.Joiner;
import java.util.List;

/** The base class for command handlers. */
public abstract class CommandHandler {
  /** The joiner for comma-separated values. */
  public static final Joiner COMMA_JOINER = Joiner.on(", ").skipNulls();

  /** Handles the command. */
  protected abstract void handle();

  /**
   * Checks if the given list of entities is empty. If it is, prints an error message and exits the
   * program with status -1.
   *
   * @param entities the list of entities to check
   */
  protected void checkEntities(List<String> entities) {
    if (!entities.isEmpty()) {
      System.err.println(ErrorMessages.MISSING_ENTITIES + COMMA_JOINER.join(entities));
      Main.exit(-1);
    }
  }
}
