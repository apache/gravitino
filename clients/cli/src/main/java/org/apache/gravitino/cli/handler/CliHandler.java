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

package org.apache.gravitino.cli.handler;

import com.google.common.base.Joiner;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.gravitino.cli.CliFullName;
import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.cli.MainCli;
import picocli.CommandLine;

/** Base class fro all cli handlers. */
public abstract class CliHandler implements Callable<Integer> {
  /** The joiner for comma-separated values. */
  public static final Joiner COMMA_JOINER = Joiner.on(", ").skipNulls();
  /** The full name of the entity, constructed from the metalake, schema, and entity name. */
  protected CliFullName fullName;
  /** The command spec, injected by picocli. */
  @CommandLine.Spec protected CommandLine.Model.CommandSpec spec;

  private String metalake;

  /**
   * Validate the options for the command, check whether the required options are present. or wheter
   * the options are conflict.
   */
  protected void validateOptions(NameValidator validator) {
    this.fullName = new CliFullName(spec);
    this.metalake = fullName.getMetalakeName();
    if (metalake == null) {
      MainCli.exit(-1);
    }

    List<String> missingEntities = validator.getMissingEntities(fullName);
    if (!missingEntities.isEmpty()) {
      throw new RuntimeException(
          ErrorMessages.MISSING_ENTITIES + COMMA_JOINER.join(missingEntities));
    }
  }

  /** {@inheritDoc} */
  @Override
  public Integer call() throws Exception {
    NameValidator validator = createValidator();
    validateOptions(validator);

    return doCall();
  }

  /**
   * Execute the command.
   *
   * @return The exit code of the command.
   * @throws Exception If an error occurs during execution.C
   */
  protected abstract Integer doCall() throws Exception;

  /** Initialize the name option validator for the command. */
  protected abstract NameValidator createValidator();
}
