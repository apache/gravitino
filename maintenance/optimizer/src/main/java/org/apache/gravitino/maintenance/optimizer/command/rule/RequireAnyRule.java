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

package org.apache.gravitino.maintenance.optimizer.command.rule;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.cli.CommandLine;

/** Enforces that at least one option is provided from a given option set. */
final class RequireAnyRule implements CommandRule {
  private final List<String> options;
  private final String messageTemplate;
  private final Object[] formatArgs;

  RequireAnyRule(List<String> options, String messageTemplate, Object... formatArgs) {
    this.options = List.copyOf(options);
    this.messageTemplate = messageTemplate;
    this.formatArgs = Arrays.copyOf(formatArgs, formatArgs.length);
  }

  @Override
  public void validate(CommandLine cmd, String commandType) {
    boolean hasAny = options.stream().anyMatch(option -> RuleUtils.hasEffectiveValue(cmd, option));
    Preconditions.checkArgument(hasAny, messageTemplate, formatArgsWithType(commandType));
  }

  private Object[] formatArgsWithType(String commandType) {
    Object[] args = new Object[formatArgs.length + 1];
    args[0] = commandType;
    System.arraycopy(formatArgs, 0, args, 1, formatArgs.length);
    return args;
  }
}
