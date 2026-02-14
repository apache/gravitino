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
import java.util.function.Predicate;
import org.apache.commons.cli.CommandLine;

/** Forbids options when the trigger option value satisfies the predicate. */
final class ForbidWhenOptionRule implements CommandRule {
  private final String triggerOption;
  private final Predicate<String> triggerPredicate;
  private final List<String> forbiddenOptions;
  private final String messageTemplate;
  private final Object[] formatArgs;

  ForbidWhenOptionRule(
      String triggerOption,
      Predicate<String> triggerPredicate,
      List<String> forbiddenOptions,
      String messageTemplate,
      Object... formatArgs) {
    this.triggerOption = triggerOption;
    this.triggerPredicate = triggerPredicate;
    this.forbiddenOptions = List.copyOf(forbiddenOptions);
    this.messageTemplate = messageTemplate;
    this.formatArgs = Arrays.copyOf(formatArgs, formatArgs.length);
  }

  @Override
  public void validate(CommandLine cmd, String commandType) {
    String optionValue = cmd.getOptionValue(triggerOption);
    if (!triggerPredicate.test(optionValue)) {
      return;
    }
    boolean hasForbidden =
        forbiddenOptions.stream().anyMatch(option -> RuleUtils.hasEffectiveValue(cmd, option));
    Preconditions.checkArgument(!hasForbidden, messageTemplate, formatArgs);
  }
}
