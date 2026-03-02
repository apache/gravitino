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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import org.apache.commons.cli.CommandLine;

/** Factory and utility methods for command validation rules. */
public final class CommandRules {
  private CommandRules() {}

  public static ValidationPlan emptyPlan() {
    return new ValidationPlan(List.of());
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static boolean hasEffectiveValue(CommandLine cmd, String longOpt) {
    return RuleUtils.hasEffectiveValue(cmd, longOpt);
  }

  public static final class ValidationPlan {
    private final List<CommandRule> rules;

    private ValidationPlan(List<CommandRule> rules) {
      this.rules = List.copyOf(rules);
    }

    public void validate(CommandLine cmd, String commandType) {
      for (CommandRule rule : rules) {
        rule.validate(cmd, commandType);
      }
    }
  }

  public static final class Builder {
    private final List<CommandRule> rules = new ArrayList<>();

    public Builder addMutuallyExclusive(List<String> options, String messageTemplate) {
      rules.add(new MutuallyExclusiveRule(options, messageTemplate));
      return this;
    }

    public Builder addRequireAny(
        List<String> options, String messageTemplate, Object... formatArgs) {
      rules.add(new RequireAnyRule(options, messageTemplate, formatArgs));
      return this;
    }

    public Builder addForbidWhenOption(
        String triggerOption,
        Predicate<String> triggerPredicate,
        List<String> forbiddenOptions,
        String messageTemplate,
        Object... formatArgs) {
      rules.add(
          new ForbidWhenOptionRule(
              triggerOption, triggerPredicate, forbiddenOptions, messageTemplate, formatArgs));
      return this;
    }

    public Builder addRequireAnyWhenOption(
        String triggerOption,
        Predicate<String> triggerPredicate,
        List<String> options,
        String messageTemplate,
        Object... formatArgs) {
      return addRequireWhenOption(
          triggerOption,
          triggerPredicate,
          new RequireAnyRule(options, messageTemplate, formatArgs));
    }

    private Builder addRequireWhenOption(
        String triggerOption, Predicate<String> triggerPredicate, CommandRule delegate) {
      rules.add(new RequireWhenOptionRule(triggerOption, triggerPredicate, delegate));
      return this;
    }

    public ValidationPlan build() {
      return new ValidationPlan(rules);
    }
  }
}
