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

/**
 * Factory and utility methods for optimizer command validation rules.
 *
 * <p>The rule system is designed for command-specific option checks that go beyond simple
 * required/optional definitions. Typical scenarios:
 *
 * <ul>
 *   <li>Mutual exclusion: only one of two inputs can be provided.
 *   <li>Conditional requirement: when a trigger option has some value, another option set becomes
 *       mandatory.
 *   <li>Conditional forbid: when a trigger option has some value, some options must not be used.
 * </ul>
 *
 * <p>Example:
 *
 * <pre>{@code
 * CommandRules.ValidationPlan rules =
 *     CommandRules.newBuilder()
 *         .addMutuallyExclusive(
 *             List.of("statistics-payload", "file-path"),
 *             "--statistics-payload and --file-path cannot be used together")
 *         .addRequireAnyWhenOption(
 *             "calculator-name",
 *             "local-stats-calculator"::equals,
 *             List.of("statistics-payload", "file-path"),
 *             "Command '%s' with --calculator-name local-stats-calculator requires one of "
 *                 + "--statistics-payload or --file-path.")
 *         .addForbidWhenOption(
 *             "calculator-name",
 *             value -> !"local-stats-calculator".equals(value),
 *             List.of("statistics-payload", "file-path"),
 *             "--statistics-payload and --file-path are only supported when --calculator-name "
 *                 + "is local-stats-calculator.")
 *         .build();
 *
 * rules.validate(cmd, "update-statistics");
 * }</pre>
 */
public final class CommandRules {
  private CommandRules() {}

  /** Returns a no-op plan, useful for commands that have no extra rule checks. */
  public static ValidationPlan emptyPlan() {
    return new ValidationPlan(List.of());
  }

  /** Returns a mutable builder for composing a {@link ValidationPlan}. */
  public static Builder newBuilder() {
    return new Builder();
  }

  /** Returns true if an option is effectively provided (has argument value(s) and not blank). */
  public static boolean hasEffectiveValue(CommandLine cmd, String longOpt) {
    return RuleUtils.hasEffectiveValue(cmd, longOpt);
  }

  /** Immutable executable rule set for one command type. */
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

    /**
     * Adds a mutual exclusion rule.
     *
     * <p>Example: forbid using both {@code --statistics-payload} and {@code --file-path} in the
     * same command.
     */
    public Builder addMutuallyExclusive(List<String> options, String messageTemplate) {
      rules.add(new MutuallyExclusiveRule(options, messageTemplate));
      return this;
    }

    /**
     * Adds a rule that requires at least one option in {@code options} to be provided.
     *
     * <p>The {@code messageTemplate} can include {@code %s}. The first placeholder is always bound
     * to {@code commandType} at runtime; the rest are from {@code formatArgs}.
     */
    public Builder addRequireAny(
        List<String> options, String messageTemplate, Object... formatArgs) {
      rules.add(new RequireAnyRule(options, messageTemplate, formatArgs));
      return this;
    }

    /**
     * Adds a rule that forbids {@code forbiddenOptions} when the trigger option matches the
     * predicate.
     *
     * <p>Example: when {@code --calculator-name != local-stats-calculator}, forbid {@code
     * --statistics-payload} and {@code --file-path}.
     */
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

    /**
     * Adds a rule that requires at least one option when the trigger option matches the predicate.
     *
     * <p>Example: when {@code --calculator-name == local-stats-calculator}, require one of {@code
     * --statistics-payload} or {@code --file-path}.
     */
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

    /**
     * Adds a conditional rule that delegates to another rule when the trigger option matches the
     * predicate.
     *
     * @param triggerOption The option whose value is evaluated by the predicate.
     * @param triggerPredicate The predicate used to test the trigger option's value.
     * @param delegate The rule to apply when the predicate evaluates to {@code true}.
     * @return This builder for chaining.
     */
    private Builder addRequireWhenOption(
        String triggerOption, Predicate<String> triggerPredicate, CommandRule delegate) {
      rules.add(new RequireWhenOptionRule(triggerOption, triggerPredicate, delegate));
      return this;
    }

    /** Finalizes the builder and returns an immutable validation plan. */
    public ValidationPlan build() {
      return new ValidationPlan(rules);
    }
  }
}
