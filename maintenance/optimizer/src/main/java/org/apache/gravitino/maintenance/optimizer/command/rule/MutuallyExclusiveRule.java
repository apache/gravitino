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
import java.util.List;
import org.apache.commons.cli.CommandLine;

/**
 * Enforces that at most one option is provided from a given option set.
 *
 * <p>Example:
 *
 * <pre>{@code
 * new MutuallyExclusiveRule(
 *     List.of("statistics-payload", "file-path"),
 *     "--statistics-payload and --file-path cannot be used together");
 * }</pre>
 */
final class MutuallyExclusiveRule implements CommandRule {
  private final List<String> options;
  private final String messageTemplate;

  MutuallyExclusiveRule(List<String> options, String messageTemplate) {
    this.options = List.copyOf(options);
    this.messageTemplate = messageTemplate;
  }

  @Override
  public void validate(CommandLine cmd, String commandType) {
    long count =
        options.stream().filter(option -> RuleUtils.hasEffectiveValue(cmd, option)).count();
    Preconditions.checkArgument(count <= 1, messageTemplate);
  }
}
