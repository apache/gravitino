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

package org.apache.gravitino.maintenance.optimizer;

import com.google.common.base.Preconditions;
import java.io.BufferedWriter;
import java.io.File;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.maintenance.optimizer.command.AppendMetricsCommand;
import org.apache.gravitino.maintenance.optimizer.command.ListJobMetricsCommand;
import org.apache.gravitino.maintenance.optimizer.command.ListTableMetricsCommand;
import org.apache.gravitino.maintenance.optimizer.command.MonitorMetricsCommand;
import org.apache.gravitino.maintenance.optimizer.command.OptimizerCommandContext;
import org.apache.gravitino.maintenance.optimizer.command.OptimizerCommandExecutor;
import org.apache.gravitino.maintenance.optimizer.command.SubmitStrategyJobsCommand;
import org.apache.gravitino.maintenance.optimizer.command.SubmitUpdateStatsJobCommand;
import org.apache.gravitino.maintenance.optimizer.command.UpdateStatisticsCommand;
import org.apache.gravitino.maintenance.optimizer.command.rule.CommandRules;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.StatisticsInputContent;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** CLI entry point for optimizer actions. */
public class OptimizerCmd {
  private static final Logger LOG = LoggerFactory.getLogger(OptimizerCmd.class);
  private static final String DEFAULT_CONF_PATH =
      Paths.get("conf", "gravitino-optimizer.conf").toString();
  private static final long DEFAULT_RANGE_SECONDS = 24 * 3600L;
  private static final Set<CliOption> GLOBAL_OPTION_SPECS = EnumSet.of(CliOption.CONF_PATH);
  private static final String DEFAULT_USAGE =
      "./bin/gravitino-optimizer.sh --type <command> [options]";
  private static final Map<OptimizerCommandType, CommandOptionSpec> COMMAND_OPTION_SPECS =
      Map.of(
          OptimizerCommandType.SUBMIT_STRATEGY_JOBS,
          CommandOptionSpec.of(
              EnumSet.of(CliOption.IDENTIFIERS, CliOption.STRATEGY_NAME),
              EnumSet.of(CliOption.DRY_RUN, CliOption.LIMIT),
              "Submit jobs for identifiers that match the given strategy name.",
              "./bin/gravitino-optimizer.sh --type submit-strategy-jobs --identifiers "
                  + "c.db.t1,c.db.t2 --strategy-name compaction-high-file-count "
                  + "--dry-run --limit 10"),
          OptimizerCommandType.UPDATE_STATISTICS,
          CommandOptionSpec.of(
              EnumSet.of(CliOption.CALCULATOR_NAME),
              EnumSet.of(CliOption.IDENTIFIERS, CliOption.STATISTICS_PAYLOAD, CliOption.FILE_PATH),
              "Calculate and persist table or partition statistics.",
              "./bin/gravitino-optimizer.sh --type update-statistics --calculator-name "
                  + "local-stats-calculator --file-path ./table-stats.jsonl",
              statisticsInputRules()),
          OptimizerCommandType.APPEND_METRICS,
          CommandOptionSpec.of(
              EnumSet.of(CliOption.CALCULATOR_NAME),
              EnumSet.of(CliOption.IDENTIFIERS, CliOption.STATISTICS_PAYLOAD, CliOption.FILE_PATH),
              "Calculate and append table or job metrics.",
              "./bin/gravitino-optimizer.sh --type append-metrics --calculator-name "
                  + "local-stats-calculator --statistics-payload '<jsonl-lines>'",
              statisticsInputRules()),
          OptimizerCommandType.MONITOR_METRICS,
          CommandOptionSpec.of(
              EnumSet.of(CliOption.IDENTIFIERS, CliOption.ACTION_TIME),
              EnumSet.of(CliOption.RANGE_SECONDS, CliOption.PARTITION_PATH),
              "Evaluate monitor rules for metrics at the given action time.",
              "./bin/gravitino-optimizer.sh --type monitor-metrics --identifiers c.db.t "
                  + "--action-time 1735689600"),
          OptimizerCommandType.LIST_TABLE_METRICS,
          CommandOptionSpec.of(
              EnumSet.of(CliOption.IDENTIFIERS),
              EnumSet.of(CliOption.PARTITION_PATH),
              "List stored table or partition metrics.",
              "./bin/gravitino-optimizer.sh --type list-table-metrics --identifiers c.db.t"),
          OptimizerCommandType.LIST_JOB_METRICS,
          CommandOptionSpec.of(
              EnumSet.of(CliOption.IDENTIFIERS),
              EnumSet.noneOf(CliOption.class),
              "List stored job metrics.",
              "./bin/gravitino-optimizer.sh --type list-job-metrics --identifiers c.db.job"),
          OptimizerCommandType.SUBMIT_UPDATE_STATS_JOB,
          CommandOptionSpec.of(
              EnumSet.of(CliOption.IDENTIFIERS),
              EnumSet.of(
                  CliOption.DRY_RUN,
                  CliOption.LIMIT,
                  CliOption.UPDATE_MODE,
                  CliOption.TARGET_FILE_SIZE_BYTES,
                  CliOption.UPDATER_OPTIONS,
                  CliOption.UPDATER_OPTIONS_FILE,
                  CliOption.SPARK_CONF,
                  CliOption.SPARK_CONF_FILE),
              "Submit built-in Iceberg update stats jobs for given table identifiers.",
              "./bin/gravitino-optimizer.sh --type submit-update-stats-job --identifiers "
                  + "rest.ab.t1,rest.ab.t2 --update-mode stats --dry-run",
              updateStatsJobInputRules()));
  private static final Map<OptimizerCommandType, OptimizerCommandExecutor> COMMAND_HANDLERS =
      Map.of(
          OptimizerCommandType.SUBMIT_STRATEGY_JOBS, new SubmitStrategyJobsCommand(),
          OptimizerCommandType.UPDATE_STATISTICS, new UpdateStatisticsCommand(),
          OptimizerCommandType.APPEND_METRICS, new AppendMetricsCommand(),
          OptimizerCommandType.MONITOR_METRICS, new MonitorMetricsCommand(),
          OptimizerCommandType.LIST_TABLE_METRICS, new ListTableMetricsCommand(),
          OptimizerCommandType.LIST_JOB_METRICS, new ListJobMetricsCommand(),
          OptimizerCommandType.SUBMIT_UPDATE_STATS_JOB, new SubmitUpdateStatsJobCommand());
  private static final String LOCAL_STATS_CALCULATOR_NAME = "local-stats-calculator";

  static {
    validateCommandDefinitions();
  }

  public static void main(String[] args) {
    run(args, System.out, System.err);
  }

  static void run(String[] args, PrintStream out, PrintStream err) {
    Options options = buildOptions();

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd;
    OptimizerCommandType optimizerType = null;
    try {
      cmd = parser.parse(options, args);
      if (cmd.hasOption(CliOption.HELP.longOpt())) {
        String helpType = cmd.getOptionValue(CliOption.TYPE.longOpt());
        if (StringUtils.isBlank(helpType)) {
          printGlobalHelp(options, out);
        } else {
          printCommandHelp(OptimizerCommandType.fromString(helpType), out);
        }
        return;
      }

      optimizerType = OptimizerCommandType.fromString(cmd.getOptionValue(CliOption.TYPE.longOpt()));
      validateCommandOptions(cmd, optimizerType);
      String confPath = cmd.getOptionValue(CliOption.CONF_PATH.longOpt(), DEFAULT_CONF_PATH);
      String[] identifiers = cmd.getOptionValues(CliOption.IDENTIFIERS.longOpt());
      String strategyName = cmd.getOptionValue(CliOption.STRATEGY_NAME.longOpt());
      boolean dryRun = cmd.hasOption(CliOption.DRY_RUN.longOpt());
      String limit = cmd.getOptionValue(CliOption.LIMIT.longOpt());
      String calculatorName = cmd.getOptionValue(CliOption.CALCULATOR_NAME.longOpt());
      String actionTime = cmd.getOptionValue(CliOption.ACTION_TIME.longOpt());
      String rangeSeconds =
          cmd.getOptionValue(
              CliOption.RANGE_SECONDS.longOpt(), Long.toString(DEFAULT_RANGE_SECONDS));
      String partitionPathRaw = cmd.getOptionValue(CliOption.PARTITION_PATH.longOpt());
      String updateMode = cmd.getOptionValue(CliOption.UPDATE_MODE.longOpt());
      String targetFileSizeBytes = cmd.getOptionValue(CliOption.TARGET_FILE_SIZE_BYTES.longOpt());
      String updaterOptions = cmd.getOptionValue(CliOption.UPDATER_OPTIONS.longOpt());
      String updaterOptionsFile = cmd.getOptionValue(CliOption.UPDATER_OPTIONS_FILE.longOpt());
      String sparkConf = cmd.getOptionValue(CliOption.SPARK_CONF.longOpt());
      String sparkConfFile = cmd.getOptionValue(CliOption.SPARK_CONF_FILE.longOpt());
      String statisticsPayload = cmd.getOptionValue(CliOption.STATISTICS_PAYLOAD.longOpt());
      String filePath = cmd.getOptionValue(CliOption.FILE_PATH.longOpt());
      Optional<StatisticsInputContent> statisticsInputContent =
          buildStatisticsInputContent(statisticsPayload, filePath);
      OptimizerCommandContext context =
          new OptimizerCommandContext(
              new OptimizerEnv(loadOptimizerConfig(confPath)),
              identifiers,
              strategyName,
              dryRun,
              limit,
              calculatorName,
              actionTime,
              rangeSeconds,
              partitionPathRaw,
              updateMode,
              targetFileSizeBytes,
              updaterOptions,
              updaterOptionsFile,
              sparkConf,
              sparkConfFile,
              statisticsInputContent,
              out);
      executeCommand(optimizerType, context);
    } catch (ParseException e) {
      err.println(e.getMessage());
      printGlobalHelp(options, out);
      LOG.error("Error parsing command line arguments", e);
    } catch (IllegalArgumentException e) {
      err.println(e.getMessage());
      if (optimizerType != null) {
        printCommandHelp(optimizerType, out);
      } else {
        printGlobalHelp(options, out);
      }
      LOG.error("Command validation failed", e);
    } catch (Exception e) {
      err.println(e.getMessage());
      LOG.error("Error executing optimizer command", e);
    }
  }

  private static OptimizerConfig loadOptimizerConfig(String confPath) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(confPath), "Optimizer config path is empty.");
    File confFile = Paths.get(confPath).toFile();
    if (!Files.exists(confFile.toPath())) {
      throw new IllegalArgumentException(
          "Specified optimizer config file does not exist: " + confPath);
    }

    OptimizerConfig config = new OptimizerConfig();
    try {
      config.loadFromProperties(config.loadPropertiesFromFile(confFile));
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to load optimizer config: " + confPath, e);
    }
    return config;
  }

  private static Optional<StatisticsInputContent> buildStatisticsInputContent(
      String statisticsPayload, String filePath) {
    boolean hasPayload = StringUtils.isNotBlank(statisticsPayload);
    boolean hasFilePath = StringUtils.isNotBlank(filePath);
    Preconditions.checkArgument(
        !(hasPayload && hasFilePath),
        "--statistics-payload and --file-path cannot be used together");
    if (hasPayload) {
      return Optional.of(StatisticsInputContent.fromPayload(statisticsPayload));
    }
    if (hasFilePath) {
      return Optional.of(StatisticsInputContent.fromFilePath(filePath));
    }
    return Optional.empty();
  }

  private static void validateCommandOptions(CommandLine cmd, OptimizerCommandType optimizerType) {
    CommandOptionSpec spec = COMMAND_OPTION_SPECS.get(optimizerType);
    Preconditions.checkArgument(spec != null, "Missing command option spec for %s", optimizerType);

    List<CliOption> missingRequiredOptions =
        spec.requiredOptions().stream()
            .filter(option -> !CommandRules.hasEffectiveValue(cmd, option.longOpt()))
            .toList();
    Preconditions.checkArgument(
        missingRequiredOptions.isEmpty(),
        "Missing required options for command '%s': %s. Optional: %s",
        optimizerType.toCliType(),
        joinOptions(missingRequiredOptions),
        joinOptions(spec.optionalOptions()));

    Set<String> allowedOptions =
        Stream.concat(
                Stream.concat(GLOBAL_OPTION_SPECS.stream(), spec.requiredOptions().stream()),
                spec.optionalOptions().stream())
            .map(CliOption::longOpt)
            .collect(Collectors.toSet());
    allowedOptions.add(CliOption.TYPE.longOpt());

    List<String> unsupportedOptions =
        Arrays.stream(cmd.getOptions())
            .map(Option::getLongOpt)
            .filter(StringUtils::isNotBlank)
            .filter(longOpt -> !allowedOptions.contains(longOpt))
            .map(longOpt -> "--" + longOpt)
            .toList();
    Preconditions.checkArgument(
        unsupportedOptions.isEmpty(),
        "Unsupported options for command '%s': %s. Optional: %s",
        optimizerType.toCliType(),
        String.join(",", unsupportedOptions),
        joinOptions(spec.optionalOptions()));

    spec.rules().validate(cmd, optimizerType.toCliType());
  }

  private static String joinOptions(Set<CliOption> options) {
    if (options.isEmpty()) {
      return "<none>";
    }
    return options.stream().map(option -> "--" + option.longOpt()).collect(Collectors.joining(","));
  }

  private static String joinOptions(List<CliOption> options) {
    if (options.isEmpty()) {
      return "<none>";
    }
    return options.stream().map(option -> "--" + option.longOpt()).collect(Collectors.joining(","));
  }

  private static CommandRules.ValidationPlan statisticsInputRules() {
    return CommandRules.newBuilder()
        .addMutuallyExclusive(
            List.of(CliOption.STATISTICS_PAYLOAD.longOpt(), CliOption.FILE_PATH.longOpt()),
            "--statistics-payload and --file-path cannot be used together")
        .addRequireAnyWhenOption(
            CliOption.CALCULATOR_NAME.longOpt(),
            value -> LOCAL_STATS_CALCULATOR_NAME.equals(value),
            List.of(CliOption.STATISTICS_PAYLOAD.longOpt(), CliOption.FILE_PATH.longOpt()),
            "Command '%s' with --calculator-name %s requires one of --statistics-payload "
                + "or --file-path.",
            LOCAL_STATS_CALCULATOR_NAME)
        .addForbidWhenOption(
            CliOption.CALCULATOR_NAME.longOpt(),
            value -> !LOCAL_STATS_CALCULATOR_NAME.equals(value),
            List.of(CliOption.STATISTICS_PAYLOAD.longOpt(), CliOption.FILE_PATH.longOpt()),
            "--statistics-payload and --file-path are only supported when --calculator-name is %s.",
            LOCAL_STATS_CALCULATOR_NAME)
        .build();
  }

  private static CommandRules.ValidationPlan updateStatsJobInputRules() {
    return CommandRules.newBuilder()
        .addMutuallyExclusive(
            List.of(CliOption.UPDATER_OPTIONS.longOpt(), CliOption.UPDATER_OPTIONS_FILE.longOpt()),
            "--updater-options and --updater-options-file cannot be used together")
        .addMutuallyExclusive(
            List.of(CliOption.SPARK_CONF.longOpt(), CliOption.SPARK_CONF_FILE.longOpt()),
            "--spark-conf and --spark-conf-file cannot be used together")
        .build();
  }

  private static void printGlobalHelp(Options options, PrintStream out) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(
        new PrintWriter(
            new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8)), true),
        formatter.getWidth(),
        DEFAULT_USAGE,
        null,
        options,
        formatter.getLeftPadding(),
        formatter.getDescPadding(),
        null,
        true);
  }

  private static Options buildOptions() {
    Options options = new Options();
    for (CliOption cliOption : CliOption.values()) {
      options.addOption(cliOption.toApacheOption());
    }
    return options;
  }

  private static void printCommandHelp(OptimizerCommandType optimizerType, PrintStream out) {
    CommandOptionSpec spec = COMMAND_OPTION_SPECS.get(optimizerType);
    Preconditions.checkArgument(spec != null, "Missing command option spec for %s", optimizerType);
    out.printf("Command: %s%n", optimizerType.toCliType());
    out.printf("Description: %s%n", spec.description());
    out.printf("Required options: %s%n", joinOptions(spec.requiredOptions()));
    out.printf("Optional options: %s%n", joinOptions(spec.optionalOptions()));
    out.printf("Usage example: %s%n", spec.example());
  }

  private static void validateCommandDefinitions() {
    Set<OptimizerCommandType> specCommands = COMMAND_OPTION_SPECS.keySet();
    Set<OptimizerCommandType> handlerCommands = COMMAND_HANDLERS.keySet();
    Preconditions.checkState(
        specCommands.equals(handlerCommands),
        "Inconsistent command definitions. optionSpecs=%s, handlers=%s",
        specCommands,
        handlerCommands);
  }

  private static String typeValues() {
    return COMMAND_OPTION_SPECS.entrySet().stream()
        .map(Map.Entry::getKey)
        .map(OptimizerCommandType::toCliType)
        .sorted()
        .collect(Collectors.joining(","));
  }

  private static void executeCommand(
      OptimizerCommandType optimizerType, OptimizerCommandContext context) throws Exception {
    OptimizerCommandExecutor handler = COMMAND_HANDLERS.get(optimizerType);
    Preconditions.checkArgument(handler != null, "Unsupported optimizer type: %s", optimizerType);
    handler.execute(context);
  }

  private static final class CommandOptionSpec {
    private final Set<CliOption> requiredOptions;
    private final Set<CliOption> optionalOptions;
    private final String description;
    private final String example;
    private final CommandRules.ValidationPlan rules;

    private CommandOptionSpec(
        Set<CliOption> requiredOptions,
        Set<CliOption> optionalOptions,
        String description,
        String example,
        CommandRules.ValidationPlan rules) {
      this.requiredOptions = Collections.unmodifiableSet(EnumSet.copyOf(requiredOptions));
      this.optionalOptions = Collections.unmodifiableSet(EnumSet.copyOf(optionalOptions));
      this.description = description;
      this.example = example;
      this.rules = rules;
    }

    private static CommandOptionSpec of(
        Set<CliOption> requiredOptions,
        Set<CliOption> optionalOptions,
        String description,
        String example) {
      return new CommandOptionSpec(
          requiredOptions, optionalOptions, description, example, CommandRules.emptyPlan());
    }

    private static CommandOptionSpec of(
        Set<CliOption> requiredOptions,
        Set<CliOption> optionalOptions,
        String description,
        String example,
        CommandRules.ValidationPlan rules) {
      return new CommandOptionSpec(requiredOptions, optionalOptions, description, example, rules);
    }

    private Set<CliOption> requiredOptions() {
      return requiredOptions;
    }

    private Set<CliOption> optionalOptions() {
      return optionalOptions;
    }

    private String description() {
      return description;
    }

    private String example() {
      return example;
    }

    private CommandRules.ValidationPlan rules() {
      return rules;
    }
  }

  private enum CliOption {
    TYPE("type", CliOptionArgType.SINGLE, null, "Optimizer type."),
    HELP(
        "help",
        CliOptionArgType.NONE,
        null,
        "Show help. Combine with --type to show command help."),
    CONF_PATH("conf-path", CliOptionArgType.SINGLE, null, "Optimizer configuration path"),
    IDENTIFIERS("identifiers", CliOptionArgType.MULTI, ',', "Comma separated identifier list"),
    STRATEGY_NAME("strategy-name", CliOptionArgType.SINGLE, null, "Strategy name"),
    DRY_RUN("dry-run", CliOptionArgType.NONE, null, "Preview strategy jobs without submitting"),
    LIMIT("limit", CliOptionArgType.SINGLE, null, "Maximum number of strategy jobs to process"),
    CALCULATOR_NAME(
        "calculator-name",
        CliOptionArgType.SINGLE,
        null,
        "The statistics calculator name to compute statistics or metrics"),
    STATISTICS_PAYLOAD(
        "statistics-payload",
        CliOptionArgType.SINGLE,
        null,
        "Inline statistics payload for local-stats-calculator"),
    FILE_PATH(
        "file-path",
        CliOptionArgType.SINGLE,
        null,
        "Path to statistics input file for local-stats-calculator"),
    ACTION_TIME("action-time", CliOptionArgType.SINGLE, null, "Action time in epoch seconds"),
    RANGE_SECONDS(
        "range-seconds", CliOptionArgType.SINGLE, null, "Range seconds for monitor evaluation"),
    PARTITION_PATH(
        "partition-path",
        CliOptionArgType.SINGLE,
        null,
        "Partition path for monitor-metrics "
            + "(JSON array, for example: [{\"p1\":\"v1\"},{\"p2\":\"v2\"}])"),
    UPDATE_MODE(
        "update-mode",
        CliOptionArgType.SINGLE,
        null,
        "Update mode for submit-update-stats-job: stats|metrics|all"),
    TARGET_FILE_SIZE_BYTES(
        "target-file-size-bytes",
        CliOptionArgType.SINGLE,
        null,
        "Target file size bytes for submit-update-stats-job"),
    UPDATER_OPTIONS(
        "updater-options",
        CliOptionArgType.SINGLE,
        null,
        "JSON map for updater options in submit-update-stats-job"),
    UPDATER_OPTIONS_FILE(
        "updater-options-file",
        CliOptionArgType.SINGLE,
        null,
        "Path to JSON file for updater options in submit-update-stats-job"),
    SPARK_CONF(
        "spark-conf",
        CliOptionArgType.SINGLE,
        null,
        "JSON map for Spark configs in submit-update-stats-job"),
    SPARK_CONF_FILE(
        "spark-conf-file",
        CliOptionArgType.SINGLE,
        null,
        "Path to JSON file for Spark configs in submit-update-stats-job");

    private final String longOpt;
    private final CliOptionArgType argType;
    private final Character valueSeparator;
    private final String description;

    CliOption(
        String longOpt, CliOptionArgType argType, Character valueSeparator, String description) {
      this.longOpt = longOpt;
      this.argType = argType;
      this.valueSeparator = valueSeparator;
      this.description = description;
    }

    private String longOpt() {
      return longOpt;
    }

    private Option toApacheOption() {
      String optionDescription =
          this == TYPE ? String.format("Optimizer type: %s", typeValues()) : description;
      Option.Builder builder =
          Option.builder().longOpt(longOpt).required(false).desc(optionDescription);
      if (argType == CliOptionArgType.SINGLE) {
        builder.hasArg();
      } else if (argType == CliOptionArgType.MULTI) {
        builder.hasArgs();
      }
      if (valueSeparator != null) {
        builder.valueSeparator(valueSeparator);
      }
      return builder.build();
    }
  }

  private enum CliOptionArgType {
    NONE,
    SINGLE,
    MULTI
  }

  enum OptimizerCommandType {
    SUBMIT_STRATEGY_JOBS,
    UPDATE_STATISTICS,
    APPEND_METRICS,
    MONITOR_METRICS,
    LIST_TABLE_METRICS,
    LIST_JOB_METRICS,
    SUBMIT_UPDATE_STATS_JOB;

    public static String allValues() {
      return Arrays.stream(values())
          .map(OptimizerCommandType::toCliType)
          .collect(Collectors.joining(","));
    }

    public static OptimizerCommandType fromString(String rawValue) {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(rawValue), "Invalid --type: value is empty.");
      String value = rawValue.trim().toLowerCase(Locale.ROOT);
      Preconditions.checkArgument(
          !value.contains("_"),
          "Invalid --type: %s. Use kebab-case format, for example: update-statistics.",
          rawValue);
      for (OptimizerCommandType type : values()) {
        if (type.toCliType().equals(value)) {
          return type;
        }
      }
      throw new IllegalArgumentException(
          String.format("Invalid --type: %s. Supported values: %s", rawValue, allValues()));
    }

    private String toCliType() {
      return name().toLowerCase(Locale.ROOT).replace('_', '-');
    }
  }
}
