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
import java.util.Comparator;
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
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.MetricSample;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.monitor.EvaluationResult;
import org.apache.gravitino.maintenance.optimizer.api.monitor.MetricScope;
import org.apache.gravitino.maintenance.optimizer.api.monitor.MetricsProvider;
import org.apache.gravitino.maintenance.optimizer.command.rule.CommandRules;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.StatisticsInputContent;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.maintenance.optimizer.common.util.ProviderUtils;
import org.apache.gravitino.maintenance.optimizer.monitor.Monitor;
import org.apache.gravitino.maintenance.optimizer.recommender.Recommender;
import org.apache.gravitino.maintenance.optimizer.recommender.util.PartitionUtils;
import org.apache.gravitino.maintenance.optimizer.updater.UpdateType;
import org.apache.gravitino.maintenance.optimizer.updater.Updater;
import org.apache.gravitino.maintenance.optimizer.updater.calculator.local.LocalStatisticsCalculator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** CLI entry point for optimizer actions. */
public class OptimizerCmd {
  private static final Logger LOG = LoggerFactory.getLogger(OptimizerCmd.class);
  private static final String DEFAULT_CONF_PATH =
      Paths.get("conf", "gravitino-optimizer.conf").toString();
  private static final long DEFAULT_RANGE_SECONDS = 24 * 3600L;
  private static final long METRICS_LIST_FROM_SECONDS = 0L;
  private static final long METRICS_LIST_TO_SECONDS = Long.MAX_VALUE;
  private static final Set<CliOption> GLOBAL_OPTION_SPECS = EnumSet.of(CliOption.CONF_PATH);
  private static final String DEFAULT_USAGE =
      "./bin/gravitino-optimizer.sh --type <command> [options]";
  private static final Map<OptimizerCommandType, CommandOptionSpec> COMMAND_OPTION_SPECS =
      Map.of(
          OptimizerCommandType.RECOMMEND_STRATEGY_TYPE,
          CommandOptionSpec.of(
              EnumSet.of(CliOption.IDENTIFIERS, CliOption.STRATEGY_TYPE),
              EnumSet.noneOf(CliOption.class),
              "./bin/gravitino-optimizer.sh --type recommend-strategy-type --identifiers c.db.t1,c.db.t2 --strategy-type compaction"),
          OptimizerCommandType.UPDATE_STATISTICS,
          CommandOptionSpec.of(
              EnumSet.of(CliOption.CALCULATOR_NAME),
              EnumSet.of(CliOption.IDENTIFIERS, CliOption.STATISTICS_PAYLOAD, CliOption.FILE_PATH),
              "./bin/gravitino-optimizer.sh --type update-statistics --calculator-name local-stats-calculator --file-path ./table-stats.jsonl",
              statisticsInputRules()),
          OptimizerCommandType.APPEND_METRICS,
          CommandOptionSpec.of(
              EnumSet.of(CliOption.CALCULATOR_NAME),
              EnumSet.of(CliOption.IDENTIFIERS, CliOption.STATISTICS_PAYLOAD, CliOption.FILE_PATH),
              "./bin/gravitino-optimizer.sh --type append-metrics --calculator-name local-stats-calculator --statistics-payload '<jsonl-lines>'",
              statisticsInputRules()),
          OptimizerCommandType.MONITOR_METRICS,
          CommandOptionSpec.of(
              EnumSet.of(CliOption.IDENTIFIERS, CliOption.ACTION_TIME),
              EnumSet.of(CliOption.RANGE_SECONDS, CliOption.PARTITION_PATH),
              "./bin/gravitino-optimizer.sh --type monitor-metrics --identifiers c.db.t --action-time 1735689600"),
          OptimizerCommandType.LIST_TABLE_METRICS,
          CommandOptionSpec.of(
              EnumSet.of(CliOption.IDENTIFIERS),
              EnumSet.of(CliOption.PARTITION_PATH),
              "./bin/gravitino-optimizer.sh --type list-table-metrics --identifiers c.db.t"),
          OptimizerCommandType.LIST_JOB_METRICS,
          CommandOptionSpec.of(
              EnumSet.of(CliOption.IDENTIFIERS),
              EnumSet.noneOf(CliOption.class),
              "./bin/gravitino-optimizer.sh --type list-job-metrics --identifiers c.db.job"));
  private static final Map<OptimizerCommandType, CommandHandler> COMMAND_HANDLERS =
      Map.of(
          OptimizerCommandType.RECOMMEND_STRATEGY_TYPE, OptimizerCmd::executeRecommendStrategyType,
          OptimizerCommandType.UPDATE_STATISTICS, OptimizerCmd::executeUpdateStatistics,
          OptimizerCommandType.APPEND_METRICS, OptimizerCmd::executeAppendMetrics,
          OptimizerCommandType.MONITOR_METRICS, OptimizerCmd::executeMonitorMetrics,
          OptimizerCommandType.LIST_TABLE_METRICS, OptimizerCmd::executeListTableMetrics,
          OptimizerCommandType.LIST_JOB_METRICS, OptimizerCmd::executeListJobMetrics);
  private static final String LOCAL_STATS_CALCULATOR_NAME = LocalStatisticsCalculator.NAME;

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
      String strategyType = cmd.getOptionValue(CliOption.STRATEGY_TYPE.longOpt());
      String calculatorName = cmd.getOptionValue(CliOption.CALCULATOR_NAME.longOpt());
      String actionTime = cmd.getOptionValue(CliOption.ACTION_TIME.longOpt());
      String rangeSeconds =
          cmd.getOptionValue(
              CliOption.RANGE_SECONDS.longOpt(), Long.toString(DEFAULT_RANGE_SECONDS));
      String partitionPathRaw = cmd.getOptionValue(CliOption.PARTITION_PATH.longOpt());
      String statisticsPayload = cmd.getOptionValue(CliOption.STATISTICS_PAYLOAD.longOpt());
      String filePath = cmd.getOptionValue(CliOption.FILE_PATH.longOpt());
      Optional<StatisticsInputContent> statisticsInputContent =
          buildStatisticsInputContent(statisticsPayload, filePath);
      CommandContext context =
          new CommandContext(
              new OptimizerEnv(loadOptimizerConfig(confPath)),
              identifiers,
              strategyType,
              calculatorName,
              actionTime,
              rangeSeconds,
              partitionPathRaw,
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
    OptimizerConfig config = new OptimizerConfig();
    try {
      if (StringUtils.isNotBlank(confPath)) {
        File confFile = Paths.get(confPath).toFile();
        if (Files.exists(confFile.toPath())) {
          config.loadFromProperties(config.loadPropertiesFromFile(confFile));
          return config;
        }
      }
      config.loadFromFile(confPath);
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to load optimizer config: " + confPath, e);
    }
    return config;
  }

  private static List<NameIdentifier> parseIdentifiers(String[] identifiers) {
    return Arrays.stream(identifiers).map(NameIdentifier::parse).toList();
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

  private static OptimizerEnv withStatisticsInput(
      OptimizerEnv optimizerEnv, Optional<StatisticsInputContent> statisticsInputContent) {
    return statisticsInputContent.map(optimizerEnv::withContent).orElse(optimizerEnv);
  }

  private static long parseLongOption(String optionName, String optionValue, boolean allowZero) {
    try {
      long value = Long.parseLong(optionValue);
      Preconditions.checkArgument(
          allowZero ? value >= 0 : value > 0,
          "Option %s must be %s",
          optionName,
          allowZero ? ">= 0" : "> 0");
      return value;
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          String.format("Option %s must be a valid integer value.", optionName), e);
    }
  }

  private static Optional<PartitionPath> parsePartitionPath(String partitionPathStr) {
    if (StringUtils.isBlank(partitionPathStr)) {
      return Optional.empty();
    }
    String trimmed = partitionPathStr.trim();
    Preconditions.checkArgument(
        trimmed.startsWith("["),
        "--partition-path must be a JSON array, for example: [{\"p1\":\"v1\"},{\"p2\":\"v2\"}]");
    return Optional.of(PartitionUtils.decodePartitionPath(trimmed));
  }

  private static void printEvaluationResult(PrintStream out, EvaluationResult result) {
    MetricScope scope = result.scope();
    String partition =
        scope.partition().map(PartitionPath::toString).orElse("<table-or-job-scope>");
    out.printf(
        "EvaluationResult{scopeType=%s, identifier=%s, partitionPath=%s, evaluation=%s, evaluatorName=%s, actionTimeSeconds=%d, rangeSeconds=%d, beforeMetrics=%s, afterMetrics=%s}%n",
        scope.type(),
        scope.identifier(),
        partition,
        result.evaluation(),
        result.evaluatorName(),
        result.actionTimeSeconds(),
        result.rangeSeconds(),
        result.beforeMetrics(),
        result.afterMetrics());
  }

  private static void printMetricsResult(
      PrintStream out,
      MetricScope.Type scopeType,
      NameIdentifier identifier,
      Optional<PartitionPath> partitionPath,
      Map<String, List<MetricSample>> metrics) {
    String partition = partitionPath.map(PartitionPath::toString).orElse("<table-or-job-scope>");
    out.printf(
        "MetricsResult{scopeType=%s, identifier=%s, partitionPath=%s, metrics=%s}%n",
        scopeType, identifier, partition, formatMetrics(metrics));
  }

  private static String formatMetrics(Map<String, List<MetricSample>> metrics) {
    if (metrics == null || metrics.isEmpty()) {
      return "{}";
    }
    return metrics.entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .map(
            entry ->
                entry.getKey()
                    + "="
                    + formatMetricSamples(entry.getValue() == null ? List.of() : entry.getValue()))
        .collect(Collectors.joining(", ", "{", "}"));
  }

  private static String formatMetricSamples(List<MetricSample> samples) {
    if (samples.isEmpty()) {
      return "[]";
    }
    return samples.stream()
        .sorted(Comparator.comparingLong(MetricSample::timestamp))
        .map(
            sample ->
                String.format(
                    "{timestamp=%d, value=%s}",
                    sample.timestamp(), String.valueOf(sample.statistic().value().value())))
        .collect(Collectors.joining(", ", "[", "]"));
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
            "Command '%s' with --calculator-name %s requires one of --statistics-payload or --file-path.",
            LOCAL_STATS_CALCULATOR_NAME)
        .addForbidWhenOption(
            CliOption.CALCULATOR_NAME.longOpt(),
            value -> !LOCAL_STATS_CALCULATOR_NAME.equals(value),
            List.of(CliOption.STATISTICS_PAYLOAD.longOpt(), CliOption.FILE_PATH.longOpt()),
            "--statistics-payload and --file-path are only supported when --calculator-name is %s.",
            LOCAL_STATS_CALCULATOR_NAME)
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
    out.printf("Required options: %s%n", joinOptions(spec.requiredOptions()));
    out.printf("Optional options: %s%n", joinOptions(spec.optionalOptions()));
    out.printf("Example: %s%n", spec.example());
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

  private static void executeCommand(OptimizerCommandType optimizerType, CommandContext context)
      throws Exception {
    CommandHandler handler = COMMAND_HANDLERS.get(optimizerType);
    Preconditions.checkArgument(handler != null, "Unsupported optimizer type: %s", optimizerType);
    handler.execute(context);
  }

  private static void executeRecommendStrategyType(CommandContext context) throws Exception {
    try (Recommender recommender = new Recommender(context.optimizerEnv())) {
      recommender.recommendForStrategyType(
          parseIdentifiers(context.identifiers()), context.strategyType());
    }
  }

  private static void executeUpdateStatistics(CommandContext context) throws Exception {
    try (Updater updater =
        new Updater(
            withStatisticsInput(context.optimizerEnv(), context.statisticsInputContent()))) {
      if (context.identifiers() == null || context.identifiers().length == 0) {
        updater.updateAll(context.calculatorName(), UpdateType.STATISTICS);
      } else {
        updater.update(
            context.calculatorName(),
            parseIdentifiers(context.identifiers()),
            UpdateType.STATISTICS);
      }
    }
  }

  private static void executeAppendMetrics(CommandContext context) throws Exception {
    try (Updater updater =
        new Updater(
            withStatisticsInput(context.optimizerEnv(), context.statisticsInputContent()))) {
      if (context.identifiers() == null || context.identifiers().length == 0) {
        updater.updateAll(context.calculatorName(), UpdateType.METRICS);
      } else {
        updater.update(
            context.calculatorName(), parseIdentifiers(context.identifiers()), UpdateType.METRICS);
      }
    }
  }

  private static void executeMonitorMetrics(CommandContext context) throws Exception {
    long actionTimeSeconds = parseLongOption("action-time", context.actionTime(), false);
    long rangeSecondsLong = parseLongOption("range-seconds", context.rangeSeconds(), true);
    Optional<PartitionPath> partitionPath = parsePartitionPath(context.partitionPathRaw());
    Preconditions.checkArgument(
        partitionPath.isEmpty() || context.identifiers().length == 1,
        "--partition-path requires exactly one identifier");
    try (Monitor monitor = new Monitor(context.optimizerEnv())) {
      for (NameIdentifier identifier : parseIdentifiers(context.identifiers())) {
        List<EvaluationResult> results =
            monitor.evaluateMetrics(identifier, actionTimeSeconds, rangeSecondsLong, partitionPath);
        results.forEach(result -> printEvaluationResult(context.output(), result));
      }
    }
  }

  private static void executeListTableMetrics(CommandContext context) {
    Optional<PartitionPath> partitionPath = parsePartitionPath(context.partitionPathRaw());
    Preconditions.checkArgument(
        partitionPath.isEmpty() || context.identifiers().length == 1,
        "--partition-path requires exactly one identifier");
    try (MetricsProvider metricsProvider = createMetricsProvider(context.optimizerEnv())) {
      for (NameIdentifier identifier : parseIdentifiers(context.identifiers())) {
        Map<String, List<MetricSample>> metrics =
            partitionPath
                .map(
                    path ->
                        metricsProvider.partitionMetrics(
                            identifier, path, METRICS_LIST_FROM_SECONDS, METRICS_LIST_TO_SECONDS))
                .orElseGet(
                    () ->
                        metricsProvider.tableMetrics(
                            identifier, METRICS_LIST_FROM_SECONDS, METRICS_LIST_TO_SECONDS));
        printMetricsResult(
            context.output(),
            partitionPath.isPresent() ? MetricScope.Type.PARTITION : MetricScope.Type.TABLE,
            identifier,
            partitionPath,
            metrics);
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to list table metrics", e);
    }
  }

  private static void executeListJobMetrics(CommandContext context) {
    try (MetricsProvider metricsProvider = createMetricsProvider(context.optimizerEnv())) {
      for (NameIdentifier identifier : parseIdentifiers(context.identifiers())) {
        Map<String, List<MetricSample>> metrics =
            metricsProvider.jobMetrics(
                identifier, METRICS_LIST_FROM_SECONDS, METRICS_LIST_TO_SECONDS);
        printMetricsResult(
            context.output(), MetricScope.Type.JOB, identifier, Optional.empty(), metrics);
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to list job metrics", e);
    }
  }

  private static MetricsProvider createMetricsProvider(OptimizerEnv optimizerEnv) {
    MetricsProvider provider =
        ProviderUtils.createMetricsProviderInstance(
            optimizerEnv.config().get(OptimizerConfig.METRICS_PROVIDER_CONFIG));
    provider.initialize(optimizerEnv);
    return provider;
  }

  private static final class CommandOptionSpec {
    private final Set<CliOption> requiredOptions;
    private final Set<CliOption> optionalOptions;
    private final String example;
    private final CommandRules.ValidationPlan rules;

    private CommandOptionSpec(
        Set<CliOption> requiredOptions,
        Set<CliOption> optionalOptions,
        String example,
        CommandRules.ValidationPlan rules) {
      this.requiredOptions = Collections.unmodifiableSet(EnumSet.copyOf(requiredOptions));
      this.optionalOptions = Collections.unmodifiableSet(EnumSet.copyOf(optionalOptions));
      this.example = example;
      this.rules = rules;
    }

    private static CommandOptionSpec of(
        Set<CliOption> requiredOptions, Set<CliOption> optionalOptions, String example) {
      return new CommandOptionSpec(
          requiredOptions, optionalOptions, example, CommandRules.emptyPlan());
    }

    private static CommandOptionSpec of(
        Set<CliOption> requiredOptions,
        Set<CliOption> optionalOptions,
        String example,
        CommandRules.ValidationPlan rules) {
      return new CommandOptionSpec(requiredOptions, optionalOptions, example, rules);
    }

    private Set<CliOption> requiredOptions() {
      return requiredOptions;
    }

    private Set<CliOption> optionalOptions() {
      return optionalOptions;
    }

    private String example() {
      return example;
    }

    private CommandRules.ValidationPlan rules() {
      return rules;
    }
  }

  @FunctionalInterface
  private interface CommandHandler {
    void execute(CommandContext context) throws Exception;
  }

  private static final class CommandContext {
    private final OptimizerEnv optimizerEnv;
    private final String[] identifiers;
    private final String strategyType;
    private final String calculatorName;
    private final String actionTime;
    private final String rangeSeconds;
    private final String partitionPathRaw;
    private final Optional<StatisticsInputContent> statisticsInputContent;
    private final PrintStream output;

    private CommandContext(
        OptimizerEnv optimizerEnv,
        String[] identifiers,
        String strategyType,
        String calculatorName,
        String actionTime,
        String rangeSeconds,
        String partitionPathRaw,
        Optional<StatisticsInputContent> statisticsInputContent,
        PrintStream output) {
      this.optimizerEnv = optimizerEnv;
      this.identifiers = identifiers;
      this.strategyType = strategyType;
      this.calculatorName = calculatorName;
      this.actionTime = actionTime;
      this.rangeSeconds = rangeSeconds;
      this.partitionPathRaw = partitionPathRaw;
      this.statisticsInputContent = statisticsInputContent;
      this.output = output;
    }

    private OptimizerEnv optimizerEnv() {
      return optimizerEnv;
    }

    private String[] identifiers() {
      return identifiers;
    }

    private String strategyType() {
      return strategyType;
    }

    private String calculatorName() {
      return calculatorName;
    }

    private String actionTime() {
      return actionTime;
    }

    private String rangeSeconds() {
      return rangeSeconds;
    }

    private String partitionPathRaw() {
      return partitionPathRaw;
    }

    private Optional<StatisticsInputContent> statisticsInputContent() {
      return statisticsInputContent;
    }

    private PrintStream output() {
      return output;
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
    STRATEGY_TYPE("strategy-type", CliOptionArgType.SINGLE, null, "Strategy type"),
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
        "Partition path for monitor-metrics (JSON array, for example: [{\"p1\":\"v1\"},{\"p2\":\"v2\"}])");

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
    RECOMMEND_STRATEGY_TYPE,
    UPDATE_STATISTICS,
    APPEND_METRICS,
    MONITOR_METRICS,
    LIST_TABLE_METRICS,
    LIST_JOB_METRICS;

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
