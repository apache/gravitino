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

package org.apache.gravitino.testing;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.services.BuildService;
import org.gradle.api.services.BuildServiceParameters;

/**
 * A Gradle {@link BuildService} that owns a single, shared MySQL container and a single, shared
 * PostgreSQL container for the whole lifetime of the build, so that the Core database test tasks
 * no longer have to start one container per Gradle test-worker fork.
 *
 * <p>Containers are started lazily, on the first call to {@link #connectionInfo(String)} for a
 * given backend, and are reused for every subsequent call for that backend within the same build.
 * Each forked test JVM is expected to create its own database on the shared container rather than
 * requesting its own container.
 *
 * <p>The main Gradle process does not have Testcontainers on its classpath, so containers are
 * managed directly through the {@code docker} CLI rather than through Testcontainers. Every
 * container this service starts is tagged with the {@value #CONTAINER_LABEL_KEY} label plus a
 * {@value #CONTAINER_PID_LABEL_KEY} label recording the Gradle daemon process that started it, so
 * that a crashed or forcibly killed build does not leak a container forever: the next build that
 * creates this service removes any labelled container that has either exited, or is still
 * running but whose recording daemon process is no longer alive - a still-running container whose
 * daemon *is* alive belongs to a build genuinely still in progress (a second Gradle invocation,
 * another CI job sharing this runner, an IDE test run started mid-build) and is left alone.
 * Databases created on a container are never individually dropped; they are discarded together
 * with the container when it is removed.
 */
public abstract class SharedDbContainerService
    implements BuildService<BuildServiceParameters.None>, AutoCloseable {

  private static final Logger LOG = Logging.getLogger(SharedDbContainerService.class);

  /** Docker label key applied to every container this service starts. */
  private static final String CONTAINER_LABEL_KEY = "gravitino-core-test";

  /**
   * Docker label key recording the PID of the Gradle daemon process that started a container, so
   * a later build can tell "still running because its build is still in progress" apart from
   * "still running because its build (and daemon) died without cleaning up".
   */
  private static final String CONTAINER_PID_LABEL_KEY = "gravitino-core-test-pid";

  private static final String MYSQL_BACKEND = "mysql";
  private static final String POSTGRESQL_BACKEND = "postgresql";

  private static final String MYSQL_IMAGE = "mysql:8.0";
  private static final String POSTGRESQL_IMAGE = "postgres:13";

  private static final String MYSQL_CONTAINER_PORT = "3306";
  private static final String POSTGRESQL_CONTAINER_PORT = "5432";

  private static final String DB_USER = "root";
  private static final String DB_PASSWORD = "root";

  private static final int READINESS_MAX_ATTEMPTS = 120;
  private static final long READINESS_POLL_INTERVAL_MILLIS = 1000L;

  // How long a single docker CLI invocation (including a cold image pull) may run before it is
  // treated as hung and killed.
  private static final long EXEC_TIMEOUT_SECONDS = 300L;

  // Unique per build-service instance, embedded in every container this instance starts (see
  // startMySql/startPostgreSql). Purely informational/diagnostic - see CONTAINER_PID_LABEL_KEY
  // for the label removeStaleContainers() actually uses to decide what is safe to remove.
  private final String labelValue = "run-" + UUID.randomUUID();

  // The current Gradle daemon's PID, recorded on every container this instance starts (see
  // CONTAINER_PID_LABEL_KEY / removeStaleContainers()).
  private final long ownerPid = ProcessHandle.current().pid();

  private final Map<String, DbConnectionInfo> connectionInfoByBackend = new ConcurrentHashMap<>();
  private final Map<String, String> containerIdByBackend = new ConcurrentHashMap<>();

  // Drains a docker CLI child process's stdout/stderr concurrently (see exec()). Daemon threads
  // so a stuck drain never keeps the Gradle daemon process alive.
  private final ExecutorService streamReader =
      Executors.newCachedThreadPool(
          runnable -> {
            Thread thread = new Thread(runnable, "shared-db-container-stream-reader");
            thread.setDaemon(true);
            return thread;
          });

  /** Creates the service and removes any stale containers left by a previously killed build. */
  public SharedDbContainerService() {
    removeStaleContainers();
  }

  /**
   * Returns connection info for the shared container for the given backend, starting that
   * backend's container on first use.
   *
   * @param backend either {@code "mysql"} or {@code "postgresql"}
   * @return administrative connection info for the shared container, not naming any database
   */
  public synchronized DbConnectionInfo connectionInfo(String backend) {
    return connectionInfoByBackend.computeIfAbsent(backend, this::startContainer);
  }

  /** Stops every container this service instance started. */
  @Override
  public void close() {
    containerIdByBackend.values().forEach(this::removeContainer);
    containerIdByBackend.clear();
    streamReader.shutdownNow();
  }

  private void removeContainer(String containerId) {
    try {
      runDocker("rm", "-f", containerId);
    } catch (RuntimeException e) {
      LOG.warn("Failed to stop shared test database container {}: {}", containerId, e.getMessage());
    }
  }

  /**
   * Removes containers left behind by a previous, no-longer-running build.
   *
   * <p>A labelled container with {@code status=exited} is always reaped (its owning build is, by
   * definition, done with it). A labelled container that is still {@code running} is reaped only
   * when its recorded {@value #CONTAINER_PID_LABEL_KEY} process is no longer alive - that is the
   * one realistic leak this class exists to close: a Gradle daemon that was SIGKILLed, OOM-killed,
   * or lost with its host, which never got to call {@link #close()}. A still-running container
   * whose daemon process *is* alive is never touched, since it belongs to a build genuinely still
   * in progress on this host (a second Gradle invocation, another CI job sharing this runner, an
   * IDE test run started mid-build).
   */
  private void removeStaleContainers() {
    String labelledIds =
        runDockerQuiet("ps", "-aq", "--filter", "label=" + CONTAINER_LABEL_KEY);
    List<String> ids = splitNonBlankLines(labelledIds);
    List<String> toRemove = new ArrayList<>();
    for (String id : ids) {
      if (isReapable(id)) {
        toRemove.add(id);
      }
    }
    if (toRemove.isEmpty()) {
      return;
    }

    LOG.lifecycle(
        "Removing {} stale shared test database container(s) from a previous build", toRemove.size());
    List<String> removeCommand = new ArrayList<>();
    removeCommand.add("rm");
    removeCommand.add("-f");
    removeCommand.addAll(toRemove);
    runDockerQuiet(removeCommand.toArray(new String[0]));
  }

  /**
   * Returns whether the given labelled container may be safely removed: it has already exited,
   * or its recorded owning daemon process is no longer alive.
   */
  private boolean isReapable(String containerId) {
    String state =
        runDockerQuiet("inspect", "-f", "{{.State.Status}}", containerId).trim();
    if ("exited".equals(state)) {
      return true;
    }

    String pidLabel =
        runDockerQuiet(
                "inspect",
                "-f",
                "{{index .Config.Labels \"" + CONTAINER_PID_LABEL_KEY + "\"}}",
                containerId)
            .trim();
    return !isOwningProcessAlive(pidLabel);
  }

  /** Returns whether {@code pidLabel} parses to a PID that is currently alive on this host. */
  static boolean isOwningProcessAlive(String pidLabel) {
    try {
      long pid = Long.parseLong(pidLabel.trim());
      return ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false);
    } catch (NumberFormatException e) {
      // No usable PID label (e.g. a container from before this label existed, or empty output) -
      // treat it as not verifiably alive, so removeStaleContainers() falls through to reaping it
      // rather than leaking it forever on a false-negative liveness check.
      return false;
    }
  }


  /**
   * Why this tuning exists at all: one shared container serves roughly twice the concurrent
   * DDL/DML load that per-fork containers each saw, because every test fork now writes to the
   * same server. A database's production durability defaults -- an fsync on every commit, frequent
   * checkpoints, doublewrite/full-page writes, binary logging -- are per-commit costs, so that
   * doubled write load multiplies them rather than absorbing them. Relaxing those guarantees for
   * a container that is disposable and torn down at the end of the run was the difference between
   * the shared container being a ~5.6% regression and being a 3x+ speedup.
   *
   * <p>Loads extra container-start args for {@code backend} from a plain-text resource at
   * {@code shared-db-tuning/<backend>.args} (one token per line; blank lines and lines
   * starting with {@code #} are ignored). Missing resources or read failures are logged and
   * treated as "no extra args" so a bad/missing tuning file can never fail the build --
   * it only ever falls back to untuned defaults.
   */
  private List<String> loadExtraDbArgs(String backend) {
    String resourcePath = "shared-db-tuning/" + backend + ".args";
    try (java.io.InputStream in =
        SharedDbContainerService.class.getClassLoader().getResourceAsStream(resourcePath)) {
      if (in == null) {
        LOG.warn(
            "No DB tuning args found on the classpath at {} for backend {}; the shared container"
                + " will run with untuned server defaults",
            resourcePath,
            backend);
        return java.util.Collections.emptyList();
      }
      List<String> args = new ArrayList<>();
      try (java.io.BufferedReader reader =
          new java.io.BufferedReader(new java.io.InputStreamReader(in, StandardCharsets.UTF_8))) {
        String line;
        while ((line = reader.readLine()) != null) {
          String trimmed = line.trim();
          if (!trimmed.isEmpty() && !trimmed.startsWith("#")) {
            args.add(trimmed);
          }
        }
      }
      return args;
    } catch (IOException e) {
      LOG.warn("Failed to load DB tuning args for {} from {}, continuing untuned",
          backend, resourcePath, e);
      return java.util.Collections.emptyList();
    }
  }

  /**
   * Logs the tuning args actually applied to a shared container, so a run that is unexpectedly
   * slow can be told apart from a run whose tuning file silently failed to load.
   */
  private void logTuningArgs(String backendLabel, List<String> tuningArgs) {
    if (tuningArgs.isEmpty()) {
      LOG.warn(
          "Starting shared {} test database container with NO tuning args (untuned server"
              + " defaults); expect a substantially slower run",
          backendLabel);
    } else {
      LOG.lifecycle(
          "Applying {} tuning args to shared {} test database container: {}",
          tuningArgs.size(),
          backendLabel,
          String.join(" ", tuningArgs));
    }
  }

  private DbConnectionInfo startContainer(String backend) {
    switch (backend) {
      case MYSQL_BACKEND:
        return startMySql();
      case POSTGRESQL_BACKEND:
        return startPostgreSql();
      default:
        throw new IllegalArgumentException("Unsupported shared test database backend: " + backend);
    }
  }

  private DbConnectionInfo startMySql() {
    List<String> mysqlRunArgs =
        new ArrayList<>(
            Arrays.asList(
                "run",
                "-d",
                "--rm",
                "--label",
                CONTAINER_LABEL_KEY + "=" + labelValue,
                "--label",
                CONTAINER_PID_LABEL_KEY + "=" + ownerPid,
                "-P",
                // Disposable container -- backing the data dir with tmpfs removes real disk
                // I/O for every write, on top of the durability-relaxing mysqld flags below.
                //
                // The size cap is mandatory, not cosmetic: an uncapped `--tmpfs` mount defaults
                // to 50% of host RAM *per mount*, so MySQL's and PostgreSQL's data dirs could
                // between them claim 100% of RAM and get the Gradle daemon or a test fork
                // OOM-killed. Headroom math for this repo's real lane -- the backend
                // integration-test job in .github/workflows/backend-integration-test.yml runs
                // on `ubuntu-22.04`, a GitHub-hosted runner with 4 vCPU / 16 GiB RAM:
                //     16 GiB  total
                //   -  4 GiB  build-process reserve (CoreDatabaseConcurrency
                //             DEFAULT_BUILD_PROCESS_MEMORY_RESERVE_BYTES)
                //   - 12 GiB  2 concurrent test forks x (4 GiB max heap,
                //             TEST_WORKER_MAX_HEAP_MIB + 2 GiB native overhead,
                //             DEFAULT_TEST_WORKER_OVERHEAD_BYTES)
                // i.e. the JVM side alone nominally commits the whole box, so these mounts must
                // stay a bounded worst case rather than an open-ended 8 GiB each. MySQL's share
                // is 2g: the core test schema is small, and --skip-log-bin plus the default
                // ~100 MiB redo capacity mean little but table data lands here, while
                // --innodb-buffer-pool-size=2G is separately resident in the container's RSS.
                "--tmpfs",
                "/var/lib/mysql:size=2g",
                "-e",
                "MYSQL_ROOT_PASSWORD=" + DB_PASSWORD,
                MYSQL_IMAGE));
    List<String> mysqlTuningArgs = loadExtraDbArgs("mysql");
    logTuningArgs("MySQL", mysqlTuningArgs);
    mysqlRunArgs.addAll(mysqlTuningArgs);
    String containerId = runDocker(mysqlRunArgs.toArray(new String[0]));
    try {
      awaitReady(
          containerId,
          new String[] {
            "mysqladmin", "ping", "-h", "127.0.0.1", "-u" + DB_USER, "-p" + DB_PASSWORD
          },
          "MySQL");
      int hostPort = resolveHostPort(containerId, MYSQL_CONTAINER_PORT);
      containerIdByBackend.put(MYSQL_BACKEND, containerId);

      String adminJdbcUrl = String.format("jdbc:mysql://127.0.0.1:%d", hostPort);
      LOG.lifecycle(
          "Started shared MySQL test database container {} on port {}", containerId, hostPort);
      return new DbConnectionInfo(adminJdbcUrl, DB_USER, DB_PASSWORD);
    } catch (RuntimeException e) {
      // Not yet recorded in containerIdByBackend (only done on success above), so close() would
      // never reap this container - remove it now instead of leaking it until the next build's
      // stale-container cleanup.
      removeContainer(containerId);
      throw e;
    }
  }

  private DbConnectionInfo startPostgreSql() {
    List<String> postgresRunArgs =
        new ArrayList<>(
            Arrays.asList(
                "run",
                "-d",
                "--rm",
                "--label",
                CONTAINER_LABEL_KEY + "=" + labelValue,
                "--label",
                CONTAINER_PID_LABEL_KEY + "=" + ownerPid,
                "-P",
                // Disposable container -- see the equivalent tmpfs mount in startMySql() for
                // the runner spec (ubuntu-22.04, 4 vCPU / 16 GiB) and the headroom math.
                //
                // PostgreSQL gets a larger cap than MySQL (5g vs 2g) because pg_wal lives
                // *inside* the data dir and postgresql.args sets max_wal_size=4GB with
                // checkpoint_timeout=1h, so up to ~4 GiB of WAL can legitimately accumulate
                // here before a checkpoint recycles it. Capping this at MySQL's 2g would turn a
                // merely-late checkpoint into an ENOSPC and a PostgreSQL PANIC partway through
                // the run. 5g = ~4 GiB WAL headroom + ~1 GiB for the schema and its data.
                // Worst case both mounts together top out at 7 GiB, leaving ~9 GiB for the OS,
                // the two database processes' RSS (2 GiB buffer pool + 1 GiB shared_buffers,
                // plus overhead) and the fork JVMs' actual (as opposed to maximum) heap usage.
                "--tmpfs",
                "/var/lib/postgresql/data:size=5g",
                "-e",
                "POSTGRES_USER=" + DB_USER,
                "-e",
                "POSTGRES_PASSWORD=" + DB_PASSWORD,
                POSTGRESQL_IMAGE));
    List<String> postgresTuningArgs = loadExtraDbArgs("postgresql");
    logTuningArgs("PostgreSQL", postgresTuningArgs);
    postgresRunArgs.addAll(postgresTuningArgs);
    String containerId = runDocker(postgresRunArgs.toArray(new String[0]));
    try {
      // -h forces a TCP health check. The postgres image's entrypoint first runs a *temporary*,
      // socket-only server (listen_addresses='') to execute init scripts before starting the
      // real, TCP-listening server; a socket-based pg_isready (no -h) reports ready during that
      // window, before this container can actually accept the JDBC connection made right after.
      awaitReady(containerId, new String[] {"pg_isready", "-h", "127.0.0.1", "-U", DB_USER}, "PostgreSQL");
      int hostPort = resolveHostPort(containerId, POSTGRESQL_CONTAINER_PORT);
      containerIdByBackend.put(POSTGRESQL_BACKEND, containerId);

      // Mirrors PostgreSQLContainer#getJdbcUrl(): no database name, trailing slash.
      String adminJdbcUrl = String.format("jdbc:postgresql://127.0.0.1:%d/", hostPort);
      LOG.lifecycle(
          "Started shared PostgreSQL test database container {} on port {}",
          containerId,
          hostPort);
      return new DbConnectionInfo(adminJdbcUrl, DB_USER, DB_PASSWORD);
    } catch (RuntimeException e) {
      removeContainer(containerId);
      throw e;
    }
  }

  private int resolveHostPort(String containerId, String containerPort) {
    String output = runDocker("port", containerId, containerPort + "/tcp");
    return parseHostPort(output, containerId);
  }

  /**
   * Parses the host port `docker port &lt;container&gt; &lt;port&gt;/tcp` publishes from its raw
   * output, e.g. {@code "0.0.0.0:49153"} or, when the daemon also publishes an IPv6 address,
   * {@code "0.0.0.0:49153\n[::]:49153"} (the first line is used either way).
   *
   * @throws IllegalStateException if {@code output} has no lines (no published port)
   * @throws NumberFormatException if the port segment after the last {@code ':'} is not numeric
   */
  static int parseHostPort(String output, String containerId) {
    List<String> lines = splitNonBlankLines(output);
    if (lines.isEmpty()) {
      throw new IllegalStateException(
          "docker port did not report a published host port for container " + containerId);
    }
    String firstLine = lines.get(0);
    String portPart = firstLine.substring(firstLine.lastIndexOf(':') + 1).trim();
    return Integer.parseInt(portPart);
  }

  private void awaitReady(String containerId, String[] healthCheckCommand, String backendLabel) {
    List<String> execCommand = new ArrayList<>();
    execCommand.add("exec");
    execCommand.add(containerId);
    execCommand.addAll(Arrays.asList(healthCheckCommand));

    for (int attempt = 1; attempt <= READINESS_MAX_ATTEMPTS; attempt++) {
      if (runDockerExitCode(execCommand) == 0) {
        return;
      }
      sleep(READINESS_POLL_INTERVAL_MILLIS);
    }

    throw new IllegalStateException(
        String.format(
            "Timed out after %d attempts waiting for shared %s test database container %s to"
                + " become ready",
            READINESS_MAX_ATTEMPTS, backendLabel, containerId));
  }

  private void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while waiting for shared test database container", e);
    }
  }

  /** Runs {@code docker <args>}, throwing if it exits non-zero, and returns its stdout. */
  private String runDocker(String... args) {
    ProcessResult result = exec(args);
    if (result.exitCode != 0) {
      throw new IllegalStateException(
          "docker " + String.join(" ", args) + " failed with exit code " + result.exitCode
              + ": " + result.stderr);
    }
    return result.stdout;
  }

  /** Runs {@code docker <args>}, logging (but not throwing on) a non-zero exit, and returns stdout. */
  private String runDockerQuiet(String... args) {
    ProcessResult result = exec(args);
    if (result.exitCode != 0) {
      LOG.warn("docker {} exited with {}: {}", String.join(" ", args), result.exitCode, result.stderr);
    }
    return result.stdout;
  }

  /** Runs {@code docker <args>} and returns only its exit code. */
  private int runDockerExitCode(List<String> args) {
    return exec(args.toArray(new String[0])).exitCode;
  }

  private ProcessResult exec(String... args) {
    List<String> command = new ArrayList<>();
    command.add("docker");
    command.addAll(Arrays.asList(args));

    Process process = null;
    try {
      // Deliberately does NOT merge stderr into stdout: `docker run` writes image-pull progress
      // to stderr while it is still starting the container, and prints only the container ID to
      // stdout on success. Merging the two streams (as redirectErrorStream(true) would) corrupts
      // the container ID with pull-progress text on a cold cache, breaking every later `docker
      // exec`/`docker port` call keyed on that ID.
      //
      // stdout and stderr are drained on separate threads, concurrently with the process running:
      // a slow image pull can write more to stderr than one pipe buffer holds, and letting stdout
      // fill up while stderr is not yet being drained would deadlock against the child process
      // blocking on a full stderr pipe.
      process = new ProcessBuilder(command).start();
      process.getOutputStream().close(); // no docker subcommand used here reads stdin
      Process startedProcess = process;
      Future<byte[]> stdoutFuture =
          streamReader.submit(() -> startedProcess.getInputStream().readAllBytes());
      Future<byte[]> stderrFuture =
          streamReader.submit(() -> startedProcess.getErrorStream().readAllBytes());

      // waitFor(timeout) is checked BEFORE reading the futures: readAllBytes() only returns at
      // EOF (i.e. once the process has actually exited), so blocking on the futures first would
      // make this timeout unreachable - a hung `docker` call would block the build forever.
      boolean finished = process.waitFor(EXEC_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      if (!finished) {
        process.destroyForcibly();
        stdoutFuture.cancel(true);
        stderrFuture.cancel(true);
        throw new IllegalStateException("Command timed out: " + String.join(" ", command));
      }

      String stdout =
          new String(stdoutFuture.get(EXEC_TIMEOUT_SECONDS, TimeUnit.SECONDS), StandardCharsets.UTF_8);
      String stderr =
          new String(stderrFuture.get(EXEC_TIMEOUT_SECONDS, TimeUnit.SECONDS), StandardCharsets.UTF_8);
      return new ProcessResult(process.exitValue(), stdout.trim(), stderr.trim());
    } catch (IOException e) {
      throw new IllegalStateException("Failed to run command: " + String.join(" ", command), e);
    } catch (ExecutionException e) {
      throw new IllegalStateException(
          "Failed to read output of command: " + String.join(" ", command), e.getCause());
    } catch (TimeoutException e) {
      throw new IllegalStateException(
          "Timed out reading output of command: " + String.join(" ", command), e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while running command: " + String.join(" ", command), e);
    } finally {
      if (process != null && process.isAlive()) {
        process.destroyForcibly();
      }
    }
  }

  static List<String> splitNonBlankLines(String text) {
    List<String> lines = new ArrayList<>();
    for (String line : text.split("\\R")) {
      if (!line.trim().isEmpty()) {
        lines.add(line.trim());
      }
    }
    return lines;
  }

  private static final class ProcessResult {
    private final int exitCode;
    private final String stdout;
    private final String stderr;

    private ProcessResult(int exitCode, String stdout, String stderr) {
      this.exitCode = exitCode;
      this.stdout = stdout;
      this.stderr = stderr;
    }
  }
}
