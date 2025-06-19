<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# JCStress-Based Concurrency Tests for Gravitino

## Overview

This folder contains concurrency tests for Gravitino, leveraging the JCStress framework to uncover subtle thread-safety issues. These tests are designed to expose concurrency problems that traditional unit tests often miss—such as data races, visibility violations, and atomicity errors.

## What is JCStress?

[JCStress](https://github.com/openjdk/jcstress) is a concurrency stress-testing tool developed by Oracle to verify the correctness of multithreaded Java code. Unlike unit tests, JCStress:

- Executes test scenarios across multiple threads.
- Repeats executions with varying interleavings.
- Reveals low-probability bugs due to concurrency.
- Reports outcomes and their frequencies, categorized as acceptable, forbidden, or interesting.

## Running the Tests

In Gravitino, we use the `jcstress-gradle-plugin` to integrate JCStress testing into the Gradle build system.

This plugin simplifies running JCStress tests by:

- Automatically generating the test harness.
- Providing built-in Gradle tasks (e.g., `:core:jcstress`).
- Generating detailed HTML reports under `build/reports/jcstress/`.

To run the jcstress tests in Gravitino, use the following Gradle command:

```bash
./gradlew :core:jcstress 
```

or a subset of your tests:

```bash
gradle jcstress --tests "MyFirstTest|MySecondTest"
```

After execution, reports are available at:
`core/build/reports/jcstress/index.html`

> The test suite may take several minutes depending on test complexity and available CPU resources.

## Understanding Test Results

JCStress test results are presented in a table format showing:

- Observed outcomes and their frequencies
- Whether each outcome is expected or forbidden
- Total number of iterations
- Time taken for the test

A "FAILED" result means that a forbidden outcome was observed, indicating a potential concurrency issue.

## Configuration

If you need to customize the configuration, add a block like the following to configure the plugin:

```plantuml
jcstress {
    verbose = "true"
    timeMillis = "200"
    spinStyle = "THREAD_YIELD"
}
```

These are all possible configuration options:

| Name             | Description                                                                                                                                                                                                              |
|------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `affinityMode`   | Use the specific affinity mode, if available. `NONE` = No affinity whatsoever; `GLOBAL` = Affnity for the entire JVM; `LOCAL` = Affinity for the individual actors.                                                      |
| `cpuCount`       | Number of CPUs to use. Defaults to all CPUs in the system. Reducing the number of CPUs limits the amount of resources (including memory) the run is using.                                                               |
| `heapPerFork`    | Java heap size per fork, in megabytes. This affects the stride size: maximum footprint will never be exceeded, regardless of min/max stride sizes.                                                                       |
| `forkMultiplier` | "Fork multiplier for randomized/stress tests. This allows more efficient randomized testing, as each fork would use a different seed."                                                                                   |
| `forks`          | Should fork each test N times. Must be 1 or higher.                                                                                                                                                                      |
| `iterations`     | Iterations per test.                                                                                                                                                                                                     |
| `jvmArgs`        | Use given JVM arguments. This disables JVM flags auto-detection, and runs only the single JVM mode. Either a single space-separated option line, or multiple options are accepted. This option only affects forked runs. |
| `jvmArgsPrepend` | Prepend given JVM arguments to auto-detected configurations. This option only affects forked runs."                                                                                                                      |
| `mode`           | Test mode preset: `sanity`, `quick`, `default`, `tough`, `stress`.                                                                                                                                                       |
| `regexp`         | Regexp selector for tests.                                                                                                                                                                                               |
| `reportDir`      | Target destination to put the report into.                                                                                                                                                                               |
| `spinStyle`      | Busy loop wait style. `HARD` = hard busy loop; `THREAD_YIELD` = use `Thread.yield()`; `THREAD_SPIN_WAIT` = use `Thread.onSpinWait()`; `LOCKSUPPORT_PARK_NANOS` = use `LockSupport.parkNanos()`.                          |
| `splitPerActor`  | Use split per-actor compilation mode, if available.                                                                                                                                                                      |
| `strideCount`    | Internal stride count per epoch. Larger value increases cache footprint.                                                                                                                                                 |
| `strideSize`     | Internal stride size. Larger value decreases the synchronization overhead, but also reduces the number of collisions.                                                                                                    |
| `timeMillis`     | Time to spend in single test iteration. Larger value improves test reliability, since schedulers do better job in the long run.                                                                                          |
| `verbose`        | Be extra verbose.                                                                                                                                                                                                        |

## Writing JCStress Tests

JCStress tests can be written in two main styles:

### Style 1: Arbiter-Based Tests

This style uses a separate method to observe the final state:

1. Define shared state variables
2. Create methods annotated with `@Actor` that will run concurrently
3. Define an `@Arbiter` method to check the final state
4. Use `@JCStressTest` and `@Outcome` annotations to specify expected results

Example:
```java
@JCStressTest
@Outcome(id = "1", expect = Expect.ACCEPTABLE, desc = "Normal outcome")
@Outcome(id = "0", expect = Expect.FORBIDDEN, desc = "Race condition")
@State
public class ArbiterTest {
    int x;

    @Actor
    void actor1() {
        x = 1;
    }

    @Actor
    void actor2() {
        x = 1;
    }

    @Arbiter
    void arbiter(I_Result r) {
        r.r1 = x;
    }
}
```

### Style 2: Direct Result Reporting

Actors can directly report results by accepting a result parameter:

1. Define shared state variables
2. Create methods annotated with `@Actor` that accept a result parameter
3. Each actor records its observations directly
4. Use `@JCStressTest` and `@Outcome` annotations to specify expected results

Example:
```java
@JCStressTest
@Outcome(id = "0, 1", expect = Expect.ACCEPTABLE, desc = "Thread 2 sees the write")
@Outcome(id = "0, 0", expect = Expect.ACCEPTABLE, desc = "Thread 2 doesn't see the write")
@State
public class DirectReportingTest {
    int x;

    @Actor
    void actor1() {
        x = 1;
    }

    @Actor
    void actor2(II_Result r) {
        r.r1 = 0;
        r.r2 = x; // Records what this thread observes
    }
}
```

You may add a `@Description` annotation to briefly explain the test scenario. However, avoid writing overly long descriptions, as this can trigger a `StringIndexOutOfBoundsException` due to a known limitation in the JCStress result frame parser.

Choose the style based on your test needs:
- Use an arbiter when the final state needs to be observed after all actors have finished.
- Use direct reporting when actors need to record observations during their execution.
- Direct reporting is particularly useful for testing visibility and ordering guarantees.

A complete example is shown below. It demonstrates how to test the thread-safety of concurrent `put()` operations on distinct keys in a cache implementation.

First, I use two @Actor methods to perform `put()` operations on two different entities (e.g., a schema and a table) concurrently. Then, I use an `@Arbiter` method to verify how many of these entities are visible in the cache after both operations complete. The test defines multiple `@Outcome` values to capture possible results:

- If both entries are present, the result is 2, which is the expected and acceptable outcome.
- If only one is present, the result is 1, indicating a potential visibility or atomicity issue.
- If neither is present, the result is 0, which reveals a serious concurrency failure.

```java
public class xxx {
  // some entity to test.
  
  @JCStressTest
  @Outcome.Outcomes({
      @Outcome(id = "2", expect = Expect.ACCEPTABLE, desc = "Both put() calls succeeded; both entries are visible in the cache."),
      @Outcome(id = "1", expect = Expect.FORBIDDEN, desc = "Only one entry is visible; potential visibility or atomicity issue."),
      @Outcome(id = "0", expect = Expect.FORBIDDEN, desc = "Neither entry is visible; indicates a serious failure in write propagation or cache logic.")})
  @Description("Concurrent put() on different keys. Both schema and table should be visible (result = 2). " + "Lower results may indicate visibility or concurrency issues.")
  @State

  public static class ConcurrentPutDifferentKeysTest {
    private final EntityCache cache;

    public ConcurrentPutDifferentKeysTest() {
      this.cache = new CaffeineEntityCache(new Config() {
      });
    }

    @Actor
    public void actor1() {
      cache.put(schemaEntity);
    }

    @Actor
    public void actor2() {
      cache.put(tableEntity);
    }

    @Arbiter
    public void arbiter(I_Result r) {
      int count = 0;
      if (cache.contains(schemaEntity.nameIdentifier(), schemaEntity.type())) count++;
      if (cache.contains(tableEntity.nameIdentifier(), tableEntity.type())) count++;

      r.r1 = count;
    }
  }
  
  // ... other test classes
}
```

This test helps ensure that the cache implementation handles concurrent writes correctly and does not lose visibility or violate consistency guarantees.

## JCStress Test Lifecycle and Execution Modes

JCStress follows a well-defined execution model to simulate concurrent interactions:

1. **State Initialization**: A fresh instance of the test class (annotated with `@State`) is created before each test iteration to ensure full isolation.
2. **Concurrent Execution of Actors**: All methods annotated with `@Actor` are invoked concurrently, each on a separate thread. These methods simulate the real-world concurrent behavior being tested.
3. **Synchronization Barrier**: After both (or all) actors complete, JCStress inserts a memory fence to ensure all actions are visible to the next phase.
4. **Arbiter Execution (Optional)**: If an `@Arbiter` method is defined, it runs after all actor methods have completed. It observes the final state and writes results to the `*Result` object.
5. **Result Collection and Classification**: The outcome of each iteration is recorded and matched against the `@Outcome` definitions. JCStress performs this process millions of times with different thread interleavings to uncover rare concurrency issues.

JCStress offers several execution modes to control the intensity and duration of the tests. Available modes:

| Mode      | Description                                                                                                       |
|-----------|-------------------------------------------------------------------------------------------------------------------|
| `sanity`  | Runs for a few seconds with minimal iterations. Useful to check test setup.                                       |
| `quick`   | Runs for a short duration with a moderate number of iterations. Good for fast feedback during development.        |
| `default` | Balanced mode with sufficient iterations. Takes a few minutes. Recommended for regular use.                       |
| `tough`   | Longest and most thorough mode. Runs many iterations over tens of minutes. Best for CI and production validation. |

## Concurrency Issues We Target

Our JCStress tests help identify several types of concurrency problems:

1. **Race Conditions**: When multiple threads access shared data without proper synchronization
2. **Memory Visibility**: When changes made by one thread are not visible to other threads
3. **Atomicity Violations**: When operations that should be atomic can be interrupted
4. **Ordering Issues**: When operations execute in unexpected orders due to compiler or CPU reordering

## Best Practices

When working with JCStress tests:

- Keep tests focused on a single concurrency aspect
- Use meaningful names that describe the scenario being tested
- Document expected outcomes and their reasoning
- Run tests multiple times, as issues may not appear in every run
- Review test results carefully, especially for tests with multiple possible valid outcomes

## Resources

- [JCStress Official Documentation](https://wiki.openjdk.java.net/display/CodeTools/jcstress)
- [Java Memory Model Specification](https://docs.oracle.com/javase/specs/jls/se8/html/jls-17.html#jls-17.4)
- [Doug Lea's JSR-133 Cookbook](http://gee.cs.oswego.edu/dl/jmm/cookbook.html)
- [jcstress-gradle-plugin](https://github.com/reyerizo/jcstress-gradle-plugin)