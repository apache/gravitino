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

# Shared test database tuning

`mysql.args` and `postgresql.args` hold extra server flags that
`SharedDbContainerService` passes to the single shared MySQL / PostgreSQL
container used by the `core` database tests. This file explains what the tuning
does, what it bought, and -- most importantly -- how to re-derive the memory
caps if the CI runner class ever changes.

## What the tuning does and why

The `core` DB tests used to get one container per test fork. They now share one
container per backend, which removes a lot of container startup cost but means
a single server absorbs roughly **twice the concurrent DDL/DML** it saw before.

That matters because a database's default durability guarantees are *per-commit*
costs: an fsync on every commit, doublewrite / full-page writes, binary logging,
and frequent checkpoints. Doubling the write load multiplies those costs rather
than absorbing them, so the shared container started out *slower* than the
per-fork design it replaced.

The `.args` files relax exactly those guarantees (`fsync=off`,
`innodb_flush_log_at_trx_commit=0`, `synchronous_commit=off`,
`full_page_writes=off`, `--skip-log-bin`, `autovacuum=off`, a long
`checkpoint_timeout`, and a larger buffer pool / `shared_buffers`), and the
service mounts each data dir on `tmpfs`. This is safe **here and only here**:
the container is disposable and is destroyed at the end of the run, so crash
recovery, replication and long-term maintenance have no one to serve. None of
these settings would be acceptable in production.

One flag is deliberately *not* set: `--performance-schema=OFF`.
`TestRoleMembershipWrites` and `TestOwnerAssignmentWrites` verify real row-lock
contention by polling `performance_schema.data_lock_waits`. With the
performance schema disabled that table is silently always empty, so those
assertions would stop verifying anything without ever failing.

## Measured effect

Measured on `apache/gravitino#13553` with `-PcoreDatabaseForks=2`:

| Task                      | Before (per-fork) | After (shared + tuned) | Speedup |
| ------------------------- | ----------------- | ---------------------- | ------- |
| `core:coreMySQLTest`      | 26m25s            | 7m04s                  | 3.74x   |
| `core:corePostgreSQLTest` | 7m10s             | 2m11s                  | 3.28x   |
| `core:coreH2Test`         | 1m27s             | 1m21s                  | ~1.0x (control) |

`coreH2Test` uses no container and is flat across every run, which is what
confirms the speedup is database I/O rather than a faster host.

### Attribution: the tmpfs data dir did most of the work

The numbers above were originally collected while a classpath-path bug meant
the `.args` files were never actually loaded (`ClassLoader#getResourceAsStream`
resolving an absolute name that did not match where the resources are
packaged), so both containers silently ran on stock server defaults. They
therefore measure **the tmpfs data dir alone**, not the server flags.

Re-measured after that bug was fixed, with the flags verified as applied in the
running container (`innodb_buffer_pool_size=2147483648`,
`innodb_flush_log_at_trx_commit=0`, `innodb_doublewrite=OFF`, `log_bin=OFF`):

| Task                      | tmpfs only | tmpfs + flags | Flags' marginal gain |
| ------------------------- | ---------- | ------------- | -------------------- |
| `core:coreMySQLTest`      | 7m05s      | 6m40s         | ~6%                  |
| `core:corePostgreSQLTest` | 2m11s      | 2m10s         | ~0% (within noise)   |

So the headline 3-4x speedup comes almost entirely from moving the data dir to
`tmpfs`; the durability flags are a modest additional gain on top. Keep that in
mind before trading test fidelity for another flag -- the remaining headroom is
small. It is also why `--performance-schema=OFF` is not worth having: it would
buy a little overhead back while silently disabling the lock-contention
assertions described above.

### Caveat: those numbers came from bigger hardware than CI

The table above was measured on an **8 vCPU / 31 GB** development box. This
repo's actual lane -- the backend integration-test job in
`.github/workflows/backend-integration-test.yml` -- is `runs-on: ubuntu-22.04`,
a GitHub-hosted runner with **4 vCPU / 16 GiB RAM**. Do not expect to reproduce
3.73x there.

Expect a real but smaller speedup. The win is **architectural**, not tied to a
specific machine: relaxed fsync/checkpoint semantics and a `tmpfs` data dir
remove I/O work that no amount of CPU would have made free. A smaller box has
less parallelism to exploit and less page cache, so the multiplier shrinks --
the direction does not.

## Headroom math: sizing the tmpfs caps

Both `tmpfs` mounts are explicitly capped. This is required, not tidiness: an
uncapped `--tmpfs` defaults to **50% of host RAM per mount**, so two uncapped
mounts can claim 100% of RAM and get the Gradle daemon or a test fork
OOM-killed instead of failing cleanly.

Use this formula to re-derive the caps for a different runner class:

```
safe tmpfs ceiling = total RAM
                   - build-process reserve        (CoreDatabaseConcurrency
                                                   DEFAULT_BUILD_PROCESS_MEMORY_RESERVE_BYTES)
                   - forks x (max heap + native overhead)
                                                  (TEST_WORKER_MAX_HEAP_MIB +
                                                   DEFAULT_TEST_WORKER_OVERHEAD_BYTES)
                   - database process RSS         (innodb_buffer_pool_size /
                                                   shared_buffers, plus overhead)
                   - OS and Docker daemon
```

Worked example for the current `ubuntu-22.04` lane at `-PcoreDatabaseForks=2`:

| Term                                            | Size    |
| ----------------------------------------------- | ------- |
| Total RAM                                       | 16 GiB  |
| Build-process reserve                           | -4 GiB  |
| 2 forks x (4 GiB heap + 2 GiB native overhead)  | -12 GiB |

The JVM side alone nominally commits the entire box, so the mounts must be a
bounded worst case rather than an open-ended 8 GiB each. Real fork heap usage
sits well below the 4 GiB maximum, which is where the actual headroom comes
from -- but the *cap* has to assume it might not.

Current caps:

- **MySQL, `size=2g`.** The core test schema is small, and `--skip-log-bin`
  plus the default (~100 MiB) redo capacity mean little besides table data
  lands on the mount. The 2G buffer pool is separately resident as container
  RSS, not on the tmpfs.
- **PostgreSQL, `size=5g`.** Deliberately larger, because `pg_wal` lives
  *inside* the data dir and `postgresql.args` sets `max_wal_size=4GB` with
  `checkpoint_timeout=1h`. Up to ~4 GiB of WAL can legitimately accumulate
  before a checkpoint recycles it, so a 2g cap would turn a merely-late
  checkpoint into an `ENOSPC` and a PostgreSQL `PANIC` mid-run. 5g = ~4 GiB WAL
  headroom + ~1 GiB schema and data.

Worst case both mounts total **7 GiB**, leaving ~9 GiB for the OS, both
database processes' RSS, and the fork JVMs' real heap usage.

If you raise `max_wal_size` or `checkpoint_timeout`, raise the PostgreSQL cap
to match. If you move to a smaller runner, re-run the formula rather than
assuming these two numbers still fit.
