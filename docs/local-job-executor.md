---
title: "Local Job Executor"
slug: "/local-job-executor"
keyword: "job executor, local job executor, job system, shell job, spark job, Gravitino"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

The `local` job executor runs every job as a process on the Gravitino server. It is the default job
executor and is intended for testing and development. To run jobs on a cluster, use the
[Kubernetes job executor](./k8s-job-executor.md) or a
[custom job executor](./development/custom-job-executor.md).

For job templates and the job API, see [Manage Jobs](./manage-jobs-in-gravitino.md).

## Requirements

- A shell template whose `executable` is a command name, such as `python`, needs that command on
  the `PATH` of the Gravitino server process.
- A Spark template needs either `gravitino.jobExecutor.local.sparkHome` or `SPARK_HOME` set before
  the server starts, pointing to a Spark installation with an executable `bin/spark-submit`.
  Otherwise, the run request is rejected with an error that names the missing setting, and no job
  is created.

## Job Execution

The local job executor fetches the resources of a job, that is its executable, scripts, jars, files,
and archives, into the staging directory of the job, and runs the job there as a process on the
Gravitino server host. The resources must be reachable by the Gravitino server, which accepts local
paths and HTTP, HTTPS, FTP, and FTPS URLs.

The executor always uses `gravitino.job.stagingDir` as its staging directory, the same one the job
system stages jobs in. A `gravitino.jobExecutor.local.stagingDir` setting is ignored.

## Configuration

Set the following configurations in the `gravitino.conf` file:

| Property name                                          | Description                                                                                                                                                                                  | Default value                          | Required |
|--------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------|----------|
| `gravitino.jobExecutor.local.waitingQueueSize`         | The size of the waiting queue for queued jobs in the local job executor                                                                                                                      | `100`                                  | No       |
| `gravitino.jobExecutor.local.maxRunningJobs`           | The maximum number of running jobs in the local job executor                                                                                                                                 | `max(1, min(available cores / 2, 10))` | No       |
| `gravitino.jobExecutor.local.jobStatusKeepTimeInMs`    | The time in milliseconds to keep the job status in the local job executor                                                                                                                    | `3600000` (1 hour)                     | No       |
| `gravitino.jobExecutor.local.cancelForceKillDelayInMs` | How long in milliseconds a cancelled job's process may keep running after it is asked to stop before the executor kills it forcibly. Raise it for jobs that need longer to shut down cleanly | `30000` (30 seconds)                   | No       |
| `gravitino.jobExecutor.local.sparkHome`                | The home directory of Spark, Gravitino checks this configuration firstly and then `SPARK_HOME` env. Either of them should be set to run Spark job                                            | `None`                                 | No       |

## Queueing

The local job executor runs up to `gravitino.jobExecutor.local.maxRunningJobs` jobs at the same
time, each in its own process on the Gravitino server host, and queues the others. Make sure the
host has enough resources for that many jobs, or lower this value, especially when running Spark
jobs.

## Environment Variables

The `environments` of a job template are set as the environment variables of the job's process.

## Job Output and Times

The local job executor keeps a job's output in the job's staging directory, so it's available until
the staging directory is cleaned up (`gravitino.job.stagingDirKeepTimeInMs` after the job finishes),
also across server restarts.

:::caution
When multiple Gravitino servers share the same metadata store, `gravitino.job.stagingDir` must be on
storage shared by all servers (for example an NFS mount) for the local job executor to return a
job's output from any server. The servers may mount it at different paths. Otherwise, only the
server that ran a job can return its output, and the other servers return empty output rather than
an error.
:::

The local job executor finds a job's output through a small index file it writes to
`<gravitino.job.stagingDir>/.job-output-index` when the job is submitted:

- Jobs submitted before Gravitino 2.0.0, or during a rolling upgrade by a server that isn't upgraded
  yet, have no index file and return empty output.
- Deleting this directory makes the output of existing jobs unavailable. After downgrading to an
  earlier version, it isn't used anymore and can be removed.
- A missing index returns empty output, but an index that exists and can't be read, for example
  while the shared storage is unavailable, fails the request with an error rather than returning
  empty output that looks like the job printed nothing.

The local job executor reports when each job actually started and finished, even for a job that
starts and finishes between two status pulls. It only keeps a job's state in memory. When that state
is lost before Gravitino records the finished job, the actual times are lost too, and the job's
`finishedAt` is the time Gravitino marks it as finished instead:

- If Gravitino can't pull the job within `gravitino.jobExecutor.local.jobStatusKeepTimeInMs` after it
  finished, the job is marked as `FAILED`, or `CANCELLED` if it was being cancelled, on the next pull.
- If the server running the job exits, the job is expired as described in
  [Multiple Servers and Restarts](#multiple-servers-and-restarts).

## Multiple Servers and Restarts

When multiple Gravitino servers share the same metadata store, each server's local job executor
only tracks the jobs it runs itself:

- A job can only be run and tracked by the server that received the run request. Other servers
  skip it when pulling job statuses.
- Cancelling a job on a server that doesn't run it marks the job as `CANCELLING`, and the server
  running the job cancels it the next time it pulls job statuses. This can take up to
  `gravitino.job.statusPullIntervalInMs`.
- A job's output can be read from any server only if `gravitino.job.stagingDir` is shared by all
  servers, see [Job Output and Times](#job-output-and-times).
- If a server exits while running jobs, nobody can track these jobs anymore. When such a job has
  not been updated for `gravitino.job.stagingDirKeepTimeInMs`, it is marked as `FAILED`, or as
  `CANCELLED` if it was being cancelled. Like other finished jobs, it is then kept for another
  `gravitino.job.stagingDirKeepTimeInMs` before being cleaned up together with its staging
  directory.

:::caution
The local job executor can't tell a job left behind by an exited server from a job that is still
running without changing its status. A job of the local job executor that is still queued, started
or cancelling after `gravitino.job.stagingDirKeepTimeInMs` is marked as `FAILED` (or `CANCELLED`),
even if the job is still running, and keeps this status even if it later finishes. Set this time
longer than any job can run, or stay queued, without changing its status.
:::

:::caution
The local job executor gets a new identity every time the Gravitino server starts, so a restarted
server doesn't recognize the jobs it ran before the restart. This also applies to a single-server
deployment. The processes of these jobs are usually gone with the previous server process, but the
jobs are only marked as `FAILED` once they expire as described above, which can take up to about
1.1 times `gravitino.job.stagingDirKeepTimeInMs` (about 7.7 days by default), as the cleanup runs
every tenth of that time. Until then, they are still reported as queued, started or cancelling.
Cancelling such a job only marks it as `CANCELLING`, which also restarts the expiration.
:::
