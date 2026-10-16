---
title: "Custom Job Executor"
slug: "/development/custom-job-executor"
keyword: "job executor, job system, extension, JobExecutor, Gravitino"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

The `local` job executor that ships with Gravitino runs jobs as a process on the Gravitino server
and is intended for testing. Running jobs anywhere else, such as against a distributed scheduler,
means implementing your own job executor.

## Implement a Custom Job Executor

Gravitino's job system is extensible: you can implement your own job executor
to run jobs in a distributed environment. Refer to the interface `JobExecutor` in the
code [here](https://github.com/apache/gravitino/blob/main/core/src/main/java/org/apache/gravitino/connector/job/JobExecutor.java).

### Submit Jobs and Handle Job Resources

Gravitino submits a job by calling `submitJob(JobContext, JobTemplate)`, which a job executor
implements:

- The `JobTemplate` is the runtime job template. Its placeholders are already replaced with the
  job configuration, but its resources, that is its executable, scripts, jars, files and archives,
  are still the URIs written in the template. A shell executable can also be a command name with no
  path, such as `python`: it is not a file to fetch, but a command to look up in the environment the
  job runs in. `JobResourceUtils.isCommandName` tells whether it is one; the `local` job executor
  runs it through the `PATH` of the Gravitino server process.
- The `JobContext` carries the Gravitino job id, the metalake of the job, and a staging directory on
  the Gravitino server dedicated to the job. Gravitino creates the staging directory before
  submitting the job, removes it right away if the job fails to be submitted, and otherwise cleans
  it up `gravitino.job.stagingDirKeepTimeInMs` after the job finishes.

The job executor decides how to handle the resources:

- A job executor that runs jobs on the Gravitino server fetches them with
  `JobResourceUtils.localizeJobTemplate(jobTemplate, context.stagingDir())`, which returns a copy of
  the job template whose resources are the fetched local files. The `local` job executor does this,
  and runs the job in the staging directory.
- A job executor that hands jobs to an external job runner can pass the URIs on to it instead, so
  the resources are fetched where the job actually runs.

`submitJob(JobTemplate)` is deprecated. A job executor that still implements only this method keeps
working: the default implementation of `submitJob(JobContext, JobTemplate)` localizes the job
template into the staging directory and passes it on. Gravitino refuses to load a job executor that
implements neither method.

### Track Jobs

Gravitino tracks the jobs by pulling their state with `getJobExecutionInfo`, which every job executor
must implement. It returns the job's status, and when the job actually started and finished. Report
these times whenever the job runner provides them: Gravitino pulls job states only every
`gravitino.job.statusPullIntervalInMs`, so without them it can only record when it observed each
status change, and a job that starts and finishes between two pulls gets no start time. Once a job
has started, keep reporting its start time in every later state, including the finished one.

### Register the Job Executor

After you implement your own job executor, you need to register it in the Gravitino server by
using the `gravitino.conf` file. For example, if you have implemented a job executor named
`airflow`, you need to configure it as follows:

```
gravitino.job.executor = airflow
gravitino.jobExecutor.airflow.class = com.example.MyAirflowJobExecutor
```

Configure the job executor with additional properties, like:

```
gravitino.jobExecutor.airflow.host = http://localhost:8080
gravitino.jobExecutor.airflow.username = myuser
gravitino.jobExecutor.airflow.password = mypassword
```

These properties will be passed to the airflow job executor when it is instantiated.
