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
