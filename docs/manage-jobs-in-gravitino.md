---
title: "Manage Jobs"
slug: "/manage-jobs-in-gravitino"
keyword: "job management, job template, shell job, spark job, Gravitino"
license: "This software is licensed under the Apache License version 2."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Introduction

This page covers the Gravitino API for job templates and jobs. For what a template is, how it
relates to a run, and what the job statuses mean, see [Jobs](./jobs.md).

Jobs run through a job executor, set with `gravitino.job.executor`. The default, `local`, launches
the job as a process on the Gravitino server and is intended for testing. Running jobs anywhere else
means implementing an executor. See
[Custom job executor](./development/custom-job-executor.md).

:::note
1. The job system is still under development, so some features may not be fully
   implemented yet.
2. The aim of the job system is not to replace the existing job executors. So, it can only
   support running a single job at a time, and it doesn't support job scheduling for now.
   :::

## Job Template Operations

### Register a Shell Template

A shell template runs an executable. Placeholders in `arguments`, `environments`, and `customFields`
are filled in when a job runs.

```json
{
  "name": "nightly_export",
  "jobType": "shell",
  "comment": "Exports a table to a drop location",
  "executable": "/opt/jobs/export.sh",
  "arguments": ["{{table}}", "{{target}}"],
  "environments": {"REGION": "{{region}}"},
  "scripts": ["/opt/jobs/lib/common.sh"]
}
```

`executable` and `scripts` must be reachable by the Gravitino server, which accepts local paths and
HTTP, HTTPS, FTP, and FTPS URLs.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
  "jobTemplate": {
    "name": "nightly_export",
    "jobType": "shell",
    "comment": "Exports a table to a drop location",
    "executable": "/opt/jobs/export.sh",
    "arguments": ["{{table}}", "{{target}}"]
  }
}' http://localhost:8090/api/metalakes/example/jobs/templates
```

</TabItem>
</Tabs>

### Register a Spark Template

A Spark template submits an application. Running one with the local executor needs either
`gravitino.jobExecutor.local.sparkHome` or `SPARK_HOME` set before the server starts, pointing to a
Spark installation with an executable `bin/spark-submit`. Otherwise, the run request is rejected with
an error that names the missing setting, and no job is created.

```json
{
  "name": "nightly_rollup",
  "jobType": "spark",
  "comment": "Rolls up daily aggregates",
  "executable": "/opt/jobs/rollup.jar",
  "className": "com.example.Rollup",
  "arguments": ["{{date}}"],
  "configs": {"spark.executor.memory": "4g"}
}
```

### List, Get, and Delete Templates

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/example/jobs/templates

curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/example/jobs/templates/nightly_export

curl -X DELETE -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/example/jobs/templates/nightly_export
```

</TabItem>
<TabItem value="java" label="Java">

```java
List<JobTemplate> templates = client.listJobTemplates();
JobTemplate template = client.getJobTemplate("nightly_export");
boolean deleted = client.deleteJobTemplate("nightly_export");
```

</TabItem>
<TabItem value="python" label="Python">

```python
templates = client.list_job_templates()
template = client.get_job_template("nightly_export")
deleted = client.delete_job_template("nightly_export")
```

</TabItem>
</Tabs>

A template cannot be deleted while jobs from it are queued or running.

### Alter a Template

| Change             | JSON                                                     | Java                                                |
|--------------------|----------------------------------------------------------|-----------------------------------------------------|
| Rename             | `{"@type":"rename","newName":"nightly_export_v2"}`       | `JobTemplateChange.rename("nightly_export_v2")`     |
| Update the comment | `{"@type":"updateComment","newComment":"new_comment"}`   | `JobTemplateChange.updateComment("new_comment")`    |
| Update the template| `{"@type":"updateTemplate","newTemplate":{...}}`         | `JobTemplateChange.updateTemplate(...)`             |

## Job Operations

### Run a Job

Running names a template and supplies values for its placeholders.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
  "jobTemplateName": "nightly_export",
  "jobConf": {
    "table": "sales.public.orders",
    "target": "s3a://exports/orders",
    "region": "us"
  }
}' http://localhost:8090/api/metalakes/example/jobs/runs
```

</TabItem>
<TabItem value="java" label="Java">

```java
JobHandle job = client.runJob(
    "nightly_export",
    ImmutableMap.of(
        "table", "sales.public.orders",
        "target", "s3a://exports/orders",
        "region", "us"));
```

</TabItem>
<TabItem value="python" label="Python">

```python
job = client.run_job(
    job_template_name="nightly_export",
    job_conf={
        "table": "sales.public.orders",
        "target": "s3a://exports/orders",
        "region": "us",
    })
```

</TabItem>
</Tabs>

### List Jobs, Get a Job, and Cancel

Listing can be filtered to one template. A job is identified by its id, and cancelling is a `POST`
to the job's own path.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
  "http://localhost:8090/api/metalakes/example/jobs/runs?jobTemplateName=nightly_export"

curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/example/jobs/runs/{job_id}

curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/example/jobs/runs/{job_id}
```

</TabItem>
<TabItem value="java" label="Java">

```java
List<JobHandle> jobs = client.listJobs("nightly_export");
JobHandle job = client.getJob(jobId);
JobHandle cancelling = client.cancelJob(jobId);
```

</TabItem>
<TabItem value="python" label="Python">

```python
jobs = client.list_jobs(job_template_name="nightly_export")
job = client.get_job(job_id)
cancelling = client.cancel_job(job_id)
```

</TabItem>
</Tabs>

Cancelling is a request rather than an instant. The job moves to `CANCELLING` and then to
`CANCELLED`, and one that finishes first keeps the status it finished with.

### Get a Job's Output

A job's captured stdout/stderr can be fetched alongside its metadata by asking for it explicitly.
Output is fetched live from the job executor on every call rather than stored in Gravitino, so it's
only included when requested - a plain `getJob`/`get_job` call, or `listJobs`/`list_jobs`, never
returns it.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
  "http://localhost:8090/api/metalakes/example/jobs/runs/{job_id}?includeOutput=true"
```

</TabItem>
<TabItem value="java" label="Java">

```java
JobHandle job = client.getJob(jobId, true);
List<String> stdout = job.stdout();
List<String> stderr = job.stderr();
```

</TabItem>
<TabItem value="python" label="Python">

```python
job = client.get_job(job_id, include_output=True)
stdout = job.stdout()
stderr = job.stderr()
```

</TabItem>
</Tabs>

Output is only kept for as long as the job executor retains it - for the local job executor, that's
tied to `gravitino.jobExecutor.local.jobStatusKeepTimeInMs` below, and it's lost entirely across a
server restart. The number of lines returned is capped by `gravitino.job.outputMaxLines`.

### Job System Configuration

Configure the job system through the `gravitino.conf` file. The following are the
default configurations:

| Property name                          | Description                                                                       | Default value                 | Required |
|----------------------------------------|-----------------------------------------------------------------------------------|-------------------------------|----------|
| `gravitino.job.stagingDir`             | Directory for managing the staging files when running jobs                        | `/tmp/gravitino/jobs/staging` | No       |
| `gravitino.job.executor`               | The job executor to use for running jobs                                          | `local`                       | No       |
| `gravitino.job.stagingDirKeepTimeInMs` | The time in milliseconds to keep the staging directory after the job is completed | `604800000` (7 days)          | No       |
| `gravitino.job.statusPullIntervalInMs` | The interval in milliseconds to pull the job status from the job executor         | `300000` (5 minutes)          | No       |
| `gravitino.job.outputMaxLines`         | The maximum number of lines returned when fetching a job's stdout/stderr output   | `1000`                        | No       |

#### Configurations for Local Job Executor

The local job executor is used for testing and development purposes, it runs the job in the local process.
The following are the default configurations for the local job executor:

| Property name                                       | Description                                                                                                                                       | Default value                          | Required |
|-----------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------|----------|
| `gravitino.jobExecutor.local.waitingQueueSize`      | The size of the waiting queue for queued jobs in the local job executor                                                                           | `100`                                  | No       |
| `gravitino.jobExecutor.local.maxRunningJobs`        | The maximum number of running jobs in the local job executor                                                                                      | `max(1, min(available cores / 2, 10))` | No       |
| `gravitino.jobExecutor.local.jobStatusKeepTimeInMs` | The time in milliseconds to keep the job status in the local job executor                                                                         | `3600000` (1 hour)                     | No       |
| `gravitino.jobExecutor.local.sparkHome`             | The home directory of Spark, Gravitino checks this configuration firstly and then `SPARK_HOME` env. Either of them should be set to run Spark job | `None`                                 | No       |

The local job executor runs up to `gravitino.jobExecutor.local.maxRunningJobs` jobs at the same
time, each in its own process on the Gravitino server host, and queues the others. Make sure the
host has enough resources for that many jobs, or lower this value, especially when running Spark
jobs.

When multiple Gravitino servers share the same metadata store, each server's local job executor
only tracks the jobs it runs itself:

- A job can only be run and tracked by the server that received the run request. Other servers
  skip it when pulling job statuses.
- Cancelling a job on a server that doesn't run it marks the job as `CANCELLING`, and the server
  running the job cancels it the next time it pulls job statuses. This can take up to
  `gravitino.job.statusPullIntervalInMs`.
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

## Future Work

The job system still needs more work:

1. Support modification of job templates.
2. Support running Spark jobs (Java and PySpark) based on the Spark job template in the local job
   executor.
3. Support more job templates, like Python, SQL, etc.
4. Support more job executors, like Apache Airflow, Apache Livy, etc.
5. Support uploading job template related artifacts to the Gravitino server, also support
   downloading the artifacts from more distributed file systems like HDFS, S3, etc.
6. Support job scheduling, like running jobs periodically, or based on some events.
