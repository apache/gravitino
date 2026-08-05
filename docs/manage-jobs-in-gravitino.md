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
`gravitino.jobExecutor.local.sparkHome` or `SPARK_HOME` set before the server starts, or the job
fails to launch.

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

### Job System Configuration

Configure the job system through the `gravitino.conf` file. The following are the
default configurations:

| Property name                          | Description                                                                       | Default value                 | Required |
|----------------------------------------|-----------------------------------------------------------------------------------|-------------------------------|----------|
| `gravitino.job.stagingDir`             | Directory for managing the staging files when running jobs                        | `/tmp/gravitino/jobs/staging` | No       |
| `gravitino.job.executor`               | The job executor to use for running jobs                                          | `local`                       | No       |
| `gravitino.job.stagingDirKeepTimeInMs` | The time in milliseconds to keep the staging directory after the job is completed | `604800000` (7 days)          | No       |
| `gravitino.job.statusPullIntervalInMs` | The interval in milliseconds to pull the job status from the job executor         | `300000` (5 minutes)          | No       |


#### Configurations for Local Job Executor

The local job executor is used for testing and development purposes, it runs the job in the local process.
The following are the default configurations for the local job executor:

| Property name                                       | Description                                                                                                                                       | Default value                          | Required |
|-----------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------|----------|
| `gravitino.jobExecutor.local.waitingQueueSize`      | The size of the waiting queue for queued jobs in the local job executor                                                                           | `100`                                  | No       |
| `gravitino.jobExecutor.local.maxRunningJobs`        | The maximum number of running jobs in the local job executor                                                                                      | `max(1, min(available cores / 2, 10))` | No       |
| `gravitino.jobExecutor.local.jobStatusKeepTimeInMs` | The time in milliseconds to keep the job status in the local job executor                                                                         | `3600000` (1 hour)                     | No       |
| `gravitino.jobExecutor.local.sparkHome`             | The home directory of Spark, Gravitino checks this configuration firstly and then `SPARK_HOME` env. Either of them should be set to run Spark job | `None`                                 | No       |

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
