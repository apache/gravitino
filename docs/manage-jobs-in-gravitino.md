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
the job as a process on the Gravitino server and is intended for testing. The `k8s` job executor
runs Spark jobs on Kubernetes. Running jobs anywhere else means implementing an executor. See
[Job Executors](#job-executors).

:::note
1. The job system is still under development, so some features may not be fully
   implemented yet.
2. The aim of the job system is not to replace the existing job executors, and it doesn't
   support job scheduling for now.
   :::

## Job Template Operations

### Register a Shell Template

A shell template runs an executable. Its placeholders are filled in with the job configuration when
a job runs. See [Placeholders](#placeholders).

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

`executable` and `scripts` are fetched by the job executor. With the `local` job executor they must
be reachable by the Gravitino server, which accepts local paths and HTTP, HTTPS, FTP, and FTPS URLs.

With the local job executor, each of them is fetched into the job's working directory under the
file name of its path, so it must point to a file, and two different resources of a template must
not share a file name: a run is rejected otherwise, instead of one file silently overwriting the
other. The same applies to the `executable`, `jars`, `files` and `archives` of a Spark template.
The Kubernetes job executor doesn't fetch the resources, see
[Kubernetes Job Executor](./k8s-job-executor.md#job-resources).

`executable` can also be a command name with no path, such as `python` or `bash`. Such a command is
not fetched, it is looked up where the job runs: the `local` job executor looks it up on the `PATH`
of the Gravitino server process, and setting `PATH` in `environments` doesn't change that. The
scripts are still fetched next to it, into the job's working directory, so a template can run a
script with an installed interpreter:

```json
{
  "name": "python_report",
  "jobType": "shell",
  "executable": "python",
  "arguments": ["report.py", "{{date}}"],
  "scripts": ["https://repo.example.com/jobs/report.py"]
}
```

A file name without a path, such as `run.sh`, is a command name too. Earlier versions fetched it as
a file relative to the working directory of the Gravitino server; to run a file, write its absolute
path or URI instead. To avoid running a different program than the one that was fetched, a run is
rejected when a script has the same file name as a command-name `executable`.

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

A Spark template submits an application. What it needs depends on the job executor:

- The local job executor needs a Spark installation on the Gravitino server, see
  [Local Job Executor](./local-job-executor.md#requirements).
- The Kubernetes job executor needs every resource of the template to be a URI that the cluster can
  reach, such as `local://` for a file in the Spark image, `https://`, or `s3a://`, see
  [Kubernetes Job Executor](./k8s-job-executor.md#job-execution).

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

### Placeholders

Any string in a template can contain placeholders: `executable`, `className`, the entries of
`scripts`, `jars`, `files`, and `archives`, and the keys and values of `environments`, `configs`,
and `customFields`. When a job runs, each placeholder is replaced with a value from the job
configuration (`jobConf`).

| Syntax              | Meaning                                                                                      |
|---------------------|----------------------------------------------------------------------------------------------|
| `{{name}}`          | Required. A run without a value for `name` is rejected.                                      |
| `{{name:-default}}` | Optional. `default` is used when the job configuration has no value. It can be empty: `{{name:-}}`. |
| `\{{`               | A literal `{{`, for example to pass `{{ds}}` to another tool. Written as `"\\{{"` in JSON.   |

For example, a template with `"arguments": ["--date", "{{date}}", "--mode", "{{mode:-full}}"]` run
with `{"date": "2026-09-22"}` passes `--date 2026-09-22 --mode full` to the job.

:::caution
Templates registered before Gravitino supported default values could pass an unresolved
placeholder through to the job as literal text, which is how templates carried another tool's
syntax, such as `{{ds}}` or `{{.Values.image}}`. A placeholder with no value is now a required
parameter, so those runs are rejected. Escape such text as `\{{ds}}`, or give the parameter a
default value.
:::

- A value in the job configuration is used as is, including an empty string `""`. A `null` value
  counts as no value. Values are never scanned for placeholders.
- A default declared on one occurrence of a parameter applies to all of its occurrences. A
  template that gives the same parameter different defaults is rejected when it is registered or
  updated.
- A default value is used as is and can span lines, but its braces must be balanced, so the
  placeholder ends at the first `}}` outside of them. This makes a JSON object a valid default, for
  example `{{options:-{"k":"v"}}}`. A default with unbalanced braces, such as `{{options:-{}}`, is
  rejected when the template is registered or updated.
- If any required parameter has no value, the run request fails with an error that lists all the
  missing parameters. No job is created and no file is downloaded.
- Keys in the job configuration that the template does not use are ignored, and the server logs a
  warning.
- A placeholder name can contain ASCII letters, digits, `_`, `.`, and `-`. Text such as
  `{{ name }}`, with spaces, is not a placeholder and is passed through as is.
- Only `{{` needs escaping. A placeholder always starts with `{{`, so `}}` on its own is plain
  text. A value that ends with a backslash right before a placeholder doubles it, as in
  `\\{{name}}`, which keeps one backslash and still resolves the placeholder. A backslash anywhere
  else is plain text.
- A placeholder whose default value contains braces cannot be immediately followed by a literal
  `}`, because it is then unclear which `}}` closes it. Put the whole structure in the default
  value, or insert a space. A default value without braces, such as `{"k":{{v:-1}}}`, is fine.

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

A job carries three timestamps:

- `queuedAt`: when Gravitino submitted the job to the job executor.
- `startedAt`: when the job started executing.
- `finishedAt`: when the job finished.

Gravitino pulls job statuses from the job executor every `gravitino.job.statusPullIntervalInMs`,
so a job's status can lag behind by up to this interval. The timestamps usually don't lag: the local
and Kubernetes job executors report when each job actually started and finished, even for a job that
starts and finishes between two pulls. A job executor that doesn't report these times gets the time
Gravitino first observes the job running or finished instead. In that case, a job that finishes
between two pulls has no `startedAt`.

For the cases in which the actual times are lost, see
[Local Job Executor](./local-job-executor.md#job-output-and-times) and
[Kubernetes Job Executor](./k8s-job-executor.md#job-output-and-times).

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

Output is only kept for as long as the job executor retains it: the local job executor keeps it in
the job's staging directory, and the Kubernetes job executor reads it from the driver pod of the
job. See [Local Job Executor](./local-job-executor.md#job-output-and-times) and
[Kubernetes Job Executor](./k8s-job-executor.md#job-output-and-times).
What's returned is always the tail of the output (the most recent content), capped
by `gravitino.job.outputMaxLines` (line count) and `gravitino.job.outputMaxBytes` (byte size),
whichever limit is hit first.

The REST API also accepts `outputMaxLines`/`outputMaxBytes` query parameters to request less output
than these global caps for a single call (e.g. a quick check that doesn't need the full 1000
lines) - a value larger than the global cap is clamped down to it, so the global configuration
always remains a hard upper bound:

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
  "http://localhost:8090/api/metalakes/example/jobs/runs/{job_id}?includeOutput=true&outputMaxLines=50&outputMaxBytes=8192"
```

### Job System Configuration

Configure the job system through the `gravitino.conf` file. The following are the
default configurations:

| Property name                          | Description                                                                                                                                                                                                                    | Default value                 | Required |
|----------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------|----------|
| `gravitino.job.stagingDir`             | Directory for managing the staging files when running jobs. With the local job executor, it must be shared by all servers in a multi-server deployment, see [Local Job Executor](./local-job-executor.md#job-output-and-times) | `/tmp/gravitino/jobs/staging` | No       |
| `gravitino.job.executor`               | The job executor to use for running jobs, see [Job Executors](#job-executors)                                                                                                                                                  | `local`                       | No       |
| `gravitino.job.stagingDirKeepTimeInMs` | The time in milliseconds to keep the staging directory after the job is completed                                                                                                                                              | `604800000` (7 days)          | No       |
| `gravitino.job.statusPullIntervalInMs` | The interval in milliseconds to pull the job status from the job executor                                                                                                                                                      | `300000` (5 minutes)          | No       |
| `gravitino.job.outputMaxLines`         | The maximum number of lines returned when fetching a job's stdout/stderr output                                                                                                                                                | `1000`                        | No       |
| `gravitino.job.outputMaxBytes`         | The maximum number of bytes read from the tail of a job's stdout/stderr output                                                                                                                                                 | `262144` (256KB)              | No       |

## Job Executors

A job executor decides where the jobs run. Set it with `gravitino.job.executor`:

| Job executor | Jobs run as                                                      | Job types    | Intended for            | Configuration                                    |
|--------------|------------------------------------------------------------------|--------------|-------------------------|--------------------------------------------------|
| `local`      | Processes on the Gravitino server                                | Shell, Spark | Testing and development | [Local Job Executor](./local-job-executor.md)    |
| `k8s`        | `SparkApplication` resources of the Spark operator on Kubernetes | Spark        | Production              | [Kubernetes Job Executor](./k8s-job-executor.md) |

To run jobs anywhere else, implement a
[custom job executor](./development/custom-job-executor.md).

## Future Work

The job system still needs more work:

1. Support more job templates, like Python, SQL, etc.
2. Support more job executors, like Apache Airflow, Apache Livy, etc.
3. Support uploading job template related artifacts to the Gravitino server, also support
   downloading the artifacts from more distributed file systems like HDFS, S3, etc.
4. Support job scheduling, like running jobs periodically, or based on some events.
