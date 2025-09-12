---
title: "Manage jobs in Gravitino"
slug: /manage-jobs-in-gravitino
date: 2025-08-13
keywords:
  - job
  - job template
  - gravitino
license: "This software is licensed under the Apache License version 2."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Introduction

Starting from 1.0.0, Apache Gravitino introduces a new submodule called the job system for users to
register, run, and manage jobs. This job system integrates with the existing metadata
management, enabling users to execute the jobs or actions based on the metadata, 
known as metadata-driven actions. For instance, this allows users to run jobs for tasks such as 
compacting Iceberg tables or cleaning old data based on TTL properties.

The aim of the job system is to provide a unified way to manage job templates and jobs,
including registering job templates, running jobs based on the job templates, and other related 
tasks. The job system itself is a unified job submitter that allows users to run jobs through it,
but it doesn't provide the actual job execution capabilities. Instead, it relies on the
existing job executors (schedulers), such as Apache Airflow, Apache Livy, to execute the jobs.
Gravitino's job system provides an extensible way to connect to different job executors.

:::note
1. The job system is a new feature introduced in Gravitino 1.0.0, and it is still under
   development, so some features may not be fully implemented yet.
2. The aim of the job system is not to replace the existing job executors. So, it can only
   support running a single job at a time, and it doesn't support job scheduling for now.
   :::

## Job operations

### Register a new job template

Before running a job, the first step is to register a job template. Currently, Gravitino
supports two types of job templates: `shell` and `spark` (we will add more job templates in the
future).

#### Shell job template

The `shell` job template is used to run scripts, it can be a shell script, or any executable
script. The template is defined as follows:

```json
{
  "name": "my_shell_job_template",
  "jobType": "shell",
  "comment": "A shell job template to run a script",
  "executable": "/path/to/my_script.sh",
  "arguments": ["{{arg1}}", "{{arg2}}"],
  "environments": {
    "ENV_VAR1": "{{value1}}",
    "ENV_VAR2": "{{value2}}"
  },
  "customFields": {
    "field1": "{{value1}}",
    "field2": "{{value2}}"
  },
  "scripts": ["/path/to/script1.sh", "/path/to/script2.sh"]
}
```

Here is a brief description of the fields in the job template:

- `name`: The name of the job template, must be unique.
- `jobType`: The type of the job template, use `shell` for a shell job template.
- `comment`: A comment for the job template, which can be used to describe the job template.
- `executable`: The path to the executable script, which can be a shell script or any executable script.
- `arguments`: The arguments to pass to the executable script, you can use placeholders like `{{arg1}}`
  and `{{arg2}}` to be replaced with actual values when running the job.
- `environments`: The environment variables to set when running the job, you can use placeholders like
  `{{value1}}` and `{{value2}}` to be replaced with actual values when running the job.
- `customFields`: Custom fields for the job template, which can be used to store additional
  information, you can use placeholders like `{{value1}}` and `{{value2}}` to be replaced with actual
  values when running the job.
- `scripts`: A list of scripts that the main executable script can use.

Please note that:

1. The `executable` and `scripts` must be accessible by the Gravitino server. Currently,
   Gravitino supports accessing files from the local file system, HTTP(S) URLs, and FTP(S) URLs
   (more distributed file system support will be added in the future). So the `executable` and
   `scripts` can be a local file path, or a URL like `http://example.com/my_script.sh`.
2. The `arguments`, `environments`, and `customFields` can use placeholders like `{{arg1}}` and
   `{{value1}}` to be replaced with actual values when running the job. The placeholders will be
   replaced with the actual values when running the job, so you can use them to pass dynamic values
   to the job template.
3. Gravitino will copy the `executable` and `scripts` files to the job working directory
   when running the job, so you can use the relative path in the `executable` and `scripts` to
   refer to other scripts in the job working directory.

#### Spark job template

The `spark` job template is used to run Spark jobs, it is a Spark application JAR file for now.

**Note** that the Spark job support is still under development, in 1.0.0, it only supports
registering a Spark job template, running a Spark job is not supported yet.

The template is defined as follows:

```json
{
  "name": "my_spark_job_template",
  "jobType": "spark",
  "comment": "A Spark job template to run a Spark application",
  "executable": "/path/to/my_spark_app.jar",
  "arguments": ["{{arg1}}", "{{arg2}}"],
  "environments": {
    "ENV_VAR1": "{{value1}}",
    "ENV_VAR2": "{{value2}}"
  },
  "customFields": {
    "field1": "{{value1}}",
    "field2": "{{value2}}"
  },
  "className": "com.example.MySparkApp",
  "jars": ["/path/to/dependency1.jar", "/path/to/dependency2.jar"],
  "files": ["/path/to/file1.txt", "/path/to/file2.txt"],
  "archives": ["/path/to/archive1.zip", "/path/to/archive2.zip"],
  "configs": {
    "spark.executor.memory": "2g",
    "spark.executor.cores": "2"
  }
}
```

Here is a brief description of the fields in the Spark job template:

- `name`: The name of the job template, which must be unique.
- `jobType`: The type of the job template, use `spark` for Spark job template.
- `comment`: A comment for the job template, which can be used to describe the job template.
- `executable`: The path to the Spark application JAR file, which can be a local file path or a URL
  with a supported scheme.
- `arguments`: The arguments to pass to the Spark application, you can use placeholders like
  `{{arg1}}` and `{{arg2}}` to be replaced with actual values when running the job.
- `environments`: The environment variables to set when running the job, you can use placeholders like
  `{{value1}}` and `{{value2}}` to be replaced with actual values when running the job.
- `customFields`: Custom fields for the job template, which can be used to store additional information.
  It can use placeholders like `{{value1}}` and `{{value2}}` to be replaced with actual values
  when running the job.
- `className`: The main class of the Spark application, it is required for Spark job template.
- `jars`: A list of JAR files to add to the Spark job classpath, which can be a local file path or a URL
  with a supported scheme.
- `files`: A list of files to be copied to the working directory of the Spark job, which can be a local
  file path or a URL with a supported scheme.
- `archives`: A list of archives to be extracted to the working directory of the Spark job, which
  can be a local file path or a URL with a supported scheme.
- `configs`: A map of Spark configurations to set when running the Spark job, which can use placeholders
  like `{{value1}}` to be replaced with actual values when running the job.

Note that:

1. The `executable`, `jars`, `files`, and `archives` must be accessible by the Gravitino server.
   Currently, Gravitino support accessing files from the local file system, HTTP(S) URLs, and
   FTP(S) URLs (more distributed file system supports will be added in the future). So the
   `executable`, `jars`, `files`, and `archives` can be a local file path, or a URL like
   `http://example.com/my_spark_app.jar`.
2. The `arguments`, `environments`, `customFields`, and `configs` can use placeholders like
   `{{arg1}}` and `{{value1}}` to be replaced with actual values when running the job. The placeholders
   will be replaced with the actual values when running the job, so you can use them to pass dynamic
   values to the job template.
3. Gravitino will copy the `executable`, `jars`, `files`, and `archives` files to the job working
   directory when running the job, so you can use the relative path in the `executable`, `jars`,
   `files`, and `archives` to refer to other files in the job working directory.
4. The `className` is required for the Spark job template, it is the main class of the Spark 
   application to be executed.

To register a job template, you can use REST API or the Java and Python SDKs. Here is the
example to register a shell job template:

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
     -H "Content-Type: application/json" \
     -d '{
           "jobTemplate": {
             "name": "my_shell_job_template",
             "jobType": "shell",
             "comment": "A shell job template to run a script",
             "executable": "/path/to/my_script.sh",
             "arguments": ["{{arg1}}", "{{arg2}}"],
             "environments": {
               "ENV_VAR1": "{{value1}}",
               "ENV_VAR2": "{{value2}}"
             },
             "customFields": {
               "field1": "{{value1}}",
               "field2": "{{value2}}"
             },
             "scripts": ["/path/to/script1.sh", "/path/to/script2.sh"]
           }
        }' \
     http://localhost:8090/api/metalakes/test/jobs/templates
```

</TabItem>
<TabItem value="java" label="Java">

```java
  ShellJobTemplate jobTemplate = ShellJobTemplate.builder()
      .name("my_shell_job_template")
      .comment("A shell job template to run a script")
      .executable("/path/to/my_script.sh")
      .arguments(List.of("{{arg1}}", "{{arg2}}"))
      .environments(Map.of("ENV_VAR1", "{{value1}}", "ENV_VAR2", "{{value2}}"))
      .customFields(Map.of("field1", "{{value1}}", "field2", "{{value2}}"))
      .scripts(List.of("/path/to/script1.sh", "/path/to/script2.sh"))
      .build();

  GravitinoClient client = ...;
  client.registerJobTemplate(jobTemplate);
```

</TabItem>
<TabItem value="python" label="Python">

```python
  shell_job_template = (
      ShellJobTemplate.builder()
      .with_name("my_shell_job_template")
      .with_comment("A shell job template to run a script")
      .with_executable("/path/to/my_script.sh")
      .with_arguments(["{{arg1}}", "{{arg2}}"])
      .with_environments({"ENV_VAR1": "{{value1}}", "ENV_VAR2": "{{value2}}"})
      .with_custom_fields({"field1": "{{value1}}", "field2": "{{value2}}"})
      .with_scripts(["/path/to/script1.sh", "/path/to/script2.sh"])
      .build()
  )

  client = GravitinoClient(...)
  client.register_job_template(shell_job_template)
```

</TabItem>
</Tabs>

### List registered job templates

You can list all the registered job templates under a metalake by using the REST API or the Java
and Python SDKs.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
     http://localhost:8090/api/metalakes/test/jobs/templates

Or using query parameter "details=true" to get more details of the job templates:

curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
     http://localhost:8090/api/metalakes/test/jobs/templates?details=true
```

</TabItem>
<TabItem value="java" label="Java">

```java
    GravitinoClient client = ...;
    List<JobTemplate> detailedJobTemplates = client.listJobTemplates();
```

</TabItem>
<TabItem value="python" label="Python">

```python
    client = GravitinoClient(...)
    detailed_job_templates = client.list_job_templates()
```

</TabItem>
</Tabs>

### Get a registered job template by name

You can get a registered job template by its name using the REST API or the Java and Python SDKs.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
     http://localhost:8090/api/metalakes/test/jobs/templates/my_shell_job_template
```

</TabItem>
<TabItem value="java" label="Java">

```java
    GravitinoClient client = ...;
    JobTemplate jobTemplate = client.getJobTemplate("my_shell_job_template");
```

</TabItem>
<TabItem value="python" label="Python">

```python
    client = GravitinoClient(...)
    job_template = client.get_job_template("my_shell_job_template")
```

</TabItem>
</Tabs>

### Delete a registered job template by name

You can delete a registered job template by its name using the REST API or the Java and Python SDKs.

Note that deleting a job template will also delete all the jobs that are using this job template.
If there are queued, started, or to be cancelled jobs that are using this job template, the deletion
will fail with an `InUseException` error.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X DELETE -H "Accept: application/vnd.gravitino.v1+json" \
     http://localhost:8090/api/metalakes/test/jobs/templates/my_shell_job_template
```

</TabItem>
<TabItem value="java" label="Java">

```java
    GravitinoClient client = ...;
    client.deleteJobTemplate("my_shell_job_template");
```

</TabItem>
<TabItem value="python" label="Python">

```python
    client = GravitinoClient(...)
    client.delete_job_template("my_shell_job_template")
```

</TabItem>
</Tabs>

### Run a job based on a job template

To run a job based on the registered job template, you can use the REST API or the Java and Python SDKs.
When running a job, you need to provide the job template name and the parameters to replace the
placeholders in the job template.

Gravitino leverages the job executor to run the job, so you need to specify the job executor
through configuration `gravitino.job.executor`. By default, it is set to "local", which means
the job will be launched as a process within the same machine that runs the Gravitino server. Note 
that the local job executor is only for testing. If you want to run the job in a distributed environment,
you need to implement your own `JobExecutor` and set the configuration, please see
[Implement a custom job executor](#implement-a-custom-job-executor) section below.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell

curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
     -H "Content-Type: application/json" \
     -d '{
           "jobTemplateName": "my_shell_job_template",
           "jobConf": {
             "arg1": "value1",
             "arg2": "value2",
             "value1": "env_value1",
             "value2": "env_value2"
           }
        }' \
     http://localhost:8090/api/metalakes/test/jobs/runs
```

</TabItem>
<TabItem value="java" label="Java">

```java
    GravitinoClient client = ...;
    JobHandle jobHandle = client.runJob("my_shell_job_template", ImmutableMap.of(
        "arg1", "value1",
        "arg2", "value2",
        "value1", "env_value1",
        "value2", "env_value2"
    ));
```

</TabItem>
<TabItem value="python" label="Python">

```python
    client = GravitinoClient(...)
    job_handle = client.run_job("my_shell_job_template", {
        "arg1": "value1",
        "arg2": "value2",
        "value1": "env_value1",
        "value2": "env_value2"
    })
```

</TabItem>
</Tabs>

The returned `JobHandle` contains the job ID and other information about the job. You can use the job ID to
check the job status and cancel the job.

The runJob API will return immediately after the job is submitted to the job executor, and the job will be
executed asynchronously. You can check the job status using the job ID returned by the runJob API.

### List all jobs

You can list all the jobs under a metalake by using the REST API or the Java and Python SDKs.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
     http://localhost:8090/api/metalakes/test/jobs/runs

Or using query parameter "jobTemplateName=my_shell_job_template" to filter jobs by job template name:

curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
     http://localhost:8090/api/metalakes/test/jobs/runs?jobTemplateName=my_shell_job_template
```

</TabItem>
<TabItem value="java" label="Java">

```java
    GravitinoClient client = ...;
    List<JobHandle> jobHandles = client.listJobs();

    // To filter jobs by job template name
    List<JobHandle> filteredJobHandles = client.listJobs("my_shell_job_template");

```

</TabItem>
<TabItem value="python" label="Python">

```python
    client = GravitinoClient(...)
    job_handles = client.list_jobs()

    # To filter jobs by job template name
    filtered_job_handles = client.list_jobs(job_template_name="my_shell_job_template")
```

</TabItem>
</Tabs>

### Get a job by job ID

You can get a job by its job ID using the REST API or the Java and Python SDKs.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
     http://localhost:8090/api/metalakes/test/jobs/runs/job-1234567890
```

</TabItem>
<TabItem value="java" label="Java">

```java
    GravitinoClient client = ...;
    JobHandle jobHandle = client.getJob("job-1234567890");
```

</TabItem>
<TabItem value="python" label="Python">

```python
    client = GravitinoClient(...)
    job_handle = client.get_job("job-1234567890")
```

</TabItem>
</Tabs>

### Cancel a job by job ID

You can cancel a job by its job ID using the REST API or the Java and Python SDKs.

The job will be cancelled asynchronously, and the job status will be updated to `CANCELLING` first,
then to `CANCELLED` when the cancellation is completed. If the job is already in `SUCCEEDED`,
`FAILED`, `CANCELLING`, or `CANCELLED` status, the cancellation will be ignored.

The cancellation will be done by the job executor with the best effort, it relies on the job
executor that supports cancellation. Also, because of the asynchronous nature of the job
cancellation, the job may not be actually cancelled.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
     http://localhost:8090/api/metalakes/test/jobs/runs/job-1234567890
```

</TabItem>
<TabItem value="java" label="Java">

```java
    GravitinoClient client = ...;
    client.cancelJob("job-1234567890");
```

</TabItem>
<TabItem value="python" label="Python">

```python
    client = GravitinoClient(...)
    client.cancel_job("job-1234567890")
```

</TabItem>
</Tabs>

### Configurations of the job system

You can configure the job system through the `gravitino.conf` file. The following are the
default configurations:

| Property name                          | Description                                                                       | Default value                 | Required | Since Version |
|----------------------------------------|-----------------------------------------------------------------------------------|-------------------------------|----------|---------------|
| `gravitino.job.stagingDir`             | Directory for managing the staging files when running jobs                        | `/tmp/gravitino/jobs/staging` | No       | 1.0.0         |
| `gravitino.job.executor`               | The job executor to use for running jobs                                          | `local`                       | No       | 1.0.0         |
| `gravitino.job.stagingDirKeepTimeInMs` | The time in milliseconds to keep the staging directory after the job is completed | `604800000` (7 days)          | No       | 1.0.0         |
| `gravitino.job.statusPullIntervalInMs` | The interval in milliseconds to pull the job status from the job executor         | `300000` (5 minutes)          | No       | 1.0.0         |


#### Configurations for local job executor

The local job executor is used for testing and development purposes, it runs the job in the local process.
The following are the default configurations for the local job executor:

| Property name                                       | Description                                                               | Default value                          | Required | Since Version |
|-----------------------------------------------------|---------------------------------------------------------------------------|----------------------------------------|----------|---------------|
| `gravitino.jobExecutor.local.waitingQueueSize`      | The size of the waiting queue for queued jobs in the local job executor   | `100`                                  | No       | 1.0.0         |
| `gravitino.jobExecutor.local.maxRunningJobs`        | The maximum number of running jobs in the local job executor              | `max(1, min(available cores / 2, 10))` | No       | 1.0.0         |
| `gravitino.jobExecutor.local.jobStatusKeepTimeInMs` | The time in milliseconds to keep the job status in the local job executor | `3600000` (1 hour)                     | No       | 1.0.0         |

### Implement a custom job executor

Gravitino's job system is designed to be extensible, allowing you to implement your own job executor
to run jobs in a distributed environment. You can refer to the interface `JobExecutor` in the
code [here](https://github.com/apache/gravitino/blob/main/core/src/main/java/org/apache/gravitino/connector/job/JobExecutor.java).

After you implement your own job executor, you need to register it in the Gravitino server by
using the `gravitino.conf` file. For example, if you have implemented a job executor named
`airflow`, you need to configure it as follows:

```
gravitino.job.executor = airflow
gravitino.jobExecutor.airflow.class = com.example.MyAirflowJobExecutor
```

You can also configure the job executor with additional properties, like:

```
gravitino.jobExecutor.airflow.host = http://localhost:8080
gravitino.jobExecutor.airflow.username = myuser
gravitino.jobExecutor.airflow.password = mypassword
```

These properties will be passed to the airflow job executor when it is instantiated.

## Future work

The job system is a new feature introduced in Gravitino 1.0.0, and it still needs more work:

1. Support modification of job templates.
2. Support running Spark jobs (Java and PySpark) based on the Spark job template in the local job
   executor.
3. Support more job templates, like Python, SQL, etc.
4. Support more job executors, like Apache Airflow, Apache Livy, etc.
5. Support uploading job template related artifacts to the Gravitino server, also support
   downloading the artifacts from more distributed file systems like HDFS, S3, etc.
6. Support job scheduling, like running jobs periodically, or based on some events.
