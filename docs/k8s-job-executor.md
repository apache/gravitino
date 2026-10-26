---
title: "Kubernetes Job Executor"
slug: "/k8s-job-executor"
keyword: "job executor, Kubernetes, k8s, Spark operator, SparkApplication, YuniKorn, Gravitino"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

The `k8s` job executor runs Spark jobs on Kubernetes. Each job becomes a `SparkApplication` resource
of the [Apache Spark Kubernetes Operator](https://github.com/apache/spark-kubernetes-operator), which
starts the driver and executor pods of the job.

All the job state lives in Kubernetes, not in the Gravitino server. Jobs keep running while Gravitino
restarts, and every Gravitino server of a deployment can query and cancel any job.

The `k8s` job executor only runs Spark templates. A run of a shell template is rejected.

For job templates and the job API, see [Manage Jobs](./manage-jobs-in-gravitino.md). To run jobs as
processes on the Gravitino server instead, see [Local Job Executor](./local-job-executor.md).

## Requirements

- Apache Spark Kubernetes Operator 0.9.x in the Kubernetes cluster that runs the jobs, watching
  every namespace the jobs run in. Operator 1.0 and later require Spark 4.0 or later, while the
  built-in jobs of Gravitino are built for Spark 3.5.
- A Spark 3.5 image, see [Spark Image](#spark-image).
- Access from the Gravitino server to the API server of the cluster, with the permissions listed in
  [Kubernetes Permissions](#kubernetes-permissions).

The job executor is the `gravitino-k8s-job-executor-<version>.jar` file in the `libs` directory of
the Gravitino distribution. Nothing else needs to be installed on the Gravitino server.

## Job Execution

When a job runs, the job executor creates a `SparkApplication` from the job template and the job
configuration:

| Job template                | SparkApplication                                                                       |
|-----------------------------|----------------------------------------------------------------------------------------|
| `executable`                | `spec.jars`, or `spec.pyFiles` for a file that ends with `.py`                         |
| `className`                 | `spec.mainClass`                                                                       |
| `arguments`                 | `spec.driverArgs`                                                                      |
| `configs`                   | `spec.sparkConf`                                                                       |
| `environments`              | `spark.kubernetes.driverEnv.<name>` and `spark.executorEnv.<name>` in `spec.sparkConf` |
| `jars`, `files`, `archives` | Appended to `spark.jars`, `spark.files`, and `spark.archives` in `spec.sparkConf`      |

The Spark configurations of a job come from these places, each overriding the ones before it:

1. The server-level defaults, set with the `gravitino.jobExecutor.k8s.spark.conf.` prefix.
2. The `environments` of the job template.
3. The `configs` of the job template.
4. For a built-in Iceberg job, its `spark_conf` parameter, see [Built-In Jobs](#built-in-jobs).

A blank value in the `configs` of a job template or in `spark_conf` leaves the configuration unset,
so it doesn't override a server-level default. This is what an optional placeholder without a value
becomes. Any other value replaces the default; a list such as `spark.jars` isn't merged with it.

The job executor then adjusts the result:

- The operator decides the master and the deploy mode, so `spark.master` and
  `spark.submit.deployMode` are removed. A job whose `spark.master` isn't a Kubernetes master, that
  is one starting with `k8s://`, such as `local[*]`, is rejected, and so is one whose
  `spark.submit.deployMode` is another mode than `cluster`, such as `client`.
- `spark.kubernetes.container.image` and `spark.kubernetes.authenticate.driver.serviceAccountName`
  take the values of `spark.image` and `spark.serviceAccount` of the job executor if neither the
  job template nor a server-level default sets them. A job template can therefore run with its
  own image and service account.
- `spark.kubernetes.namespace` is the namespace that the `configs` of the job template set, or else
  `namespace` of the job executor. A server-level default of it with the
  `spark.conf.` prefix is ignored: set `namespace` instead.

The `SparkApplication` is named `<namePrefix>-<job ID>`, for example
`gravitino-job-688822913391439376`, and carries the labels `app.kubernetes.io/managed-by=gravitino`
and `gravitino.apache.org/job-id=<job ID>`. List the jobs of a namespace with:

```shell
kubectl get sparkapplications -l app.kubernetes.io/managed-by=gravitino
```

### Job Resources

Gravitino doesn't fetch the resources of a job. It passes their URIs on to Spark, which fetches them
in the cluster. The `executable`, `jars`, `files`, and `archives` of a template must therefore be
URIs that the driver and executor pods can reach, such as `local://` for a file in the Spark image,
`https://`, `s3a://`, or `hdfs://`. So must every entry of `spark.jars`, `spark.files`, and
`spark.archives`, whether a job template, `spark_conf`, or a server-level default sets them. A run
with a path on the Gravitino server, that is a path without a scheme or with the `file` scheme, is
rejected.

The built-in job templates are the exception. Their `executable` is the path of the built-in jobs
jar on the Gravitino server, and the job executor replaces it with `spark.builtinJobsJar`, by default
`local:///opt/gravitino/jobs/gravitino-jobs.jar`, a file in the [Spark image](#spark-image).

### Built-In Jobs

The built-in job templates set the resources of the driver and the executors in their own
`configs`, from placeholders. Size a built-in job with its job configuration, for example
`spark_executor_instances`, `spark_executor_cores`, `spark_executor_memory`, and
`spark_driver_memory`, not with server-level defaults: the `configs` of a template override them.
Leave `spark_master` empty, its default: any other value than a Kubernetes master is rejected.

The `spark_conf` parameter of the built-in Iceberg jobs, a flat JSON map of Spark configurations, is
set in the `SparkApplication` when the job is submitted, over all the other Spark configurations of
the job. So configurations that Spark reads when it starts the driver take effect, such as
`spark.jars`, `spark.jars.packages`, and `spark.driver.*`. For example, a job configuration can add
the Iceberg Spark runtime to a Spark image without it:

```json
{
  "spark_conf": "{\"spark.jars\":\"https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.11.0/iceberg-spark-runtime-3.5_2.12-1.11.0.jar\"}"
}
```

- `spark.master` and `spark.submit.deployMode` in `spark_conf` are ignored, with a warning in the
  server log.
- The `spark.kubernetes.*` configurations in `spark_conf`, such as the namespace, the image, the
  service account, Secrets, and volumes, are ignored, with a warning in the server log that leaves
  out the value. Whoever can run a built-in job sets its `spark_conf`, so only the job executor
  configurations and job templates may set them. The driver ignores them as well.
- `spark.jars` in `spark_conf` replaces a server-level default of `spark.jars`, like any other
  configuration. If that default provides the Iceberg Spark runtime, list the runtime in
  `spark_conf` as well.
- The names of `spark.executorEnv.*` in `spark_conf` are checked like the `environments` of a job
  template, see [Environment Variables](#environment-variables).
- The driver applies the rest of `spark_conf` again when it starts, as it does with every job
  executor.

Configurations that every job needs, such as the Iceberg Spark runtime, the scheduler
configurations, or volumes, can also be set once as server-level defaults, or built into the
[Spark image](#spark-image).

### Rejected Runs

A run is rejected, and no job is created, when:

- The job template is a shell template.
- A resource of the job is a path on the Gravitino server, see [Job Resources](#job-resources).
- `spark.master` isn't a Kubernetes master, or `spark.submit.deployMode` is another mode than
  `cluster`, see [Job Execution](#job-execution).
- The `spark_conf` parameter of a built-in Iceberg job isn't a flat JSON map.
- `spark.kubernetes.namespace` isn't a valid namespace name, or an environment variable name is
  invalid, see [Environment Variables](#environment-variables).
- Kubernetes rejects the `SparkApplication`, for example because the namespace doesn't exist, the
  job executor lacks a permission, or the operator isn't installed. The error carries the reason
  that Kubernetes gives.

### Job Status

The job status follows the state of the `SparkApplication`:

| Job status  | SparkApplication                                                                                                                                              |
|-------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `QUEUED`    | No state yet, `Submitted`, `DriverRequested`, or `ScheduledToRestart`. The driver hasn't started, for example because its pod waits for resources.            |
| `STARTED`   | `DriverStarted`, `DriverReady`, `RunningHealthy`, `RunningWithPartialCapacity`, `InitializedBelowThresholdExecutors`, or `RunningWithBelowThresholdExecutors` |
| `SUCCEEDED` | `Succeeded`                                                                                                                                                   |
| `FAILED`    | `Failed`, `SchedulingFailure`, `DriverEvicted`, `DriverStartTimedOut`, `DriverReadyTimedOut`, or `ExecutorsStartTimedOut`                                     |
| `CANCELLED` | Cancelled through Gravitino                                                                                                                                   |

A job also fails in these cases:

- The operator writes no status for the `SparkApplication` within `noStatusTimeoutInMs`, for
  example because the operator isn't running or doesn't watch the namespace. The job executor
  deletes the `SparkApplication` before failing the job.
- The `SparkApplication` is deleted by someone else before Gravitino records the job as finished,
  even if the job succeeded.

After its outcome, a `SparkApplication` moves to `ResourceReleased` or
`TerminatedWithoutReleaseResources`. A job in these states keeps the outcome that the operator
recorded before them, or fails if there is none.

A state that the job executor doesn't know, for example of a newer operator, keeps the job `STARTED`
if it has started and `QUEUED` otherwise, until the operator records an outcome. A status that the
job executor can't read keeps the job `QUEUED` until the operator deletes the `SparkApplication`.
Use a supported operator version.

A job is one run of the application: the operator never restarts it.

## Setup

### Kubernetes Permissions

The job executor needs these permissions in every namespace that jobs run in. Only `list` on
`sparkapplications` is optional. It needs no permission on Secrets or ConfigMaps, and no
cluster-wide permission.

| Resource                                 | Verb     | Used to                                                                                                                                                                                                                                   |
|------------------------------------------|----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `sparkapplications` (`spark.apache.org`) | `create` | Submit a job                                                                                                                                                                                                                              |
| `sparkapplications` (`spark.apache.org`) | `get`    | Query, submit, and cancel a job, and read its output                                                                                                                                                                                      |
| `sparkapplications` (`spark.apache.org`) | `list`   | Query all the jobs of a namespace with one call. Optional: without it, the job executor gets the jobs one by one and logs a warning                                                                                                       |
| `sparkapplications` (`spark.apache.org`) | `update` | Mark a job as cancelled by Gravitino                                                                                                                                                                                                      |
| `sparkapplications` (`spark.apache.org`) | `delete` | Cancel a job, and delete the `SparkApplication` of a job that got no status or whose submission failed                                                                                                                                    |
| `pods`                                   | `list`   | Find the driver pod, for the output and the times of a job, and to check that the driver of a failed job has terminated. Without it, a failed job stays unfinished until its `SparkApplication` is deleted after `spark.ttlAfterStopInMs` |
| `pods`                                   | `delete` | Delete the driver pod of a failed job that hasn't terminated, for example after the driver start timeout. Without it, such a job stays unfinished until its `SparkApplication` is deleted                                                 |
| `pods/log`                               | `get`    | Read the output of a job                                                                                                                                                                                                                  |

For example, in the namespace `default`:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: gravitino-job-executor
  namespace: default
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: gravitino-job-executor
  namespace: default
rules:
  - apiGroups: ["spark.apache.org"]
    resources: ["sparkapplications"]
    verbs: ["create", "get", "list", "update", "delete"]
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["list", "delete"]
  - apiGroups: [""]
    resources: ["pods/log"]
    verbs: ["get"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: gravitino-job-executor
  namespace: default
subjects:
  - kind: ServiceAccount
    name: gravitino-job-executor
    namespace: default
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: gravitino-job-executor
```

The driver pod of a job runs with another service account, `spark.serviceAccount`, which Spark uses
to create the executor pods. The Helm chart of the operator creates the service account `spark` and
binds it to the cluster role `spark-workload-clusterrole` in the namespaces it is installed for, set
with `workloadResources.namespaces.data` of the chart.

### Job Isolation

The job executor doesn't restrict the Spark configurations of a job. Whoever can register a job
template controls the pods of its jobs, within what Kubernetes allows in their namespace. Through the
Spark configurations of the template, a job can, for example:

- Run in another namespace that the job executor has permissions in, with another service account.
- Mount `hostPath` volumes into its driver and executor pods.
- Read any Secret of its namespace into an environment variable with `secretKeyRef`, and print it to
  its output, which the job API returns.

The same applies to a job template that passes its job configuration on to these Spark
configurations, for example through a placeholder. The `spark_conf` parameter of the built-in
Iceberg jobs can't set the `spark.kubernetes.*` configurations, see [Built-In Jobs](#built-in-jobs),
but anyone who can run one of these jobs can set its other Spark configurations, such as
`spark.jars`, which adds code to the job. That code runs in the driver and the executors, with
their service accounts and the credentials of the job.

To limit what jobs can do:

- Run the jobs in dedicated namespaces that hold only the Secrets the jobs need, and grant the Role
  of the job executor only in those namespaces.
- Give the driver service account, `spark.serviceAccount`, only the permissions that Spark needs to
  manage the executors in the namespace of the job, like the `spark-workload-clusterrole` of the
  operator chart, and nothing else.
- Enforce [Pod Security Admission](https://kubernetes.io/docs/concepts/security/pod-security-admission/)
  in those namespaces, for example at the `baseline` level, which rejects pods with `hostPath`
  volumes.
- Grant `REGISTER_JOB_TEMPLATE`, `USE_JOB_TEMPLATE`, and `RUN_JOB` only to trusted users, see
  [Access Control](./security/access-control.md#governance-and-administrative-privileges).

### Spark Image

The image of the driver and the executors, `spark.image`, needs:

- Spark 3.5, whose version is set as `spark.sparkVersion`.
- The built-in jobs jar of Gravitino at `/opt/gravitino/jobs/gravitino-jobs.jar`, to run the built-in
  job templates. It is the `auxlib/gravitino-jobs-<version>.jar` file of the Gravitino distribution,
  and must come from the same version as the Gravitino server.
- The Iceberg Spark runtime for Spark 3.5 and Scala 2.12, to run the built-in Iceberg jobs. Iceberg
  1.11 needs Java 17, so build the image from a Java 17 Spark image.

For example:

```dockerfile
FROM apache/spark:3.5.9-java17
USER root
COPY gravitino-jobs-*.jar /opt/gravitino/jobs/gravitino-jobs.jar
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.11.0/iceberg-spark-runtime-3.5_2.12-1.11.0.jar /opt/spark/jars/
RUN chmod 644 /opt/gravitino/jobs/gravitino-jobs.jar /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.11.0.jar
USER spark
```

An image without the Iceberg Spark runtime also works if every job gets the runtime through a
server-level default, or through the `spark_conf` of a built-in Iceberg job, see
[Built-In Jobs](#built-in-jobs). The driver of every job then downloads the runtime when it starts.
For example:

```text
gravitino.jobExecutor.k8s.spark.conf.spark.jars = https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.11.0/iceberg-spark-runtime-3.5_2.12-1.11.0.jar
```

### Cluster Connection

The cluster that runs the jobs can be the one Gravitino runs in or another one. The job executor
connects to it in one of these ways, in this order:

| Way             | Configurations                         | Notes                                                                                                                  |
|-----------------|----------------------------------------|------------------------------------------------------------------------------------------------------------------------|
| API server URL  | `masterUrl`, `caCertFile`, `tokenFile` | The token file is read again whenever it changes, so the token can be rotated without restarting Gravitino.            |
| Kubeconfig file | `kubeconfig`, `context`                | Uses the current context of the file if `context` isn't set.                                                           |
| Default lookup  | `context`                              | The `KUBECONFIG` environment variable or `~/.kube/config`, then the service account of the pod that Gravitino runs in. |

The Gravitino server doesn't start if the connection is misconfigured:

- `masterUrl` is set together with `kubeconfig` or `context`, or `caCertFile` or `tokenFile` is set
  without `masterUrl`.
- `masterUrl` doesn't start with `https://` or `http://`.
- `masterUrl` starts with `http://` and `tokenFile` is set, which would send the token in clear
  text, or `caCertFile` is set, which has no effect without TLS.
- A configured file doesn't exist, or the token file has no token.
- The kubeconfig is empty or can't be parsed, has no `context` of that name, or has no current
  context while `context` isn't set, or the context selects no cluster with a server.
- The default lookup finds no kubeconfig, and Gravitino doesn't run in a pod.

These checks don't connect to the API server. An API server that can't be reached, an invalid or
expired token, or a missing permission only shows when a job runs, as the error of the run request,
or when Gravitino pulls the job status, in the server log.

In every case, the jobs run in `namespace` unless their job template sets its own, not in the
namespace of the kubeconfig context or of the pod that Gravitino runs in.

For example, to connect to another cluster with the service account created in [Kubernetes
Permissions](#kubernetes-permissions), issue a token for it in that cluster, and save the CA
certificate of the cluster:

```shell
kubectl create token gravitino-job-executor -n default --duration=24h > /etc/gravitino/k8s/token
kubectl config view --raw --minify -o jsonpath='{.clusters[0].cluster.certificate-authority-data}' | base64 -d > /etc/gravitino/k8s/ca.crt
```

The second command reads the CA certificate from the current context of `kubectl`, which must be
the one of that cluster.

A token issued this way expires, and Gravitino doesn't refresh it. Have whatever provides the file
replace the token before it expires, for example by updating the Secret that the file is mounted
from. Then point the job executor to the API server, its CA certificate, and the token:

```text
gravitino.jobExecutor.k8s.masterUrl = https://k8s.example.com:6443
gravitino.jobExecutor.k8s.caCertFile = /etc/gravitino/k8s/ca.crt
gravitino.jobExecutor.k8s.tokenFile = /etc/gravitino/k8s/token
```

`cluster` names the cluster in the job execution ID that Gravitino stores for each job,
`<cluster>/sparkapp/<namespace>/<name>`. Set the same name on every Gravitino server, and don't
change it while jobs run: the jobs submitted under another name can't be queried or cancelled, and
stay unfinished until the name is changed back.

If the API server can't be reached, each request is tried `1 + requestRetries` times, each waiting
up to `connectionTimeoutInMs`, or `requestTimeoutInMs` for an API server that accepts the request
but doesn't answer. With the defaults, a cancellation fails after about 15 seconds if the API server
can't be reached, or 30 seconds if it doesn't answer. A submission can take twice as long, as the
job executor then also tries to delete the `SparkApplication` in case it was created. After a status
query fails this way, the status queries of the other jobs fail right away for 10 seconds, and
Gravitino tries them again on its next status pull.

### Enable the Job Executor

Set the job executor and its three required configurations in the `gravitino.conf` file, then
restart the Gravitino server:

```text
gravitino.job.executor = k8s
gravitino.jobExecutor.k8s.namespace = <the namespace of the jobs>
gravitino.jobExecutor.k8s.spark.image = <the Spark image>
gravitino.jobExecutor.k8s.spark.sparkVersion = 3.5.9
```

## Configuration

Set the following configurations in the `gravitino.conf` file. An invalid value stops the Gravitino
server from starting.

| Property name                                                | Description                                                                                                                                                                   | Default value                                    | Required |
|--------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------|----------|
| `gravitino.jobExecutor.k8s.cluster`                          | The name of the cluster that runs the jobs, a part of the job execution ID. It can't contain `/`, and must be the same on every Gravitino server                              | `default`                                        | No       |
| `gravitino.jobExecutor.k8s.kubeconfig`                       | The kubeconfig file of the cluster                                                                                                                                            | (none)                                           | No       |
| `gravitino.jobExecutor.k8s.context`                          | The kubeconfig context to use                                                                                                                                                 | The current context                              | No       |
| `gravitino.jobExecutor.k8s.masterUrl`                        | The URL of the API server of the cluster, an alternative to the kubeconfig. It starts with `https://`, or with `http://` without `caCertFile` and `tokenFile`                 | (none)                                           | No       |
| `gravitino.jobExecutor.k8s.caCertFile`                       | The CA certificate file of the API server, used with `masterUrl`                                                                                                              | (none)                                           | No       |
| `gravitino.jobExecutor.k8s.tokenFile`                        | The file of the bearer token to access the API server, used with `masterUrl`. It is read again whenever it changes                                                            | (none)                                           | No       |
| `gravitino.jobExecutor.k8s.namespace`                        | The namespace of the jobs whose job template doesn't set `spark.kubernetes.namespace`                                                                                         | (none)                                           | Yes      |
| `gravitino.jobExecutor.k8s.namePrefix`                       | The prefix of the `SparkApplication` names: at most 24 lowercase letters, digits, and `-`, starting with a letter and ending with a letter or a digit                         | `gravitino-job`                                  | No       |
| `gravitino.jobExecutor.k8s.connectionTimeoutInMs`            | How long in milliseconds to wait to connect to the API server                                                                                                                 | `5000` (5 seconds)                               | No       |
| `gravitino.jobExecutor.k8s.requestTimeoutInMs`               | How long in milliseconds to wait for the response of a request to the API server                                                                                              | `10000` (10 seconds)                             | No       |
| `gravitino.jobExecutor.k8s.requestRetries`                   | How many times a request to the API server is tried again when the server can't be reached, doesn't answer in time, or answers with a server error                            | `2`                                              | No       |
| `gravitino.jobExecutor.k8s.statusCacheTtlInMs`               | How long in milliseconds the `SparkApplication` resources listed in a namespace are cached for status queries. Zero or less disables the cache                                | `5000` (5 seconds)                               | No       |
| `gravitino.jobExecutor.k8s.noStatusTimeoutInMs`              | How long in milliseconds a job waits for the operator to report the first state of its `SparkApplication` before it fails                                                     | `600000` (10 minutes)                            | No       |
| `gravitino.jobExecutor.k8s.spark.image`                      | The image of the Spark driver and executors, unless the job template or a server-level default sets `spark.kubernetes.container.image`                                        | (none)                                           | Yes      |
| `gravitino.jobExecutor.k8s.spark.sparkVersion`               | The Spark version of the image                                                                                                                                                | (none)                                           | Yes      |
| `gravitino.jobExecutor.k8s.spark.serviceAccount`             | The service account of the Spark driver, unless the job template or a server-level default sets `spark.kubernetes.authenticate.driver.serviceAccountName`                     | `spark`                                          | No       |
| `gravitino.jobExecutor.k8s.spark.resourceRetainDurationInMs` | How long in milliseconds the pods of a finished job are retained. The output of a job can be read during this time                                                            | `86400000` (1 day)                               | No       |
| `gravitino.jobExecutor.k8s.spark.ttlAfterStopInMs`           | How long in milliseconds the `SparkApplication` of a job is kept after the job stops                                                                                          | `604800000` (7 days)                             | No       |
| `gravitino.jobExecutor.k8s.spark.driverStartTimeoutInMs`     | How long in milliseconds the operator waits for the driver pod to start before the job fails. It includes the time the driver pod waits for resources or in a scheduler queue | `3600000` (1 hour)                               | No       |
| `gravitino.jobExecutor.k8s.spark.driverReadyTimeoutInMs`     | How long in milliseconds the operator waits for the started driver to become ready before the job fails                                                                       | `3600000` (1 hour)                               | No       |
| `gravitino.jobExecutor.k8s.spark.builtinJobsJar`             | The URI of the built-in jobs jar that the Spark pods can reach, see [Job Resources](#job-resources)                                                                           | `local:///opt/gravitino/jobs/gravitino-jobs.jar` | No       |
| `gravitino.jobExecutor.k8s.spark.conf.<key>`                 | A default Spark configuration `<key>` of every job, see [Job Execution](#job-execution)                                                                                       | (none)                                           | No       |

## Queueing

The job executor creates the `SparkApplication` of a job as soon as the job is submitted. It doesn't
limit the number of unfinished jobs and has no queue of its own, so a burst of jobs can exhaust the
cluster:

- A driver pod that doesn't fit stays pending, and its job stays `QUEUED` until
  `spark.driverStartTimeoutInMs`, then fails.
- A job whose driver runs but whose executors can't be scheduled stays `STARTED` and makes no
  progress.

To queue jobs, run them with a scheduler that has queues, such as
[Apache YuniKorn](https://yunikorn.apache.org/).

### YuniKorn

Send every job to a YuniKorn queue with three server-level Spark configurations. No annotation with
the application ID is needed: YuniKorn groups the driver and the executors of a job into one
application.

```text
gravitino.jobExecutor.k8s.spark.conf.spark.kubernetes.scheduler.name = yunikorn
gravitino.jobExecutor.k8s.spark.conf.spark.kubernetes.driver.label.queue = root.gravitino
gravitino.jobExecutor.k8s.spark.conf.spark.kubernetes.executor.label.queue = root.gravitino
```

A job template can also set these Spark configurations in its `configs`, to run in a queue of its
own.

Limit the queue in the YuniKorn configuration, the `queues.yaml` entry of the ConfigMap
`yunikorn-configs`:

```yaml
partitions:
  - name: default
    queues:
      - name: root
        submitacl: '*'
        queues:
          - name: gravitino
            maxapplications: 10
            resources:
              max:
                vcore: 40
                memory: 160Gi
```

- `maxapplications` limits the jobs that run at the same time. The driver pods of the other jobs
  stay pending and their jobs are `QUEUED`. The next job starts a few seconds after the pods of a
  running job end.
- `resources.max` limits the resources of the pods in the queue, not the number of jobs. A driver
  pod is admitted as soon as it fits, even if its executors don't: such a job is `STARTED` and
  waits for the executors of other jobs to end. Combine it with `maxapplications`, so that the
  admitted jobs can all get their executors.
- Cancelling a queued job removes it from the queue.
- A job that waits in the queue for longer than `spark.driverStartTimeoutInMs` fails. Set this
  timeout longer than the longest time a job may wait.

The job executor doesn't depend on a scheduler: queueing works through the Spark configurations
above. The behaviors above were observed with YuniKorn 1.10.0 installed without its admission
controller (`embedAdmissionController=false`).

## Environment Variables

The `environments` of a job template are set for the driver and the executors as plain Spark
configurations: a variable `REGION` becomes `spark.kubernetes.driverEnv.REGION` and
`spark.executorEnv.REGION`. A name must consist of letters, digits, `_`, `-`, and `.`, can't start
with a digit or `..`, and can't be `.`. Otherwise, the run is rejected. The same rule applies to the
names of `spark.executorEnv.*` in the `configs` of a job template and in the `spark_conf` of a
built-in Iceberg job. Such a `spark.executorEnv.<name>` overrides the variable only in the
executors, as in Spark: `spark_conf` can't set `spark.kubernetes.driverEnv.*`, so the driver keeps
the value from the `environments` of the template.

The variables are set in the driver and executor containers, while the local job executor sets them
on the `spark-submit` process on the Gravitino server. A template written for the local job
executor may set variables to paths on the Gravitino server, such as `SPARK_HOME`,
`SPARK_CONF_DIR`, or `HADOOP_CONF_DIR`. In the pods, such a variable overrides the value of the
image. A `SPARK_HOME` that isn't the Spark home of the image fails the job, as the driver can't
start `$SPARK_HOME/bin/spark-submit`. Any path in `environments` must be a path in the Spark image.

:::caution
The values are readable in the `SparkApplication` and in the driver and executor pods, as well as
in the job template stored in Gravitino and in the `runtimeJobTemplate` of a job returned by the
API. Don't put sensitive values in `environments`.
:::

### Sensitive Values

Pass a sensitive value from a Kubernetes Secret instead. Create the Secret in the namespace of the
job beforehand:

```shell
kubectl create secret generic warehouse-credentials --from-literal=token=<the token>
```

Then reference it in the `configs` of the job template, or in the server-level defaults with the
`gravitino.jobExecutor.k8s.spark.conf.` prefix. Each configuration names an environment variable,
and its value is the Secret and the key in it:

```json
{
  "name": "nightly_rollup",
  "jobType": "spark",
  "executable": "s3a://jobs/rollup.jar",
  "className": "com.example.Rollup",
  "configs": {
    "spark.kubernetes.driver.secretKeyRef.WAREHOUSE_TOKEN": "warehouse-credentials:token",
    "spark.kubernetes.executor.secretKeyRef.WAREHOUSE_TOKEN": "warehouse-credentials:token"
  }
}
```

- Kubernetes resolves the Secret when it starts the pods. Neither Gravitino nor the service account
  of the driver needs permission on Secrets.
- The Secret must exist before the job runs. Otherwise, the driver pod can't start and the job fails
  after `spark.driverStartTimeoutInMs`.
- The Secret isn't deleted with the job.
- A variable set both in `environments` and from a Secret takes the value of the Secret.
- The Secret name can be a placeholder, such as `{{secret}}:token`, to choose the Secret for each
  run.

Like `environments`, the values of the `spark_conf` parameter of a built-in Iceberg job, such as
catalog credentials, are readable in the `sparkConf` and the `driverArgs` of the `SparkApplication`.
Don't put sensitive values there. Pass them as environment variables from a Secret where the job
reads them from its environment, for example the AWS credentials, which the AWS SDK reads from
`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.

## Job Output and Times

The output of a job is the log of its driver pod, read from Kubernetes on every request:

- The standard error is merged into the standard output, so `stderr` is always empty.
- The output is available while the driver pod exists. After that, the output is empty.

The driver pod is deleted, and the output with it:

- `spark.resourceRetainDurationInMs` after the job finishes, 1 day by default, or earlier when the
  `SparkApplication` is deleted after `spark.ttlAfterStopInMs`.
- Right away when the job is cancelled, as cancelling deletes the `SparkApplication` and its pods.
- When the driver pod of a failed job hasn't terminated, for example after the driver start
  timeout. The job executor deletes it so that it can't run the job later.

The start and finish times of a job are the ones Kubernetes reports for the driver container, in
whole seconds. The operator records that a job finished 10 seconds to 2 minutes after the driver
exits, so the status of a job can lag behind by that time, but its times don't.

The actual times are lost in these cases:

- When the driver pod is gone before Gravitino records the finished job, the times are the ones the
  operator recorded, which can be later than the actual ones.
- When the `SparkApplication` is gone before Gravitino records the finished job, for example a
  cancelled job, the finish time is the time Gravitino observes it. The finish time of a cancelled
  job is therefore usually up to `gravitino.job.statusPullIntervalInMs` late.

## Multiple Servers and Restarts

The job executor keeps no job state on the Gravitino server:

- When multiple Gravitino servers share the same metadata store, each of them can query and cancel
  any job, and read its output. Configure all of them with the same cluster and the same `cluster`
  name.
- Jobs keep running while Gravitino is down, and Gravitino picks up their status when it is back.
  The `SparkApplication` of a job is deleted `spark.ttlAfterStopInMs` after the job stops, 7 days by
  default. A job that Gravitino hasn't recorded as finished by then fails.
- Gravitino deployments that share a namespace need different `namePrefix` values, so that their
  `SparkApplication` names don't collide.

## Limitations

- Only Spark templates are supported.
- The standard error of a job is merged into its standard output.
- A job whose driver pod is evicted or deleted fails. It isn't run again.
- A job whose executors can't be scheduled stays `STARTED`: there is no timeout for the executors.
- The operator checks the start timeouts about every 2 minutes, so a job can fail up to 2 minutes
  after `spark.driverStartTimeoutInMs` or `spark.driverReadyTimeoutInMs`.
- The finish time of a cancelled job is usually up to `gravitino.job.statusPullIntervalInMs` late.
- The output of a job is no longer available `spark.resourceRetainDurationInMs` after it finishes,
  and a cancelled job loses its output right away.
- Unfinished jobs fail if Gravitino is down for longer than `spark.ttlAfterStopInMs` after they stop.
- The job executor doesn't limit the number of unfinished jobs, see [Queueing](#queueing).

## Run a Built-In Job on a Local Cluster

The following steps run the built-in job `builtin-iceberg-rewrite-data-files` on the Kubernetes
cluster of [OrbStack](https://orbstack.dev/), which shares the local Docker images. Another local
cluster, such as kind, also needs the Spark image loaded into it. The steps need `kubectl`, `helm`,
`docker`, and a Gravitino distribution, see [How to build](./how-to-build.md).

Point `kubectl` and `helm` at the cluster first:

```shell
kubectl config use-context orbstack
```

### Step 1: Install the Spark Kubernetes Operator

Chart 1.7.0 installs operator 0.9.0. The chart also creates the service account `spark` for the
Spark drivers in the namespace it is installed in.

```shell
helm repo add spark https://apache.github.io/spark-kubernetes-operator
helm repo update
helm install spark spark/spark-kubernetes-operator --version 1.7.0
```

### Step 2: Build the Spark Image

Run the following commands in the directory of the Gravitino distribution. The Dockerfile is the one
in [Spark Image](#spark-image).

```shell
mkdir spark-image
cp auxlib/gravitino-jobs-*.jar spark-image/
cat > spark-image/Dockerfile <<'EOF'
FROM apache/spark:3.5.9-java17
USER root
COPY gravitino-jobs-*.jar /opt/gravitino/jobs/gravitino-jobs.jar
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.11.0/iceberg-spark-runtime-3.5_2.12-1.11.0.jar /opt/spark/jars/
RUN chmod 644 /opt/gravitino/jobs/gravitino-jobs.jar /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.11.0.jar
USER spark
EOF
docker build -t gravitino-spark:3.5.9 spark-image
```

### Step 3: Create an Iceberg Table

The built-in Iceberg jobs work on an existing table, in a catalog and a warehouse that the Spark
pods can reach. This example keeps a Hadoop catalog on a volume, and creates a table with three
data files in it. In production, use the Iceberg catalog and the object store of the tables instead.

Save the following as `iceberg-table.yaml`:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: iceberg-warehouse
spec:
  accessModes: ["ReadWriteOnce"]
  resources:
    requests:
      storage: 1Gi
---
apiVersion: v1
kind: Pod
metadata:
  name: iceberg-table
spec:
  restartPolicy: Never
  containers:
    - name: spark-sql
      image: gravitino-spark:3.5.9
      command: ["/opt/spark/bin/spark-sql"]
      args:
        - --conf
        - spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
        - --conf
        - spark.sql.catalog.demo=org.apache.iceberg.spark.SparkCatalog
        - --conf
        - spark.sql.catalog.demo.type=hadoop
        - --conf
        - spark.sql.catalog.demo.warehouse=file:///warehouse
        - -e
        - |
          CREATE TABLE demo.db.events (id BIGINT, name STRING) USING iceberg;
          INSERT INTO demo.db.events VALUES (1, 'a');
          INSERT INTO demo.db.events VALUES (2, 'b');
          INSERT INTO demo.db.events VALUES (3, 'c');
      volumeMounts:
        - name: warehouse
          mountPath: /warehouse
  volumes:
    - name: warehouse
      persistentVolumeClaim:
        claimName: iceberg-warehouse
```

Create the table and wait for the pod to finish:

```shell
kubectl apply -f iceberg-table.yaml
kubectl wait --for=jsonpath='{.status.phase}'=Succeeded pod/iceberg-table --timeout=180s
```

### Step 4: Configure and Start Gravitino

Append the following to `conf/gravitino.conf`. The four `spark.conf.` lines mount the warehouse
volume into the driver and the executors of every job. The short status pull interval shows the job
progress quickly, and is for this walkthrough only: Gravitino logs a warning about it.

```text
gravitino.job.executor = k8s
gravitino.job.statusPullIntervalInMs = 2000
gravitino.jobExecutor.k8s.context = orbstack
gravitino.jobExecutor.k8s.namespace = default
gravitino.jobExecutor.k8s.spark.image = gravitino-spark:3.5.9
gravitino.jobExecutor.k8s.spark.sparkVersion = 3.5.9
gravitino.jobExecutor.k8s.spark.conf.spark.kubernetes.driver.volumes.persistentVolumeClaim.warehouse.options.claimName = iceberg-warehouse
gravitino.jobExecutor.k8s.spark.conf.spark.kubernetes.driver.volumes.persistentVolumeClaim.warehouse.mount.path = /warehouse
gravitino.jobExecutor.k8s.spark.conf.spark.kubernetes.executor.volumes.persistentVolumeClaim.warehouse.options.claimName = iceberg-warehouse
gravitino.jobExecutor.k8s.spark.conf.spark.kubernetes.executor.volumes.persistentVolumeClaim.warehouse.mount.path = /warehouse
```

Start the server and create a metalake:

```shell
./bin/gravitino.sh start

curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{"name": "demo", "comment": "", "properties": {}}' \
  http://localhost:8090/api/metalakes
```

### Step 5: Run the Job

The Hadoop catalog doesn't use `catalog_uri`, but the job template requires a value for it. The
`min-input-files` option makes the job rewrite the three small files of the table.

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
  "jobTemplateName": "builtin-iceberg-rewrite-data-files",
  "jobConf": {
    "catalog_name": "demo",
    "catalog_type": "hadoop",
    "catalog_uri": "",
    "warehouse_location": "file:///warehouse",
    "table_identifier": "db.events",
    "where_clause": "",
    "options": "{\"min-input-files\":\"2\"}"
  }
}' http://localhost:8090/api/metalakes/demo/jobs/runs
```

The response contains the ID of the job, such as `job-688822913391439376`.

### Step 6: Check the Job and Read Its Output

Get the job with its output:

```shell
curl -H "Accept: application/vnd.gravitino.v1+json" \
  "http://localhost:8090/api/metalakes/demo/jobs/runs/{job_id}?includeOutput=true"
```

The `status` of the job turns from `queued` to `started` and then to `succeeded`, the REST
values of `QUEUED`, `STARTED`, and `SUCCEEDED`. Its `stdout` is
the log of the Spark driver, which contains the result of the job:

```text
Rewrite Data Files Results:
  Rewritten data files: 3
  Added data files: 1
  Rewritten bytes: 1923
  Failed data files: 0
  Removed delete files: 0
Rewrite data files job completed successfully
```

The job is also visible in Kubernetes:

```shell
kubectl get sparkapplications -l app.kubernetes.io/managed-by=gravitino
kubectl get pods -l spark-role=driver
```

### Step 7: Clean Up

```shell
./bin/gravitino.sh stop
kubectl delete sparkapplications -l app.kubernetes.io/managed-by=gravitino
kubectl delete -f iceberg-table.yaml
helm uninstall spark
```
