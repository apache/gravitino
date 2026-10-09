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

The Spark configurations of a job come from three places, each overriding the ones before it:

1. The server-level defaults, set with the `gravitino.jobExecutor.k8s.spark.conf.` prefix.
2. The `environments` of the job template.
3. The `configs` of the job template.

The job executor then adjusts the result:

- `spark.master` and `spark.submit.deployMode` are removed, as the operator decides them.
- `spark.kubernetes.container.image`, `spark.kubernetes.authenticate.driver.serviceAccountName`, and
  `spark.kubernetes.namespace` take the values of `spark.image`, `spark.serviceAccount`, and
  `namespace` of the job executor if the job doesn't set them. A job template can therefore run
  with its own image and service account, and in its own namespace.

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
`https://`, `s3a://`, or `hdfs://`. A run with a path on the Gravitino server, that is a path without
a scheme or with the `file` scheme, is rejected.

The built-in job templates are the exception. Their `executable` is the path of the built-in jobs
jar on the Gravitino server, and the job executor replaces it with `spark.builtinJobsJar`, by default
`local:///opt/gravitino/jobs/gravitino-jobs.jar`, a file in the [Spark image](#spark-image).

### Job Status

The job status follows the state of the `SparkApplication`:

| Job status  | SparkApplication                                                                                                                                                |
|-------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `QUEUED`    | No state yet, `Submitted`, `DriverRequested`, `DriverStarted`, or `ScheduledToRestart`. The driver pod isn't ready, for example because it waits for resources. |
| `STARTED`   | `DriverReady`, `RunningHealthy`, `RunningWithPartialCapacity`, `InitializedBelowThresholdExecutors`, or `RunningWithBelowThresholdExecutors`                    |
| `SUCCEEDED` | `Succeeded`                                                                                                                                                     |
| `FAILED`    | `Failed`, `SchedulingFailure`, `DriverEvicted`, `DriverStartTimedOut`, `DriverReadyTimedOut`, or `ExecutorsStartTimedOut`                                       |
| `CANCELLED` | Cancelled through Gravitino                                                                                                                                     |

A job also fails in these cases:

- The operator reports no state for the `SparkApplication` within `noStatusTimeoutMs`, for example
  because the operator isn't running or doesn't watch the namespace. The job executor deletes the
  `SparkApplication` before failing the job.
- The `SparkApplication` is deleted by someone else before the job finishes.

A job is one run of the application: the operator never restarts it.

## Setup

### Kubernetes Permissions

The job executor needs these permissions in every namespace that jobs run in. It needs no
permission on Secrets or ConfigMaps, and no cluster-wide permission.

| Resource                                 | Verb     | Used to                                                                                                                             |
|------------------------------------------|----------|-------------------------------------------------------------------------------------------------------------------------------------|
| `sparkapplications` (`spark.apache.org`) | `create` | Submit a job                                                                                                                        |
| `sparkapplications` (`spark.apache.org`) | `get`    | Query and cancel a job                                                                                                              |
| `sparkapplications` (`spark.apache.org`) | `list`   | Query all the jobs of a namespace with one call. Optional: without it, the job executor gets the jobs one by one and logs a warning |
| `sparkapplications` (`spark.apache.org`) | `patch`  | Mark a job as cancelled by Gravitino                                                                                                |
| `sparkapplications` (`spark.apache.org`) | `delete` | Cancel a job                                                                                                                        |
| `pods`                                   | `list`   | Find the driver pod, for the output and the times of a job                                                                          |
| `pods`                                   | `delete` | Delete the driver pod of a failed job that hasn't terminated, for example after the driver start timeout                            |
| `pods/log`                               | `get`    | Read the output of a job                                                                                                            |

For example, in the namespace `default`:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: gravitino-job-executor
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: gravitino-job-executor
rules:
  - apiGroups: ["spark.apache.org"]
    resources: ["sparkapplications"]
    verbs: ["create", "get", "list", "patch", "delete"]
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
server-level default. The driver of every job then downloads the runtime when it starts. For
example:

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

`masterUrl` can't be set together with `kubeconfig` or `context`, and `caCertFile` and `tokenFile`
can only be set together with `masterUrl`. A `context` that doesn't exist in the kubeconfig is
rejected.

For example, to connect to another cluster with the service account created in
[Kubernetes Permissions](#kubernetes-permissions), issue a token for it in that cluster:

```shell
kubectl create token gravitino-job-executor --duration=24h > /etc/gravitino/k8s/token
```

Then point the job executor to the API server, its CA certificate, and the token:

```text
gravitino.jobExecutor.k8s.masterUrl = https://k8s.example.com:6443
gravitino.jobExecutor.k8s.caCertFile = /etc/gravitino/k8s/ca.crt
gravitino.jobExecutor.k8s.tokenFile = /etc/gravitino/k8s/token
```

`cluster` names the cluster in the job execution ID that Gravitino stores for each job,
`<cluster>/sparkapp/<namespace>/<name>`. Changing it makes the job executor lose the unfinished jobs
submitted under the previous name, which then fail.

### Enable the Job Executor

Set the job executor and its two required configurations in the `gravitino.conf` file, then restart
the Gravitino server:

```text
gravitino.job.executor = k8s
gravitino.jobExecutor.k8s.spark.image = <the Spark image>
gravitino.jobExecutor.k8s.spark.sparkVersion = 3.5.9
```

## Configuration

Set the following configurations in the `gravitino.conf` file. An invalid value stops the Gravitino
server from starting.

| Property name                                              | Description                                                                                                                                                                   | Default value                                    | Required |
|------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------|----------|
| `gravitino.jobExecutor.k8s.cluster`                        | The name of the cluster that runs the jobs, a part of the job execution ID. It can't contain `/`                                                                              | `default`                                        | No       |
| `gravitino.jobExecutor.k8s.kubeconfig`                     | The kubeconfig file of the cluster                                                                                                                                            | (none)                                           | No       |
| `gravitino.jobExecutor.k8s.context`                        | The kubeconfig context to use                                                                                                                                                 | The current context                              | No       |
| `gravitino.jobExecutor.k8s.masterUrl`                      | The URL of the API server of the cluster, an alternative to the kubeconfig                                                                                                    | (none)                                           | No       |
| `gravitino.jobExecutor.k8s.caCertFile`                     | The CA certificate file of the API server, used with `masterUrl`                                                                                                              | (none)                                           | No       |
| `gravitino.jobExecutor.k8s.tokenFile`                      | The file of the bearer token to access the API server, used with `masterUrl`                                                                                                  | (none)                                           | No       |
| `gravitino.jobExecutor.k8s.namespace`                      | The namespace of the jobs that don't set `spark.kubernetes.namespace`                                                                                                         | `default`                                        | No       |
| `gravitino.jobExecutor.k8s.namePrefix`                     | The prefix of the `SparkApplication` names: at most 24 lowercase letters, digits, and `-`, starting with a letter and ending with a letter or a digit                         | `gravitino-job`                                  | No       |
| `gravitino.jobExecutor.k8s.statusCacheTtlMs`               | How long in milliseconds the `SparkApplication` resources listed in a namespace are cached for status queries. Zero or less disables the cache                                | `5000` (5 seconds)                               | No       |
| `gravitino.jobExecutor.k8s.noStatusTimeoutMs`              | How long in milliseconds a job waits for the operator to report the first state of its `SparkApplication` before it fails                                                     | `600000` (10 minutes)                            | No       |
| `gravitino.jobExecutor.k8s.spark.image`                    | The image of the Spark driver and executors, unless a job sets `spark.kubernetes.container.image`                                                                             | (none)                                           | Yes      |
| `gravitino.jobExecutor.k8s.spark.sparkVersion`             | The Spark version of the image                                                                                                                                                | (none)                                           | Yes      |
| `gravitino.jobExecutor.k8s.spark.serviceAccount`           | The service account of the Spark driver, unless a job sets `spark.kubernetes.authenticate.driver.serviceAccountName`                                                          | `spark`                                          | No       |
| `gravitino.jobExecutor.k8s.spark.resourceRetainDurationMs` | How long in milliseconds the pods of a finished job are retained. The output of a job can be read during this time                                                            | `86400000` (1 day)                               | No       |
| `gravitino.jobExecutor.k8s.spark.ttlAfterStopMs`           | How long in milliseconds the `SparkApplication` of a job is kept after the job stops                                                                                          | `604800000` (7 days)                             | No       |
| `gravitino.jobExecutor.k8s.spark.driverStartTimeoutMs`     | How long in milliseconds the operator waits for the driver pod to start before the job fails. It includes the time the driver pod waits for resources or in a scheduler queue | `3600000` (1 hour)                               | No       |
| `gravitino.jobExecutor.k8s.spark.driverReadyTimeoutMs`     | How long in milliseconds the operator waits for the started driver to become ready before the job fails                                                                       | `3600000` (1 hour)                               | No       |
| `gravitino.jobExecutor.k8s.spark.builtinJobsJar`           | The URI of the built-in jobs jar that the Spark pods can reach, see [Job Resources](#job-resources)                                                                           | `local:///opt/gravitino/jobs/gravitino-jobs.jar` | No       |
| `gravitino.jobExecutor.k8s.spark.conf.<key>`               | A default Spark configuration `<key>` of every job, see [Job Execution](#job-execution)                                                                                       | (none)                                           | No       |

## Queueing

The job executor creates the `SparkApplication` of a job as soon as the job is submitted. It doesn't
limit the number of unfinished jobs and has no queue of its own, so a burst of jobs can exhaust the
cluster:

- A driver pod that doesn't fit stays pending, and its job stays `QUEUED` until
  `spark.driverStartTimeoutMs`, then fails.
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
- A job that waits in the queue for longer than `spark.driverStartTimeoutMs` fails. Set this
  timeout longer than the longest time a job may wait.

These behaviors were verified with YuniKorn 1.10.0 installed without its admission controller
(`embedAdmissionController=false`). Gang scheduling and other schedulers, such as Volcano and Kueue,
aren't verified with the job executor.

## Environment Variables

The `environments` of a job template are set for the driver and the executors as plain Spark
configurations: a variable `REGION` becomes `spark.kubernetes.driverEnv.REGION` and
`spark.executorEnv.REGION`. A name must consist of letters, digits, `_`, `-`, and `.`, and can't
start with a digit. Otherwise, the run is rejected.

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
  after `spark.driverStartTimeoutMs`.
- The Secret isn't deleted with the job.
- A variable set both in `environments` and from a Secret takes the value of the Secret.
- The Secret name can be a placeholder, such as `{{secret}}:token`, to choose the Secret for each
  run.

## Job Output and Times

The output of a job is the log of its driver pod, read from Kubernetes on every request:

- The standard error is merged into the standard output, so `stderr` is always empty.
- The output is available while the driver pod exists, which is `spark.resourceRetainDurationMs`
  after the job finishes, one day by default. After that, the output is empty.

The start and finish times of a job are the ones Kubernetes reports for the driver container, in
whole seconds. The operator records that a job finished 10 seconds to 2 minutes after the driver
exits, so the status of a job can lag behind by that time, but its times don't.

The finish time of a cancelled job is the time Gravitino observes the cancellation, which can be up
to `gravitino.job.statusPullIntervalInMs` late.

## Multiple Servers and Restarts

The job executor keeps no job state on the Gravitino server:

- When multiple Gravitino servers share the same metadata store, each of them can query and cancel
  any job, and read its output. Configure all of them with the same cluster and the same `cluster`
  name.
- Jobs keep running while Gravitino is down, and Gravitino picks up their status when it is back.
  The `SparkApplication` of a job is deleted `spark.ttlAfterStopMs` after the job stops, seven days
  by default. A job that Gravitino hasn't recorded as finished by then fails.
- Gravitino deployments that share a namespace need different `namePrefix` values, so that their
  `SparkApplication` names don't collide.

## Limitations

- Only Spark templates are supported.
- The standard error of a job is merged into its standard output.
- A job whose driver pod is evicted or deleted fails. It isn't run again.
- A job whose executors can't be scheduled stays `STARTED`: there is no timeout for the executors.
- The operator checks the start timeouts about every 2 minutes, so a job can fail up to 2 minutes
  after `spark.driverStartTimeoutMs` or `spark.driverReadyTimeoutMs`.
- The finish time of a cancelled job can be up to `gravitino.job.statusPullIntervalInMs` late.
- The output of a job is no longer available `spark.resourceRetainDurationMs` after it finishes.
- Unfinished jobs fail if Gravitino is down for longer than `spark.ttlAfterStopMs` after they stop.
- The job executor doesn't limit the number of unfinished jobs, see [Queueing](#queueing).
- The `spark_conf` parameter of the built-in jobs is applied by the driver after it starts. Setting
  `spark.jars` there has no effect, and setting `spark.master` there runs the job in local mode
  inside the driver pod. Set such configurations as server-level defaults instead.

## Run a Built-In Job on a Local Cluster

The following steps run the built-in job `builtin-iceberg-rewrite-data-files` on the Kubernetes
cluster of [OrbStack](https://orbstack.dev/), which shares the local Docker images. Another local
cluster, such as kind, also needs the Spark image loaded into it. The steps need `kubectl`, `helm`,
`docker`, and a Gravitino distribution, see [How to build](./how-to-build.md).

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
volume into the driver and the executors of every job.

```text
gravitino.job.executor = k8s
gravitino.job.statusPullIntervalInMs = 2000
gravitino.jobExecutor.k8s.context = orbstack
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

The `status` of the job turns from `queued` to `started` and then to `succeeded`. Its `stdout` is
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
