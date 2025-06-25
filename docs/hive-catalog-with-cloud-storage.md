---
title: "Hive catalog with S3, ADLS and GCS"
slug: /hive-catalog
date: 2024-9-24
keyword: Hive catalog cloud storage S3 ADLS GCS
license: "This software is licensed under the Apache License version 2."
---


## Introduction

Since Hive 2.x, Hive has supported S3 as a storage backend, enabling users to store and manage data in Amazon S3 directly through Hive. Gravitino enhances this capability by supporting the Hive catalog with S3, allowing users to efficiently manage the storage locations of files located in S3. This integration simplifies data operations and enables seamless access to S3 data from Hive queries.

For ADLS (aka. Azure Blob Storage (ABS), or Azure Data Lake Storage (v2)) and GCS (Google Cloud Storage), the integration is similar to S3. The only difference is the configuration properties for ADLS and GCS (see below). 

The following sections will guide you through the necessary steps to configure the Hive catalog to utilize S3, ADLS, and GCS as a storage backend, including configuration details and examples for creating databases and tables.

## Hive metastore configuration

The following will mainly focus on configuring the Hive metastore to use S3 as a storage backend. The same configuration can be applied to ADLS and GCS with minor changes in the configuration properties. 

### Example Configuration Changes

Below are the essential properties to add or modify in the `hive-site.xml` file to support S3:

```xml

<property>
  <name>fs.s3a.access.key</name>
  <value>S3_ACCESS_KEY_ID</value>
</property>

<property>
  <name>fs.s3a.secret.key</name>
  <value>S3_SECRET_KEY_ID</value>
</property>

<property>
  <name>fs.s3a.endpoint</name>
  <value>S3_ENDPOINT_ID</value>
</property>

<!-- The following property is optional and can be replaced with the location property in the schema
definition and table definition, as shown in the examples below. After explicitly setting this
property, you can omit the location property in the schema and table definitions.

It's also applicable for Azure Blob Storage(ADLS) and GCS.
-->
<property>
  <name>hive.metastore.warehouse.dir</name>
  <value>S3_BUCKET_PATH</value>
</property>

<!-- The following two configurations are for Azure Blob Storage(ADLS) -->
<property>
  <name>fs.abfss.impl</name>
  <value>org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem</value>
</property>

<property>
  <name>fs.azure.account.key.ABS_ACCOUNT_NAME.dfs.core.windows.net</name>
  <value>ABS_ACCOUNT_KEY</value>
</property>

<!-- The following two configurations are only for Google Cloud Storage(gcs) -->
<property>
  <name>fs.gs.auth.service.account.enable</name>
  <value>true</value>
</property>

<!-- SERVICE_ACCOUNT_FILE should be a local file or remote file that can be access by hive server -->
<property>
  <name>fs.gs.auth.service.account.json.keyfile</name>
  <value>SERVICE_ACCOUNT_FILE</value>
</property>

```

### Adding Required JARs

After updating the `hive-site.xml`, you need to ensure that the necessary S3-related JARs are included in the Hive classpath. You can do this by executing the following command:
```shell
cp ${HADOOP_HOME}/share/hadoop/tools/lib/*aws* ${HIVE_HOME}/lib

# For Azure Blob Storage(ADLS)
cp ${HADOOP_HOME}/share/hadoop/tools/lib/*azure* ${HIVE_HOME}/lib

# For Google Cloud Storage(GCS)
cp gcs-connector-hadoop3-2.2.22-shaded.jar ${HIVE_HOME}/lib
```

[`gcs-connector-hadoop3-2.2.22-shaded.jar`](https://github.com/GoogleCloudDataproc/hadoop-connectors/releases/download/v2.2.22/gcs-connector-hadoop2-2.2.22-shaded.jar) is the bundle jar that contains Hadoop GCS connector, you need to choose the corresponding gcs connector jar for the version of Hadoop you are using.

Alternatively, you can download the required JARs from the Maven repository and place them in the Hive classpath. It is crucial to verify that the JARs are compatible with the version of Hadoop you are using to avoid any compatibility issue.

### Restart Hive metastore

Once all configurations have been correctly set, restart the Hive cluster to apply the changes. This step is essential to ensure that the new configurations take effect and that the Hive services can communicate with S3.


## Creating Tables or Databases with S3 Storage using Gravitino

Assuming you have already set up a Hive catalog with Gravitino, you can proceed to create tables or databases using S3 storage. For more information on catalog operations, refer to [Catalog operations](./manage-fileset-metadata-using-gravitino.md#catalog-operations)

### Example: Creating a Database with S3 Storage

The following is an example of how to create a database in S3 using Gravitino:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "hive_schema",
  "comment": "comment",
  "properties": {
    "location": "s3a://bucket-name/path"
     
     # The following line is for Azure Blob Storage(ADLS)
     # "location": "abfss://container-name@user-account-name.dfs.core.windows.net/path"
     
     # The following line is for Google Cloud Storage(GCS)
     # "location": "gs://bucket-name/path"
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs/catalog/schemas
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoClient gravitinoClient = GravitinoClient
    .builder("http://localhost:8090")
    .withMetalake("metalake")
    .build();

// Assuming you have just created a Hive catalog named `catalog`
Catalog catalog = gravitinoClient.loadCatalog("catalog");

SupportsSchemas supportsSchemas = catalog.asSchemas();

Map<String, String> schemaProperties = ImmutableMap.<String, String>builder()
    .put("location", "s3a://bucket-name/path")
    
    // The following line is for Azure Blob Storage(ADLS)
    // .put("location", "abfss://container-name@user-account-name.dfs.core.windows.net/path")
    
    // The following lines for Google Cloud Storage(GCS)
    // .put("location", "gs://bucket-name/path")
    
    .build();
Schema schema = supportsSchemas.createSchema("hive_schema",
    "This is a schema",
    schemaProperties
);
// ...
```

</TabItem>
</Tabs>

After creating the database, you can proceed to create tables under this schema using S3 storage. For further details on table operations, please refer to [Table operations](./manage-relational-metadata-using-gravitino.md#table-operations).

## Access tables with S3 storage by Hive CLI

Assuming you have already created a table in the section [Creating Tables or Databases with S3 Storage using Gravitino](#creating-tables-or-databases-with-s3-storage-using-gravitino), let’s say the table is named `hive_table`. You can access the database/table and view its details using the Hive CLI as follows:


```shell
hive> show create database hive_schema;
OK
CREATE DATABASE `hive_schema`
COMMENT
  'comment'
LOCATION
  's3a://my-test-bucket/test-1727168792125'
WITH DBPROPERTIES (
  'gravitino.identifier'='gravitino.v1.uid2173913050348296645',
  'key1'='val1',
  'key2'='val2')
Time taken: 0.019 seconds, Fetched: 9 row(s)
hive> use hive_schema;
OK
Time taken: 0.019 seconds
hive> show create table cataloghiveit_table_fc7c7d16;
OK
CREATE TABLE `hive_table`(
  `hive_col_name1` tinyint COMMENT 'col_1_comment',
  `hive_col_name2` date COMMENT 'col_2_comment',
  `hive_col_name3` string COMMENT 'col_3_comment')
COMMENT 'table_comment'
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3a://my-test-bucket/test-1727168821335/hive_table'
TBLPROPERTIES (
  'EXTERNAL'='FALSE',
  'gravitino.identifier'='gravitino.v1.uid292928775813252841',
  'key1'='val1',
  'key2'='val2',
  'transient_lastDdlTime'='1727168821')
Time taken: 0.071 seconds, Fetched: 19 row(s)
> insert into hive_table values(1, '2022-11-12', 'hello');
Query ID = root_20240924091305_58ab83c7-7091-4cc7-a0d9-fa44945f45c6
Total jobs = 3
Launching Job 1 out of 3
Number of reduce tasks is set to 0 since there's no reduce operator
Job running in-process (local Hadoop)
2024-09-24 09:13:08,381 Stage-1 map = 100%,  reduce = 0%
Ended Job = job_local1096072998_0001
Stage-4 is selected by condition resolver.
Stage-3 is filtered out by condition resolver.
Stage-5 is filtered out by condition resolver.
Loading data to table hive_schema.hive_table
MapReduce Jobs Launched:
Stage-Stage-1:  HDFS Read: 0 HDFS Write: 0 SUCCESS
Total MapReduce CPU Time Spent: 0 msec
OK
Time taken: 2.843 seconds
hive> select * from hive_table;
OK
1	2022-11-12	hello
Time taken: 0.116 seconds, Fetched: 1 row(s)
```

This command shows the creation details of the database hive_schema, including its location in S3 and any associated properties.

## Accessing Tables with S3 Storage via Spark

To access S3-stored tables using Spark, you need to configure the SparkSession appropriately. Below is an example of how to set up the SparkSession with the necessary S3 configurations:

```java
  SparkSession sparkSession =
        SparkSession.builder()
            .config("spark.plugins", "org.apache.gravitino.spark.connector.plugin.GravitinoSparkPlugin")
            .config("spark.sql.gravitino.uri", "http://localhost:8090")
            .config("spark.sql.gravitino.metalake", "xx")
            .config("spark.sql.catalog.{hive_catalog_name}.fs.s3a.access.key", accessKey)
            .config("spark.sql.catalog.{hive_catalog_name}.fs.s3a.secret.key", secretKey)
            .config("spark.sql.catalog.{hive_catalog_name}.fs.s3a.endpoint", getS3Endpoint)
            .config("spark.sql.catalog.{hive_catalog_name}.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

            // This two is for Azure Blob Storage(ADLS) only
            .config(
                String.format(
                    "spark.sql.catalog.{hive_catalog_name}.fs.azure.account.key.%s.dfs.core.windows.net",
                    ABS_USER_ACCOUNT_NAME),
                ABS_USER_ACCOUNT_KEY)
            .config("spark.sql.catalog.{hive_catalog_name}.fs.abfss.impl", "org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem")
  
            // This two is for Google Cloud Storage(GCS) only
            .config("spark.sql.catalog.{hive_catalog_name}.fs.gs.auth.service.account.enable", "true")
            .config("spark.sql.catalog.{hive_catalog_name}.fs.gs.auth.service.account.json.keyfile", "SERVICE_ACCOUNT_FILE")
            
            .config("spark.sql.catalog.{hive_catalog_name}.fs.s3a.path.style.access", "true")
            .config("spark.sql.catalog.{hive_catalog_name}.fs.s3a.connection.ssl.enabled", "false")
            .config(
                "spark.sql.catalog.{hive_catalog_name}.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            .config("spark.sql.storeAssignmentPolicy", "LEGACY")
            .config("mapreduce.input.fileinputformat.input.dir.recursive", "true")
            .enableHiveSupport()
            .getOrCreate();

    sparkSession.sql("...");
```

:::note
Please download [Hadoop AWS jar](https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws), [aws java sdk jar](https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk-bundle) and place them in the classpath of the Spark. If the JARs are missing, Spark will not be able to access the S3 storage.
Azure Blob Storage(ADLS) requires the [Hadoop Azure jar](https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-azure), [Azure cloud sdk jar](https://mvnrepository.com/artifact/com.azure/azure-storage-blob) to be placed in the classpath of the Spark.
for Google Cloud Storage(GCS), you need to download the [Hadoop GCS jar](https://github.com/GoogleCloudDataproc/hadoop-connectors/releases) and place it in the classpath of the Spark.
:::

By following these instructions, you can effectively manage and access your S3, ADLS or GCS data through both Hive CLI and Spark, leveraging the capabilities of Gravitino for optimal data management.
