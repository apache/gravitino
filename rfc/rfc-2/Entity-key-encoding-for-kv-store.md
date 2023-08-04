<!--
  Copyright 2023 Datastrato.
  This software is licensed under the Apache License version 2.
-->

# RFC-1: Enity Key Encoding design for KV store

| Revision | Owner |
| :------- |-------|
| v0.1     | Qi Yu |

## Background
Currently, there will be many data to storage, for example
User information. Such as username, passward, user property, this data is structure data
Metadata. Metadata is the key point of our product. Metadata is Heterogeneous and data may be very large
Other information, such as query history, query log and so on

To store this information, there will be 3 kinds of database to store, that is
Relational database
KV Store
GraphDB
Others like MongoDB, ES

After we refer to snowflake schema and others (Hashdata and so on),  KV storage is better than other kinds of storage compared to relational databases.

According to our previous design, Matadata in graviton will be originzed as the following structure
![mc](../rfc-1/schema-overview.png)

According to the picture above, Metadata in graviton can be divided into multiple layers which is a little bit like a file directory structure. To implement this hierarchy, we can choose the following options to encode keys.

## Design

### Target

We should design a key encoding method to encode the key of KV store that should satisfy the following requirements:
- Support fast point query 
- Support efficient range query
- Not very complicated 
- Good expandability and compatibility


### Design 

Firstly, we introduce a global auto-increment ID that reprents the name of namespace. This ID is unique in the whole system. 
For example, if there exists a catalog named `catalog1` with namepace name `metalake1`, we will add the following key-value pair to KV store

| Key       | Value     | Description                                                    | 
|:----------|-----------|----------------------------------------------------------------|
| metalake1 | 1         | name to id mapping                                             |
| catalog2  | 2         | name to id mapping                                             |
| 1         | metalake1 | id to name mapping                                             |
| 2         | catalake2 | id to name mapping                                             |
 | current_max_id | 2 | Storage current max id, next avaiable id is current max id + 1 |

Why we introduce this global auto-increment ID? Because we want to support the following features:
- If we want to rename a namespace, we can just update the name to id mapping
- If the name is too long, we can use a short name to represent it when encoding key of entity (see below)

Then, The whole key of entity can be encoded as the following format


| Key                                               | Value         | Description                     | 
|:--------------------------------------------------|---------------|---------------------------------|
| ml_{ml_id}                                        | matalake info | ml is a short name for metalake |
| ml_{ml_id}                                        | matalake info | ml is a short name for metalake |
| ca_{ml_id}_{ca_id}                                | catalog_info  | ca is a short name for catalog  |
| ca_{ml_id}_{ca_id}                                | catalog_info  | ca is a short name for catalog  |
| sc_{ml_id} _ {ca_id}_{sc_id}                      | schema_info   | sc is a short name for schema   |
| sc_{ml_id} _ {ca_id}_{sc_id}                      | schema_info   | sc is a short name for schema   |
| br_{ml_id} _ {ca_id}_{br_id}                      | broker_info   | br is a short name for broker   |
| br_{ml_id} _ {ca_id}_{br_id}                      | broker_info   | br is a short name for broker   |
| ta_{ml_id} _ {catalog_id}_ {schema_id}_{table_id} | table_info    | ta is a short name for table    |
| to_{ml_id} _ {catalog_id} _ {br_id}_{topic_id}    | topic_info    | to is a short name for topic    |

## Implementation

Please see code ```CustomKeyEncoder```


