---
title: "Topics"
slug: "/topics"
keyword: "topic, messaging, Kafka, streaming metadata, Gravitino"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

A topic is a stream of messages, and in Gravitino it is a metadata object like any other. Registering
a Kafka cluster as a messaging catalog puts its topics in the same hierarchy as tables and filesets,
so a stream can be found, classified, and governed alongside the data it feeds.

Gravitino holds the reference rather than the messages. Listing topics asks the cluster at request
time, so a topic created directly in Kafka appears the next time Gravitino is asked, and deleting a
topic through Gravitino deletes it in the cluster.

The value is coverage rather than new capability. Without it, streams sit outside the catalog and
outside whatever classification and access rules apply to everything else.

## Quick Start

**1. Connect a messaging catalog.** A messaging catalog takes the `kafka` provider and the cluster's
bootstrap servers. See [Catalogs and Schemas](./catalogs-and-schemas.md) and
[Apache Kafka catalog](./kafka-catalog.md).

**2. Browse or create topics.** A connected cluster surfaces the topics already there. Creating one
through Gravitino creates it in the cluster.

**3. Classify what matters.** Topics carry tags and policies the same way tables do.

## The Topic Model

### Messaging Systems

Kafka is the only messaging system with a catalog today, connected with the `kafka` provider and the
cluster's `bootstrap.servers`. Anything speaking the Kafka protocol works through the same
connector. Other messaging systems have no catalog.

### The Default Schema

A messaging catalog presents one schema, named `default`, holding every topic in the cluster. Kafka
has no namespace of its own to map onto, so creating or dropping a schema in a messaging catalog is
rejected rather than silently ignored.

The consequence for a large cluster is that topics are not grouped in the catalog the way tables are
grouped by database. Tags are the way to organize them.

### Names and Properties

A topic name is unique within its schema, and matches the topic name in the cluster.

Two properties are settable at creation. `partition-count` sets the number of partitions and can be
changed afterward. `replication-factor` sets the replication and is immutable once the topic exists.
Leaving either unset takes the broker's own default, from `num.partition` and
`default.replication.factor` respectively.

### What Gravitino Stores and What It Does Not

Gravitino stores the topic's place in the hierarchy and anything attached to it, including tags,
policies, and ownership. Message content, offsets, consumer groups, and lag stay entirely in the
cluster.

Message schemas are also outside the catalog. Gravitino does not integrate with a schema registry,
so the structure of the messages in a topic is not described here and cannot be classified per field
the way table columns can. Tags and policies attach to the topic as a whole.

## Working With Topics in the UI

Opening a messaging catalog lists its topics, and selecting one shows its properties.

Topics display the tags and policies they carry, including inherited ones, but cannot be tagged from
the UI today. Attaching a tag to a topic goes through the API.

## Permissions

| Privilege       | Grantable on                        | What it allows        |
|-----------------|-------------------------------------|-----------------------|
| `CREATE_TOPIC`  | Metalake, catalog, schema, or topic | Creating topics       |
| `PRODUCE_TOPIC` | Metalake, catalog, schema, or topic | Writing to a topic    |
| `CONSUME_TOPIC` | Metalake, catalog, schema, or topic | Reading from a topic  |

Granting at a wider scope covers everything beneath it. Dropping a topic is reserved for the metalake
owner and the object owner.

## Using the API

Topics can be created, listed, altered, and dropped over REST and through the Java client. The
Python client does not cover topics. Endpoints, payload shapes, and worked examples are in
[Manage Messaging Metadata](./manage-messaging-metadata-using-gravitino.md).
