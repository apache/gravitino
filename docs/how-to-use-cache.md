---
title: How to use cache in Gravitino
slug: /how-to-use-cache
date: 2025-06-24
license: "This software is licensed under the Apache License version 2."
---

# Apache Gravitino Cache System

Apache Gravitino provides a cache system based on Caffeine and other pluggable implementations. This system is designed to boost metadata access performance, reduce backend data source pressure, and maintain consistency between cached data and the underlying store.

## Background

The primary motivations for enabling metadata caching are as follows:

1. When loading or creating a Table, Model, or Fileset, frequent access to metadata IDs such as `metalake_id`, `catalog_id`, and `schema_id` can result in significant performance overhead.
2. Casbin requires ID-based permission checks during authorization, which involves frequent retrieval of Entity IDs.
3. Loading Role information often involves a large number of metadata objects, putting additional pressure on the database.
4. Introducing a caching mechanism can significantly reduce database access frequency and improve overall system performance.

## Enabling the Cache

To enable metadata caching, modify the following settings in the `${GRAVITINO_HOME}/conf/gravitino.conf` file:

```
# Whether to enable the cache
gravitino.cache.enabled=true
# Specify the cache implementation (no need to use the fully qualified class name)
gravitino.cache.implementation=caffeine
```

## Supported Metadata Types for Caching

The following metadata types are currently supported for caching:

- Metalake
- Catalog
- Schema
- Table
- Model
- Fileset
- Topic
- Tag
- User
- Group
- Role
- ModelVersion

## Configuration Options

| Configuration Key                | Description                                | Default Value          |
|----------------------------------|--------------------------------------------|------------------------|
| `gravitino.cache.enabled`        | Whether to enable caching                  | `true`                 |
| `gravitino.cache.implementation` | Specifies the cache implementation         | `caffeine`             |
| `gravitino.cache.maxEntries`     | Maximum number of entries allowed in cache | `10000`                |
| `gravitino.cache.expireTimeInMs` | Cache expiration time (in milliseconds)    | `3600000` (about 1 hr) |
| `gravitino.cache.enableStats`    | Whether to enable cache statistics logging | `false`                |
| `gravitino.cache.enableWeigher`  | Whether to enable weight-based eviction    | `true`                 |

- `gravitino.cache.enableWeigher`: When enabled, eviction is based on weight and `maxEntries` will be ignored.
- `gravitino.cache.expireTimeInMs`: Controls the cache TTL in milliseconds.
- If `gravitino.cache.enableStats` is enabled, Gravitino will log cache statistics (hit count, miss count, load failures, etc.) every 5 minutes at the Info level.

## Eviction Strategies

Gravitino supports multiple eviction strategies including capacity-based, weight-based, and time-based (TTL) eviction. The following describes how they work with Caffeine:

### Capacity-Based Eviction

When `gravitino.cache.enableWeigher` is **disabled**, Gravitino limits the number of cached entries using `gravitino.cache.maxEntries` and employs Caffeine’s W-TinyLFU eviction policy to remove the least-used entries when the cache is full.

### Weight-Based Eviction

When `gravitino.cache.enableWeigher` is **enabled**, Gravitino uses a combination of `maximumWeight` and a custom weigher to control the total weight of the cache:

- Each entity type has a default weight (e.g., Metalake > Catalog > Schema);
- Entries are evicted based on the combined weight limit (`maximumWeight`);
- If a single cache item exceeds the total weight limit, it will not be cached;
- Weight calculation logic is implemented via `MetadataEntityWeigher`;
- When this strategy is active, `maxEntries` will be ignored.

### Time-Based Eviction (TTL)

All cache entries are subject to a TTL (Time-To-Live) expiration policy. By default, the TTL is `3600000ms` (1 hour) and can be adjusted via the `gravitino.cache.expireTimeInMs` setting:

- TTL starts at the time of entry creation; once it exceeds the configured duration, the entry expires automatically;
- TTL can work in conjunction with both capacity and weight-based eviction;
- Expired entries will also trigger asynchronous cleanup mechanisms for resource release and logging.