---
title: "Gravitino event listener"
slug: /event-listener
date: 2024-04-18
keyword: event listener
license: Copyright 2024 Datastrato Pvt Ltd. This software is licensed under the Apache License version 2.
---

## Introduction

Gravitino provides event listener mechanism to allow use to capture the event provides by Gravitino server to inject some custom operations.

## Event 

Gravitino generates an event following the completion of CREATE, DROP, PURGE, ALTER, UPDATE, LOAD, LIST operations. The events can be hooked to the following resources:

- table
- fileset
- topic
- schema
- catalog
- metalake

However, events are not hooked to the following resources for now:

- partition
- role
- user
- group

## How to use

To leverage the event listener, you must implement the `EventListenerPlugin` interface and place the resulting JAR in the classpath of the Gravitino server. Then, add configurations to gravitino.conf to enable the event listener.

| Property name                              | Description                                                                                            | Default value | Required | Since Version |
|--------------------------------------------|--------------------------------------------------------------------------------------------------------|---------------|----------|---------------|
| `gravitino.eventListener.names`            | The name of the event listener, For multiple listeners, separate names with a comma, like "audit,sync" | (none)        | Yes      | 0.5.0         |
| `gravitino.eventListener.{name}.className` | The class name of the event listener, replace `{name}` with the actual listener name.                  | (none)        | Yes      | 0.5.0         | 
| `gravitino.eventListener.{name}.{key}`     | Custom properties that will be passed to the event listener plugin.                                    | (none)        | Yes      | 0.5.0         | 

## Event listener plugin

The `EventListenerPlugin` defines an interface for event listeners that manage the lifecycle and state of a plugin. This includes handling its initialization, startup, and shutdown processes, as well as handing events triggered by various operations.

The plugin provides several operational modes for how to process event, supporting both synchronous and asynchronous processing approaches. 

- **SYNC**: Events are processed synchronously, immediately after the related operation is completed. While this method ensures timely event handling, it may block the main process if significant time is required to process an event.

- **ASYNC_ISOLATED**: Events are processed asynchronously, with each listener having its own unique event-processing queue and dispatcher thread. 

- **ASYNC_SHARED**: In this mode, event listeners utilize a shared event-processing queue and dispatcher to process events asynchronously. 

For more details, please refer to the definition of the plugin.

