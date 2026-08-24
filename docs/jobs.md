---
title: "Jobs"
slug: "/jobs"
keyword: "job, job template, Spark job, shell job, Gravitino"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

A job is a piece of work Gravitino runs on your behalf, and a job template is the reusable
definition it runs from. Registering a template once and running it many times is the shape, so the
command, its arguments, and its environment live in the catalog rather than in whoever launched it
last.

Jobs are how catalog-driven work gets done. Table maintenance uses them, and anything else that
should run against metadata Gravitino already knows about can use the same mechanism.

## Quick Start

**1. Register a template.** A template names the kind of job, the command or application to run, and
the arguments it takes.

**2. Run it.** Running a template creates a job, optionally with values for the template's
parameters.

**3. Watch it.** A job reports its status as it moves through the queue, and finished jobs keep their
outcome.

## The Job Model

### Templates and Runs

A template is the definition and a job is one execution of it. Templates are named and reusable,
jobs are individual and carry a status.

Two kinds of template exist. A `SHELL` template runs a command. A `SPARK` template submits a Spark
application.

### Job Status

| Status       | Meaning                                    |
|--------------|--------------------------------------------|
| `QUEUED`     | Waiting to be executed                     |
| `STARTED`    | Currently executing                        |
| `SUCCEEDED`  | Finished successfully                      |
| `FAILED`     | Finished with an error                     |
| `CANCELLING` | Cancellation requested, not yet complete   |
| `CANCELLED`  | Cancellation complete                      |

Cancellation is a request rather than an instant, which is why `CANCELLING` and `CANCELLED` are
separate. A job that finishes before the cancellation lands stays in its finished state.

### Parameters

A template can declare parameters, and a run supplies values for them. That is what makes one
template serve many cases, rather than a near-copy per variation.

## Working With Jobs in the UI

The jobs area lists job templates and the runs made from them, with each run's status. Templates can
be registered and jobs started from there.

## Permissions

| Privilege               | Grantable on              | What it allows                   |
|-------------------------|---------------------------|----------------------------------|
| `REGISTER_JOB_TEMPLATE` | Metalake                  | Registering job templates        |
| `USE_JOB_TEMPLATE`      | Metalake, or one template | Reading and using a job template |
| `RUN_JOB`               | Metalake                  | Running a job                    |

Altering and deleting a job template are reserved for the metalake owner and the template owner.

## Using the API

Job templates and jobs can be registered, listed, run, and cancelled over REST and through the Java
and Python clients. Endpoints, payload shapes, and worked examples are in
[Manage Jobs](./manage-jobs-in-gravitino.md).
