---
title: "Fileset Catalog Index"
slug: "/fileset-catalog-index"
date: 2025-01-13
keyword: "Fileset catalog index S3 GCS ADLS OSS"
license: "This software is licensed under the Apache License version 2."
---

## Fileset Catalog Overall

Gravitino Fileset catalog index includes the following chapters:

- [Fileset catalog overview and features](./fileset-catalog.md): This chapter provides an overview of the Fileset catalog, its features, capabilities and related configurations.
- [Manage Fileset catalog with Gravitino API](./manage-fileset-metadata-using-gravitino.md): This chapter explains how to manage fileset metadata using Gravitino API and provides detailed examples.
- [Using Fileset catalog with Gravitino virtual file system](how-to-use-gvfs.md): This chapter explains how to use Fileset catalog with the Gravitino virtual file system and provides detailed examples.

## Fileset Catalog with Cloud Storage

Each cloud backend has its own page with a runnable end-to-end example covering catalog setup and
Java/Hadoop data access. The S3, GCS, ADLS, and OSS pages also cover Python and pandas; COS
currently has no Python data-plane implementation:

- [Using Fileset catalog to manage Amazon S3](./fileset-catalog-with-s3.md).
- [Using Fileset catalog to manage Google Cloud Storage](./fileset-catalog-with-gcs.md).
- [Using Fileset catalog to manage Azure Data Lake Storage](./fileset-catalog-with-adls.md).
- [Using Fileset catalog to manage Alibaba Cloud OSS](./fileset-catalog-with-oss.md).
- [Using Fileset catalog to manage Tencent Cloud COS](./fileset-catalog-with-cos.md).

More storage options will be added soon. Stay tuned!
