---
title: "How to customize Gravitino server configurations"
date: 2023-11-20T09:03:20-08:00
license: "Copyright 2023 Datastrato.
This software is licensed under the Apache License version 2."
---
## Gravitino Security: Things You Need To Know
Gravitino supports different security features, 

## Authentication
| Configuration item                                | Description                                                                | Default value     | Since version |
|---------------------------------------------------|----------------------------------------------------------------------------|-------------------|---------------|
| `gravitino.authenticator`                         | The authenticator which Gravitino uses, setting as `simple` or `oauth`     | `simple`          | 0.3.0         |
| `gravitino.authenicator.oauth.serviceAudience`    | The audience name when Gravitino uses oauth as the authenticator           | `GravitinoServer` | 0.3.0         |
| `gravitino.authenticator.oauth.allowSkewSecs`     | The jwt allows skew seconds when Gravitino uses oauth as the authenticator | `0`               | 0.3.0         |
| `gravitino.authenticator.oauth.defaultSignKey`    | The sign key of jwt when Gravitino uses oauth as the authenticator         | `null`            | 0.3.0         |
| `gravitino.authenticator.oauth.signAlgorithmType` | The signature algorithm when Gravitino uses oauth as the authenticator     | `RS256`           | 0.3.0         |