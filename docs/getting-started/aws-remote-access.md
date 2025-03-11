---
title: "Remote Access for Apache Gravitino on AWS"
slug: /getting-started/aws-remote-access
license: "This software is licensed under the Apache License version 2."
---

## Accessing Apache Gravitino on AWS externally

When you deploy Gravitino on AWS, accessing it externally requires
some additional configuration due to how AWS networking works.

AWS assigns your instance a public IP address, but Gravitino can't bind to that address.
To resolve this, you must find the internal IP address assigned to your AWS instance.
You can locate the private IP address in the AWS console, or by running the following command:

```shell
ip a
```

Once you have identified the internal address, edit the Gravitino configuration to bind to that address.
Open the file `<gravitino-home>/conf/gravitino.conf` and change the `gravitino.server.webserver.host`
parameter from `127.0.0.1` to your AWS instance's private IP4 address;
or you can use '0.0.0.0'. '0.0.0.0' in this context means the host's IP address.
Restart the Gravitino server so the change takes effect.

```shell
<gravitino-home>/bin/gravitino.sh restart
```

You'll also need to open port 8090 in the security group of your AWS instance to access Gravitino.
To access Hive you need to open port 10000 in the security group.

After completing these steps, you can access the Gravitino REST interface
from the command line or a web browser on your local computer.
You can also connect to Hive via DBeaver or any other database IDE.

