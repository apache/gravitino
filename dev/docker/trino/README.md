---
title: "Gravitino Trino Docker Container"
date: 2023-10-03T09:03:20-08:00
license: "Copyright 2023 Datastrato.
This software is licensed under the Apache License version 2."
---


docker network ls --filter driver=bridge --format "{{.ID}}" | xargs docker network inspect --format "route {{range .IPAM.Config}}{{.Subnet}}{{end}}" >> ./docker-connector.conf
sudo ./docker-connector -config ./docker-connector.conf


```
#docker network create mynetwork
#docker run --rm -it --name database --network gravitino-network --network-alias database busybox
#docker run --rm -it --name webapp --network gravitino-network --network-alias webapp -p 8080:8080 busybox
#docker run --rm -it --name trino --network gravitino-network --network-alias trino trinodb/trino:426

# docker exec -u 0 -it 6fc0e06d77f8 bash
# apt-get update && apt-get install -y dnsutils inetutils-ping net-tools telnet

docker network create gravitino-ci-network


docker run --rm -d --name gravitino-ci-hive -p 8022:22 -p 8088:8088 -p 9000:9000 -p 9083:9083 -p 10000:10000 -p 10002:10002 -p 50070:50070 -p 50075:50075 -p 50010:50010 --network gravitino-ci-network --network-alias gravitino-ci-hive datastrato/graviton-ci-hive:test


# docker run --rm -d --name gravitino-ci-hive -p 9000:9000 -p 9083:9083 --network gravitino-ci-network --network-alias gravitino-ci-hive datastrato/graviton-ci-hive:test
docker run --rm -it --name trino-coordinator -e HADOOP_USER_NAME=root -v /Users/liuxun/Github/xunliu/graviton/trino-connector/build/libs:/usr/lib/trino/plugin/gravitino -v /Users/liuxun/Github/xunliu/graviton/dev/docker/trino/conf:/etc/trino -p 5005:5005 -p 9080:9080 -p 8080:8080 trinodb/trino:426


docker run --rm -it -e HADOOP_USER_NAME=root -p 5005:5005 -p 9080:9080 -p 8080:8080 datastrato/graviton-ci-trino:0.1.0

./build-docker.sh --platform linux/amd64 --image datastrato/graviton-ci-trino --tag 0.1.0

## Hive

CREATE TABLE mytable (
  id INT,
  name STRING,
  age INT
);

INSERT INTO mytable VALUES (1, 'John', 25);
INSERT INTO mytable VALUES (2, 'Jane', 30);

## Trino
create SCHEMA hive.sales with (location = 'hdfs://gravitino-ci-hive:9000/user/hive/warehouse/sales.db');

#CREATE SCHEMA hive.sales

USE hive.sales;

CREATE TABLE sample_table1 (
  col_1 varchar
);
INSERT INTO sample_table1 VALUES ('test');

trino:sales> CREATE TABLE sample_table1 (
          ->   col_1 varchar, col_2 varchar, col_3 varchar, col_4 varchar, col_5 varchar,
          ->   xing varchar, fing varchar
          -> );
Query 20230920_142334_00006_ah6sd failed: Failed checking path: hdfs://localhost:9000/user/hive/warehouse/sales.db
```


## Test
docker run --rm -d -e HADOOP_USER_NAME=hive -p 8022:22 -p 8088:8088 -p 9000:9000 -p 9083:9083 -p 10000:10000 -p 10002:10002 -p 50070:50070 -p 50075:50075 -p 50010:50010 datastrato/gravitino-ci-hive:0.1.2

curl -X POST -H "Content-Type: application/json" -d '{"name":"company1","comment":"company1 Game","properties":{}}' http://127.0.0.1:8090/api/metalakes
curl -X POST -H "Content-Type: application/json" -d '{"name":"hive_test","type":"RELATIONAL","comment":"comment","provider":"hive", "properties":{"metastore.uris":"thrift://192.168.50.74:9083"}}' http://127.0.0.1:8090/api/metalakes/company1/catalogs
curl -X DELETE -H "Content-Type: application/json" http://127.0.0.1:8090/api/metalakes/company1/catalogs/hive_test/schemas/game1_db/tables/player
curl -X DELETE -H "Content-Type: application/json" http://127.0.0.1:8090/api/metalakes/company1/catalogs/hive_test/schemas/game1_db
curl -X DELETE -H "Content-Type: application/json" http://127.0.0.1:8090/api/metalakes/company1/catalogs/hive_test

hadoop dfs -chown -R liuxun:hive /user/hive/warehouse

docker run --rm -it -e HADOOP_USER_NAME=hive -v /Users/liuxun/Github/xunliu/graviton/trino-connector/build/libs:/usr/lib/trino/plugin/gravitino -v /Users/liuxun/Github/xunliu/graviton/integration-test/build/tirno-conf2:/etc/trino -p 5005:5005 -p 9080:9080 -p 8080:8080 datastrato/graviton-ci-trino:0.1.0

CREATE SCHEMA "company1.hive_test".game1_db
WITH (
location = 'hdfs://192.168.50.74:9000/user/hive/warehouse/game1_db.db'
);

create table "company1.hive_test".game1_db.player
(
user_name varchar,
gender varchar,
age varchar,
phone varchar,
email varchar,
address varchar,
birthday varchar,
create_time varchar,
update_time varchar
)
WITH (
format = 'TEXTFILE'
);

insert into "company1.hive_test".game1_db.player (user_name, gender, age, phone) values ('sam', 'man', '18', '+1 8157809623');



## 成功的容器
### hive
```
[
    {
        "Id": "5dfc892d84ce6d98f9ea739bd30498eaf4165d86111524f804b9e695175f91bf",
        "Created": "2023-10-25T10:46:14.598245723Z",
        "Path": "/bin/bash",
        "Args": [
            "/usr/local/sbin/start.sh"
        ],
        "State": {
            "Status": "running",
            "Running": true,
            "Paused": false,
            "Restarting": false,
            "OOMKilled": false,
            "Dead": false,
            "Pid": 31734,
            "ExitCode": 0,
            "Error": "",
            "StartedAt": "2023-10-25T10:46:14.943038483Z",
            "FinishedAt": "0001-01-01T00:00:00Z"
        },
        "Image": "sha256:fc41ac0fb1548d68a12fb0dfaac2997591e647dd3d628077bd41c119344516bb",
        "ResolvConfPath": "/var/lib/docker/containers/5dfc892d84ce6d98f9ea739bd30498eaf4165d86111524f804b9e695175f91bf/resolv.conf",
        "HostnamePath": "/var/lib/docker/containers/5dfc892d84ce6d98f9ea739bd30498eaf4165d86111524f804b9e695175f91bf/hostname",
        "HostsPath": "/var/lib/docker/containers/5dfc892d84ce6d98f9ea739bd30498eaf4165d86111524f804b9e695175f91bf/hosts",
        "LogPath": "/var/lib/docker/containers/5dfc892d84ce6d98f9ea739bd30498eaf4165d86111524f804b9e695175f91bf/5dfc892d84ce6d98f9ea739bd30498eaf4165d86111524f804b9e695175f91bf-json.log",
        "Name": "/elegant_mclean",
        "RestartCount": 0,
        "Driver": "overlay2",
        "Platform": "linux",
        "MountLabel": "",
        "ProcessLabel": "",
        "AppArmorProfile": "",
        "ExecIDs": [
            "85f55d5177d1d797b8bc12991c54a15ee619101205219f6cbded44d73f95e984"
        ],
        "HostConfig": {
            "Binds": null,
            "ContainerIDFile": "",
            "LogConfig": {
                "Type": "json-file",
                "Config": {}
            },
            "NetworkMode": "default",
            "PortBindings": {
                "10000/tcp": [
                    {
                        "HostIp": "",
                        "HostPort": "10000"
                    }
                ],
                "10002/tcp": [
                    {
                        "HostIp": "",
                        "HostPort": "10002"
                    }
                ],
                "22/tcp": [
                    {
                        "HostIp": "",
                        "HostPort": "8022"
                    }
                ],
                "50010/tcp": [
                    {
                        "HostIp": "",
                        "HostPort": "50010"
                    }
                ],
                "50070/tcp": [
                    {
                        "HostIp": "",
                        "HostPort": "50070"
                    }
                ],
                "50075/tcp": [
                    {
                        "HostIp": "",
                        "HostPort": "50075"
                    }
                ],
                "8088/tcp": [
                    {
                        "HostIp": "",
                        "HostPort": "8088"
                    }
                ],
                "9000/tcp": [
                    {
                        "HostIp": "",
                        "HostPort": "9000"
                    }
                ],
                "9083/tcp": [
                    {
                        "HostIp": "",
                        "HostPort": "9083"
                    }
                ]
            },
            "RestartPolicy": {
                "Name": "no",
                "MaximumRetryCount": 0
            },
            "AutoRemove": true,
            "VolumeDriver": "",
            "VolumesFrom": null,
            "CapAdd": null,
            "CapDrop": null,
            "CgroupnsMode": "private",
            "Dns": [],
            "DnsOptions": [],
            "DnsSearch": [],
            "ExtraHosts": null,
            "GroupAdd": null,
            "IpcMode": "private",
            "Cgroup": "",
            "Links": null,
            "OomScoreAdj": 0,
            "PidMode": "",
            "Privileged": false,
            "PublishAllPorts": false,
            "ReadonlyRootfs": false,
            "SecurityOpt": null,
            "UTSMode": "",
            "UsernsMode": "",
            "ShmSize": 67108864,
            "Runtime": "runc",
            "ConsoleSize": [
                0,
                0
            ],
            "Isolation": "",
            "CpuShares": 0,
            "Memory": 0,
            "NanoCpus": 0,
            "CgroupParent": "",
            "BlkioWeight": 0,
            "BlkioWeightDevice": [],
            "BlkioDeviceReadBps": null,
            "BlkioDeviceWriteBps": null,
            "BlkioDeviceReadIOps": null,
            "BlkioDeviceWriteIOps": null,
            "CpuPeriod": 0,
            "CpuQuota": 0,
            "CpuRealtimePeriod": 0,
            "CpuRealtimeRuntime": 0,
            "CpusetCpus": "",
            "CpusetMems": "",
            "Devices": [],
            "DeviceCgroupRules": null,
            "DeviceRequests": null,
            "KernelMemory": 0,
            "KernelMemoryTCP": 0,
            "MemoryReservation": 0,
            "MemorySwap": 0,
            "MemorySwappiness": null,
            "OomKillDisable": null,
            "PidsLimit": null,
            "Ulimits": null,
            "CpuCount": 0,
            "CpuPercent": 0,
            "IOMaximumIOps": 0,
            "IOMaximumBandwidth": 0,
            "MaskedPaths": [
                "/proc/asound",
                "/proc/acpi",
                "/proc/kcore",
                "/proc/keys",
                "/proc/latency_stats",
                "/proc/timer_list",
                "/proc/timer_stats",
                "/proc/sched_debug",
                "/proc/scsi",
                "/sys/firmware"
            ],
            "ReadonlyPaths": [
                "/proc/bus",
                "/proc/fs",
                "/proc/irq",
                "/proc/sys",
                "/proc/sysrq-trigger"
            ]
        },
        "GraphDriver": {
            "Data": {
                "LowerDir": "/var/lib/docker/overlay2/2827e23cec48b7ff11df929b10985ddcbb8743fca260fc838d712d5bcb0de740-init/diff:/var/lib/docker/overlay2/89f5628de14cdb576b0ce280645def0c7f52a9283e132ade77d55b530af40723/diff:/var/lib/docker/overlay2/bc3a6ca4472bf8068b37bec088fcfd957a3957f9824704d3f8eccd89072cc555/diff:/var/lib/docker/overlay2/8d6c9b4b96efad6b43fa15be440d9596f9f32c2fe5dfaf13dac160e674798e55/diff:/var/lib/docker/overlay2/7fcb089dafdd20045107394bac25428867d99cf15f032cc1cf9aab1e00b70887/diff:/var/lib/docker/overlay2/c7e45f47e622aaf906ff5813ca306ce79b969e0f618aba40b790deea6b955d10/diff:/var/lib/docker/overlay2/781bb7e05af22aface0f692bc02801362119e825151ecd08edc833693c15dd60/diff:/var/lib/docker/overlay2/5e9da1f60cc549df8e9ce9e5c7b6bb61f9687842434d09d987b96b8fb553d9ff/diff:/var/lib/docker/overlay2/995fb396660aa8551468f357069c14c7dfdb2124e837724285805ec1de33b898/diff:/var/lib/docker/overlay2/81b6a1443d46dbc94f70cbb18e03f1a98fd923e734af8e037ead781821f2802e/diff:/var/lib/docker/overlay2/b2f1cd8c66d68a95c7456285dce68f5c846642c8e9c6b2fdf822d693b6e3f539/diff:/var/lib/docker/overlay2/ee9c3e08f3580cc0170bd31885b8cf4937bf347346091e987792376c77f38089/diff:/var/lib/docker/overlay2/b174f1e34a3c8719dfab0bd7221c8ce98b16644624b06b879f429caaac637347/diff:/var/lib/docker/overlay2/2d96468917cdd8e2060e58d4c72d034974c83781548901401ace2f9b6c205622/diff:/var/lib/docker/overlay2/68a18026c4e5ca245bc40462965cb64d542c69ecd66bd0ebc9c1c56d92f1a816/diff:/var/lib/docker/overlay2/a4b820080f7a761fe6de148d477832a18c3bb48b3a8830842b10a35b8256fa56/diff:/var/lib/docker/overlay2/2049293dc0a0cb74756cd8ad9e255dbd87d359e659b261546b4f12ccf9261d56/diff:/var/lib/docker/overlay2/ccd6443ffaf675000f8889071fe5ee4f3a4a10e22a15d8a9705e9944eb614658/diff:/var/lib/docker/overlay2/8d971437db1fbead70ee2e3b0bdc7b5c5284a92fe7decd75d72e5d538ba8cb12/diff:/var/lib/docker/overlay2/370a4e2b6d09927fa6281104b192914c922c1ecf348b91f6b662af8ef52ca5c8/diff:/var/lib/docker/overlay2/641173fc7e004a41e55723ee5fbac446b874c8d5ecf6b6f4218b65e16491fb21/diff:/var/lib/docker/overlay2/d1d0fcaef1f2b987e9f5bf592ffd836056ee87524bd2c0390df5298bebd18aa8/diff:/var/lib/docker/overlay2/edc6fb7e0a35632ed85fabffbb6d2e8227cfc0a079c64ce3869be227464e7d6b/diff:/var/lib/docker/overlay2/12ec1dfd3f0e8b1bcd628cdd0f940d1723cb36016c89ac1019e36d7b80f644b8/diff:/var/lib/docker/overlay2/d32145cf01ac8fddc290f2366c128288a8f7ddd03ddf8313d60b1d0609b7a20a/diff:/var/lib/docker/overlay2/d2cd4a5ae6e26b4085866fdf994cea8166f12de868529ea2ad263b9665196755/diff:/var/lib/docker/overlay2/538ce3c84192a18150614ef26004b0bb14bb9f291f04031fd7b59c4086ac20bd/diff:/var/lib/docker/overlay2/fa5b0f918e11498c0719aa8f8eb5fe671e68cc968e0e5ce6cc6bf5f65b2fb37e/diff:/var/lib/docker/overlay2/514a0b8213df14a16889c9a050175dd1791e6b98446e4352fe491911625c8556/diff:/var/lib/docker/overlay2/274fc6e71d18822e979a5d77669e3b3b4239b1b81f727b1b9d5a8eb7ae56b450/diff:/var/lib/docker/overlay2/d16e8df385b28781c8e8cfc8752a27af32e4208a6362acc7c72ff72a9effd61d/diff:/var/lib/docker/overlay2/27f656f719df0540d232f55565e807e214e239076268be57fec76511bdb5ce4c/diff:/var/lib/docker/overlay2/152072b6acd569e2e74909b07d791be6a9096a90e419d6c874d0ed42ff9fdd98/diff:/var/lib/docker/overlay2/861318a204ae9b59056c5199e18b3194dab848123a67e0db4dab68795614eaae/diff:/var/lib/docker/overlay2/f6f3f88358186c81987e0f76f1afed882df856f60b9dee84f5ef346c82cd0ed0/diff:/var/lib/docker/overlay2/0c75e519a10a908d80a3b35a68e04b51cdb76711695b1d5b1d357ddf4ef2f566/diff:/var/lib/docker/overlay2/eda90026dcfddb417b5923ebc98a79604cb45095636525c2eb7edef57c86b42d/diff:/var/lib/docker/overlay2/82d755760fb1f75cacd73eba93e55232e37b40fd2a7d71dbd1e55bdc38b17fd2/diff:/var/lib/docker/overlay2/38e6c0774d51108c55d1f1f2d4cc862fc93c13354ade9abe20fdce3f72c93e1d/diff:/var/lib/docker/overlay2/b343810c1c63171c30a3378937d5bbdc67ac36d8ebed2dc4c35d3d54dcfdf3e8/diff:/var/lib/docker/overlay2/ae943859c43494c49e831d096c0dbf7e34ba409bcdd805eee0a3be74d80c0f34/diff:/var/lib/docker/overlay2/ef54b821747b9fd7bd7d49aeb559b13ed5363657cb0d661cacc46beb3bc68204/diff:/var/lib/docker/overlay2/92bd82e0a341c4c4782ea4f746b7ad0b1d14df54f2fce8cf72f2b1ac501b8fd8/diff:/var/lib/docker/overlay2/7d7def018b8759116d0c34cb17ffa21df278e8878a5f238f73e5ba7e0708e3c2/diff:/var/lib/docker/overlay2/c1d11d2a9be1ac90efdbc4ced5607a0c6e7bdea0bf241e3251694030a58da7f5/diff:/var/lib/docker/overlay2/0d9ff459ff1fff8115afad3d9a2a1e4b0b9ed41a862e9d8ad50d84fdedf3b0de/diff:/var/lib/docker/overlay2/ee9ef7f8ee78085f948f9e0b8af3eb74c09dc6b032e3f55430e065363764d8a0/diff:/var/lib/docker/overlay2/ff51a44f818e2c2dfba195ebc65f3ce0ed930eac344a330b960425f6dc6e5fda/diff:/var/lib/docker/overlay2/2a0f676f9c4d5ec37fcc520f6b19ffe79fabbe2c8b09995ff10185d1988034fe/diff:/var/lib/docker/overlay2/15a48c248fe6291a8f3f2cde07c76a854a2eac1cc220c14c9a1e80552c386ee5/diff:/var/lib/docker/overlay2/964048effa76d43d450509645c584949fbe4ee1b21941251090a8780746675b8/diff:/var/lib/docker/overlay2/eb7271e234c28feda7b71f5582ced8afcd79c7f8f3136c27646ae486df63f1c8/diff:/var/lib/docker/overlay2/d5276372ca2fe8943cb2a0805d85a206f096bab1a8c875db1d855640b79de630/diff:/var/lib/docker/overlay2/9751a185c9b54586d8c10276d7e04d93529cb26c0cab629f7c53844b57aa1178/diff:/var/lib/docker/overlay2/ca921ebffa663382abce29310720e3bfad14caac5895231d95bbac698cbf9f43/diff:/var/lib/docker/overlay2/4342c75a0b56f73e547492d2ab359a8728411d5197e2b453c85ce4c153eea629/diff:/var/lib/docker/overlay2/c0c81e0196ba5ade816a92b0fc219d04bfaf5e915ab01b1b530b184a2099dfa5/diff:/var/lib/docker/overlay2/2626cf8fb76864c179393e0984a08730d5344cc9a1216abf227387fbd3913ab2/diff:/var/lib/docker/overlay2/274a0566012d3cb09a29212f6c1b93225af9053806fb4ad6d316c812183006fb/diff:/var/lib/docker/overlay2/9f1135b1ff04d59fa14276600ff654e8846b4fafd824b691db23196da56e899b/diff:/var/lib/docker/overlay2/ae3e07549e8f01f776432dcfd8fa443eaa003f052d37b7af1afde60ec4586269/diff:/var/lib/docker/overlay2/9c4d34ddbec7ea9ad59c9ba1cce927562c09ffd355cf9eaf75ab6bc59073c8f5/diff:/var/lib/docker/overlay2/132d8b46bb0b6c51b3ddcd41f2859557c739567d6dfa9b83cde3b4207c34add5/diff:/var/lib/docker/overlay2/0f37d59aec4d994b1b2ee059fb24dba79d78ef014bab67572b4562a11950fc2e/diff",
                "MergedDir": "/var/lib/docker/overlay2/2827e23cec48b7ff11df929b10985ddcbb8743fca260fc838d712d5bcb0de740/merged",
                "UpperDir": "/var/lib/docker/overlay2/2827e23cec48b7ff11df929b10985ddcbb8743fca260fc838d712d5bcb0de740/diff",
                "WorkDir": "/var/lib/docker/overlay2/2827e23cec48b7ff11df929b10985ddcbb8743fca260fc838d712d5bcb0de740/work"
            },
            "Name": "overlay2"
        },
        "Mounts": [],
        "Config": {
            "Hostname": "5dfc892d84ce",
            "Domainname": "",
            "User": "",
            "AttachStdin": false,
            "AttachStdout": false,
            "AttachStderr": false,
            "ExposedPorts": {
                "10000/tcp": {},
                "10002/tcp": {},
                "22/tcp": {},
                "3306/tcp": {},
                "50010/tcp": {},
                "50070/tcp": {},
                "50075/tcp": {},
                "8088/tcp": {},
                "9000/tcp": {},
                "9083/tcp": {}
            },
            "Tty": false,
            "OpenStdin": false,
            "StdinOnce": false,
            "Env": [
                "PATH=/usr/local/jdk/bin:/usr/local/hadoop/bin:/usr/local/hadoop/sbin:/usr/local/hive/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
                "JAVA_HOME=/usr/local/jdk",
                "HIVE_HOME=/usr/local/hive",
                "HADOOP_HOME=/usr/local/hadoop",
                "HADOOP_HEAPSIZE=128",
                "HADOOP_INSTALL=/usr/local/hadoop",
                "HADOOP_MAPRED_HOME=/usr/local/hadoop",
                "HADOOP_COMMON_HOME=/usr/local/hadoop",
                "HADOOP_HDFS_HOME=/usr/local/hadoop",
                "HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop",
                "YARN_HOME=/usr/local/hadoop",
                "CLASSPATH=/usr/local/hadoop/lib/*:HIVE_HOME/lib/*:.",
                "LD_LIBRARY_PATH=/usr/local/hadoop/lib/native",
                "MYSQL_PWD=ds123"
            ],
            "Cmd": null,
            "Image": "datastrato/gravitino-ci-hive:0.1.2",
            "Volumes": null,
            "WorkingDir": "/",
            "Entrypoint": [
                "/bin/bash",
                "/usr/local/sbin/start.sh"
            ],
            "OnBuild": null,
            "Labels": {
                "maintainer": "dev@datastrato.com"
            }
        },
        "NetworkSettings": {
            "Bridge": "",
            "SandboxID": "8754c9fdb1d5c536c9a56cfab01affbbfcd284314a63faa7b052420b98025a87",
            "HairpinMode": false,
            "LinkLocalIPv6Address": "",
            "LinkLocalIPv6PrefixLen": 0,
            "Ports": {
                "10000/tcp": [
                    {
                        "HostIp": "0.0.0.0",
                        "HostPort": "10000"
                    }
                ],
                "10002/tcp": [
                    {
                        "HostIp": "0.0.0.0",
                        "HostPort": "10002"
                    }
                ],
                "22/tcp": [
                    {
                        "HostIp": "0.0.0.0",
                        "HostPort": "8022"
                    }
                ],
                "3306/tcp": null,
                "50010/tcp": [
                    {
                        "HostIp": "0.0.0.0",
                        "HostPort": "50010"
                    }
                ],
                "50070/tcp": [
                    {
                        "HostIp": "0.0.0.0",
                        "HostPort": "50070"
                    }
                ],
                "50075/tcp": [
                    {
                        "HostIp": "0.0.0.0",
                        "HostPort": "50075"
                    }
                ],
                "8088/tcp": [
                    {
                        "HostIp": "0.0.0.0",
                        "HostPort": "8088"
                    }
                ],
                "9000/tcp": [
                    {
                        "HostIp": "0.0.0.0",
                        "HostPort": "9000"
                    }
                ],
                "9083/tcp": [
                    {
                        "HostIp": "0.0.0.0",
                        "HostPort": "9083"
                    }
                ]
            },
            "SandboxKey": "/var/run/docker/netns/8754c9fdb1d5",
            "SecondaryIPAddresses": null,
            "SecondaryIPv6Addresses": null,
            "EndpointID": "ee4a03871bddff751d599e2d8a6b9ddef1ef731f5d0527025380b419e9752b70",
            "Gateway": "172.17.0.1",
            "GlobalIPv6Address": "",
            "GlobalIPv6PrefixLen": 0,
            "IPAddress": "172.17.0.2",
            "IPPrefixLen": 16,
            "IPv6Gateway": "",
            "MacAddress": "02:42:ac:11:00:02",
            "Networks": {
                "bridge": {
                    "IPAMConfig": null,
                    "Links": null,
                    "Aliases": null,
                    "NetworkID": "edb61f382d1a1fb8d6e40e8756d5ae07fafb65e99869ed7feac3377f25a37bc1",
                    "EndpointID": "ee4a03871bddff751d599e2d8a6b9ddef1ef731f5d0527025380b419e9752b70",
                    "Gateway": "172.17.0.1",
                    "IPAddress": "172.17.0.2",
                    "IPPrefixLen": 16,
                    "IPv6Gateway": "",
                    "GlobalIPv6Address": "",
                    "GlobalIPv6PrefixLen": 0,
                    "MacAddress": "02:42:ac:11:00:02",
                    "DriverOpts": null
                }
            }
        }
    }
]
```

### trino
```shell
[
    {
        "Id": "0bd3f0bf237c0c3ded2a13c17b8cce6a6f142daa7ac47d43a29acf61f120209c",
        "Created": "2023-10-25T10:46:46.088463249Z",
        "Path": "/__cacert_entrypoint.sh",
        "Args": [
            "/usr/lib/trino/bin/run-trino"
        ],
        "State": {
            "Status": "running",
            "Running": true,
            "Paused": false,
            "Restarting": false,
            "OOMKilled": false,
            "Dead": false,
            "Pid": 33046,
            "ExitCode": 0,
            "Error": "",
            "StartedAt": "2023-10-25T10:46:46.567746412Z",
            "FinishedAt": "0001-01-01T00:00:00Z",
            "Health": {
                "Status": "healthy",
                "FailingStreak": 0,
                "Log": [
                    {
                        "Start": "2023-10-25T10:52:02.656116134Z",
                        "End": "2023-10-25T10:52:02.729155505Z",
                        "ExitCode": 0,
                        "Output": ""
                    },
                    {
                        "Start": "2023-10-25T10:52:12.712956719Z",
                        "End": "2023-10-25T10:52:12.794057058Z",
                        "ExitCode": 0,
                        "Output": ""
                    },
                    {
                        "Start": "2023-10-25T10:52:22.798293423Z",
                        "End": "2023-10-25T10:52:22.877960723Z",
                        "ExitCode": 0,
                        "Output": ""
                    },
                    {
                        "Start": "2023-10-25T10:52:32.886713641Z",
                        "End": "2023-10-25T10:52:32.960654245Z",
                        "ExitCode": 0,
                        "Output": ""
                    },
                    {
                        "Start": "2023-10-25T10:52:42.948987263Z",
                        "End": "2023-10-25T10:52:43.025714398Z",
                        "ExitCode": 0,
                        "Output": ""
                    }
                ]
            }
        },
        "Image": "sha256:6a2ad4a96b14bbb500b37191392ed52e745c3ea81d25c47b89994bb157f6022c",
        "ResolvConfPath": "/var/lib/docker/containers/0bd3f0bf237c0c3ded2a13c17b8cce6a6f142daa7ac47d43a29acf61f120209c/resolv.conf",
        "HostnamePath": "/var/lib/docker/containers/0bd3f0bf237c0c3ded2a13c17b8cce6a6f142daa7ac47d43a29acf61f120209c/hostname",
        "HostsPath": "/var/lib/docker/containers/0bd3f0bf237c0c3ded2a13c17b8cce6a6f142daa7ac47d43a29acf61f120209c/hosts",
        "LogPath": "/var/lib/docker/containers/0bd3f0bf237c0c3ded2a13c17b8cce6a6f142daa7ac47d43a29acf61f120209c/0bd3f0bf237c0c3ded2a13c17b8cce6a6f142daa7ac47d43a29acf61f120209c-json.log",
        "Name": "/zealous_maxwell",
        "RestartCount": 0,
        "Driver": "overlay2",
        "Platform": "linux",
        "MountLabel": "",
        "ProcessLabel": "",
        "AppArmorProfile": "",
        "ExecIDs": [
            "0b38ef5058484c69cead01caf452e8a26c26c2ef359afc2530390b4e0462b051"
        ],
        "HostConfig": {
            "Binds": [
                "/Users/liuxun/Github/xunliu/graviton/trino-connector/build/libs:/usr/lib/trino/plugin/gravitino",
                "/Users/liuxun/Github/xunliu/graviton/integration-test/build/tirno-conf:/etc/trino"
            ],
            "ContainerIDFile": "",
            "LogConfig": {
                "Type": "json-file",
                "Config": {}
            },
            "NetworkMode": "default",
            "PortBindings": {
                "5005/tcp": [
                    {
                        "HostIp": "",
                        "HostPort": "5005"
                    }
                ],
                "8080/tcp": [
                    {
                        "HostIp": "",
                        "HostPort": "8080"
                    }
                ],
                "9080/tcp": [
                    {
                        "HostIp": "",
                        "HostPort": "9080"
                    }
                ]
            },
            "RestartPolicy": {
                "Name": "no",
                "MaximumRetryCount": 0
            },
            "AutoRemove": true,
            "VolumeDriver": "",
            "VolumesFrom": null,
            "CapAdd": null,
            "CapDrop": null,
            "CgroupnsMode": "private",
            "Dns": [],
            "DnsOptions": [],
            "DnsSearch": [],
            "ExtraHosts": null,
            "GroupAdd": null,
            "IpcMode": "private",
            "Cgroup": "",
            "Links": null,
            "OomScoreAdj": 0,
            "PidMode": "",
            "Privileged": false,
            "PublishAllPorts": false,
            "ReadonlyRootfs": false,
            "SecurityOpt": null,
            "UTSMode": "",
            "UsernsMode": "",
            "ShmSize": 67108864,
            "Runtime": "runc",
            "ConsoleSize": [
                0,
                0
            ],
            "Isolation": "",
            "CpuShares": 0,
            "Memory": 0,
            "NanoCpus": 0,
            "CgroupParent": "",
            "BlkioWeight": 0,
            "BlkioWeightDevice": [],
            "BlkioDeviceReadBps": null,
            "BlkioDeviceWriteBps": null,
            "BlkioDeviceReadIOps": null,
            "BlkioDeviceWriteIOps": null,
            "CpuPeriod": 0,
            "CpuQuota": 0,
            "CpuRealtimePeriod": 0,
            "CpuRealtimeRuntime": 0,
            "CpusetCpus": "",
            "CpusetMems": "",
            "Devices": [],
            "DeviceCgroupRules": null,
            "DeviceRequests": null,
            "KernelMemory": 0,
            "KernelMemoryTCP": 0,
            "MemoryReservation": 0,
            "MemorySwap": 0,
            "MemorySwappiness": null,
            "OomKillDisable": null,
            "PidsLimit": null,
            "Ulimits": null,
            "CpuCount": 0,
            "CpuPercent": 0,
            "IOMaximumIOps": 0,
            "IOMaximumBandwidth": 0,
            "MaskedPaths": [
                "/proc/asound",
                "/proc/acpi",
                "/proc/kcore",
                "/proc/keys",
                "/proc/latency_stats",
                "/proc/timer_list",
                "/proc/timer_stats",
                "/proc/sched_debug",
                "/proc/scsi",
                "/sys/firmware"
            ],
            "ReadonlyPaths": [
                "/proc/bus",
                "/proc/fs",
                "/proc/irq",
                "/proc/sys",
                "/proc/sysrq-trigger"
            ]
        },
        "GraphDriver": {
            "Data": {
                "LowerDir": "/var/lib/docker/overlay2/6ad4adac9b506c85f260a140cc89961ac892c1633750ff486c48731b7836477c-init/diff:/var/lib/docker/overlay2/017d3c96f0565b03bea9884567f9d761bcfba08408e1df1d34eddaff835a6b64/diff:/var/lib/docker/overlay2/3f50f1b1b22ceb7b95f8cadfc2703ece37585f09ccef4e6fbe976b37f79e8efd/diff:/var/lib/docker/overlay2/741bfc2480c57acacd920f0b3e99377ded111a11f4ebe083a8e6bce6e677b003/diff:/var/lib/docker/overlay2/2911e879abc98cae034cc5aba64e7164979bebc6d5b0e1b9af5a2fd81e40e68c/diff:/var/lib/docker/overlay2/1a358aeaa6f688ba983c65834323ead83953151a2eb8721170780099947af99a/diff:/var/lib/docker/overlay2/618d333508226f1d0824249be24b50d85bdb909382181be475ce6936560a793a/diff:/var/lib/docker/overlay2/56334043d449857f6b6b8120155fcf231b2b34f23501b63149a5b3e1fb41e5b9/diff:/var/lib/docker/overlay2/05931594cbbb74dd00ce950691997dc68e1baa76c76423770447af28c22e80bf/diff:/var/lib/docker/overlay2/fd614ef31bd860a0e0240c5bb8ed57bb1dc2c8678f2dbfc4e6c2d2e9af2fcbaa/diff:/var/lib/docker/overlay2/3e020236b44f2e230c49ac6714e5a596d9e21f640e822303fa3eb545efaa1fbc/diff:/var/lib/docker/overlay2/cd90a7b6b9178deca9a2a059c4cbf1870e43a1936384d0462fe81d9a12578e74/diff:/var/lib/docker/overlay2/2a106abdc83e0af910d15c5512a6f07f37c9f4e805450ce4bff89c1cd33f8205/diff:/var/lib/docker/overlay2/656444fb4cfe95c4480c746ebc577d9ed75077317b115944c0b92e85b74cb80f/diff:/var/lib/docker/overlay2/a93bf7fd027c01da874d0ddc69c87b02d23998bfc8ad35db7ae04ba7b2b5500c/diff:/var/lib/docker/overlay2/131d4d542f27b21171d396de32b03fe8e382dbdc1ac1d23fc3cb9223d070d230/diff:/var/lib/docker/overlay2/6d1e28ce2872e80df7f81a1475edcd4652f04b49f9b0c2fc7dac1f8cded54a66/diff:/var/lib/docker/overlay2/2997021ab296de9bd65c1cb09f4b4f0ed3af5ee7fdba331150b509a29ef16631/diff:/var/lib/docker/overlay2/6149b41299d7ee92adc89b79b3071d6f7b0f9bfa278a54d7eb0ae09d47d57066/diff:/var/lib/docker/overlay2/52677b38c02ac34969c9c0b3b620bc9cee0a3d91bdfb175778b59f0deded6172/diff:/var/lib/docker/overlay2/a06e18ef6d88b644e05d1efc2ec40d1e1f8034dacb438be1831cda66b3167bcf/diff:/var/lib/docker/overlay2/d6f0f8753aeb50c845a62915ebff55ff26b1565695743834c60efb1d319877dc/diff:/var/lib/docker/overlay2/b9c50b442582d21e1e6a3ede1f2da2fc3d69be617b13f0622e720ba022a8d654/diff:/var/lib/docker/overlay2/ed462adfe666347122673af019c5f571082ec33ddd6b89856568c365452942a6/diff:/var/lib/docker/overlay2/5870a888c3db67d528baf2287c753cbb16624e1251020e54963c953980d960d5/diff:/var/lib/docker/overlay2/cc91320dc75f61b0a7e02272595022f1527d42d8a0112ae13713d8041c975ddc/diff:/var/lib/docker/overlay2/454bb9380be2752927e374e31f3a5fecfb1b07704b40be250ec8bf09a79428c1/diff:/var/lib/docker/overlay2/8595cd4f72f2ac082eb41c87f10d18b9fe0a577a425c2f0ef9cf0996ccc57c07/diff:/var/lib/docker/overlay2/7da5a964efc04f1aea7a665732ab5f41432433dd4dde0525b85b1348cb8f977a/diff:/var/lib/docker/overlay2/7d9435669852b4a2b6ee0f98546d07f9e7e3476f70915f00548c06e3e2fa3a30/diff:/var/lib/docker/overlay2/6551b05dd2c7c67f3ee39b2c21edb6b47a69306ad866d14ae43ce365088226be/diff:/var/lib/docker/overlay2/64eda712f35ea656c18430cf2441111dfa7982bf3a2644f62c39677277ae7370/diff:/var/lib/docker/overlay2/c89e3bc43379dcd1d46ea173ea4d70d9cb7293c502ce4159933cc74b7ada2f90/diff:/var/lib/docker/overlay2/4584b12f5fcdb3d6092730fcbcee05552221ee6c3b7c1c68398cf826bfa205d5/diff:/var/lib/docker/overlay2/ed7e42b78c92d9e9d6ace8ec2db0f78563f99ad63fe81a88b6274714e7ece51c/diff:/var/lib/docker/overlay2/2fa609e01cfbe9f377afffce56e3ade4d92ab1174c635f9bb6c916f31214cf5d/diff:/var/lib/docker/overlay2/9423546a79198fd70504dd7577ff62a6b4f8c6a8b8121c746458e25e9907b0d1/diff:/var/lib/docker/overlay2/aeeac9ec36abe9b95ec54ebd6e16848580d72f0dfe3a722744d396420e00cd0a/diff:/var/lib/docker/overlay2/9443ee08347b7a3750be5dce30b7613df7606908bf9c1f3d8c753d857cfc43f9/diff:/var/lib/docker/overlay2/63d5f7be18a8efa6e64354f016ef59d2663f3c960a4ae07981fe9cc16399d1f0/diff:/var/lib/docker/overlay2/e72d9ed9fb7c28751c6d56b8e7fa84abd27beb3ae3726e0b31236599f7becf11/diff:/var/lib/docker/overlay2/6180bee377fe5dbccba239c14db6e8ea3660742bb966b551b3cc01f930abc0e1/diff:/var/lib/docker/overlay2/a709a64c432ca9aca2b5c318928afad3e92f69a5c56617c62e5b25bb8674ada9/diff:/var/lib/docker/overlay2/9b8175269b335bb7d8d72fddd1f1293d791bbf1610ca10cb8bd99675b304bec9/diff:/var/lib/docker/overlay2/e403b3338169564f15176a83a3e07cc19d78920ed4fbeaf9e46bae789b10c01e/diff:/var/lib/docker/overlay2/b24405047c11f69a6681b06871bf30ff8046dbb23c50efd5845fcd0f9d101440/diff:/var/lib/docker/overlay2/75d30d87b70a41a9eb55157691c9dddc3543ba54bec342ee5d0595e2936e2b4f/diff:/var/lib/docker/overlay2/92833809a9ac4e21c657aa0b133c887651206f98462023d74ca8dfd07cd9777f/diff:/var/lib/docker/overlay2/a979a54398513f8a7caf59c7cb3e0f0d83ef3602cae0c19fa45ec12c97c3a5c1/diff:/var/lib/docker/overlay2/eef58070d71111d2d6cd5c01b91de11762087819b67d17b8347fd97015805077/diff",
                "MergedDir": "/var/lib/docker/overlay2/6ad4adac9b506c85f260a140cc89961ac892c1633750ff486c48731b7836477c/merged",
                "UpperDir": "/var/lib/docker/overlay2/6ad4adac9b506c85f260a140cc89961ac892c1633750ff486c48731b7836477c/diff",
                "WorkDir": "/var/lib/docker/overlay2/6ad4adac9b506c85f260a140cc89961ac892c1633750ff486c48731b7836477c/work"
            },
            "Name": "overlay2"
        },
        "Mounts": [
            {
                "Type": "bind",
                "Source": "/Users/liuxun/Github/xunliu/graviton/integration-test/build/tirno-conf",
                "Destination": "/etc/trino",
                "Mode": "",
                "RW": true,
                "Propagation": "rprivate"
            },
            {
                "Type": "bind",
                "Source": "/Users/liuxun/Github/xunliu/graviton/trino-connector/build/libs",
                "Destination": "/usr/lib/trino/plugin/gravitino",
                "Mode": "",
                "RW": true,
                "Propagation": "rprivate"
            }
        ],
        "Config": {
            "Hostname": "0bd3f0bf237c",
            "Domainname": "",
            "User": "trino:trino",
            "AttachStdin": true,
            "AttachStdout": true,
            "AttachStderr": true,
            "ExposedPorts": {
                "5005/tcp": {},
                "8080/tcp": {},
                "9080/tcp": {}
            },
            "Tty": true,
            "OpenStdin": true,
            "StdinOnce": true,
            "Env": [
                "HADOOP_USER_NAME=root",
                "PATH=/opt/java/openjdk/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
                "JAVA_HOME=/opt/java/openjdk",
                "LANG=en_US.UTF-8",
                "LANGUAGE=en_US:en",
                "LC_ALL=en_US.UTF-8",
                "JAVA_VERSION=jdk-17.0.8.1+1"
            ],
            "Cmd": [
                "/usr/lib/trino/bin/run-trino"
            ],
            "Healthcheck": {
                "Test": [
                    "CMD-SHELL",
                    "/usr/lib/trino/bin/health-check"
                ],
                "Interval": 10000000000,
                "Timeout": 5000000000,
                "StartPeriod": 10000000000
            },
            "Image": "datastrato/graviton-ci-trino:0.1.0",
            "Volumes": null,
            "WorkingDir": "",
            "Entrypoint": [
                "/__cacert_entrypoint.sh"
            ],
            "OnBuild": null,
            "Labels": {
                "maintainer": "support@datastrato.com",
                "org.opencontainers.image.ref.name": "ubuntu",
                "org.opencontainers.image.version": "22.04"
            }
        },
        "NetworkSettings": {
            "Bridge": "",
            "SandboxID": "64349d4d7c21fd93f7376c8aa12c541d22865e4aa0092d57e049a4c235f7921f",
            "HairpinMode": false,
            "LinkLocalIPv6Address": "",
            "LinkLocalIPv6PrefixLen": 0,
            "Ports": {
                "5005/tcp": [
                    {
                        "HostIp": "0.0.0.0",
                        "HostPort": "5005"
                    }
                ],
                "8080/tcp": [
                    {
                        "HostIp": "0.0.0.0",
                        "HostPort": "8080"
                    }
                ],
                "9080/tcp": [
                    {
                        "HostIp": "0.0.0.0",
                        "HostPort": "9080"
                    }
                ]
            },
            "SandboxKey": "/var/run/docker/netns/64349d4d7c21",
            "SecondaryIPAddresses": null,
            "SecondaryIPv6Addresses": null,
            "EndpointID": "e1861e3fc4c92861dd349276395b38256ecddeb5cdc90c40d7e4340330c5c3e9",
            "Gateway": "172.17.0.1",
            "GlobalIPv6Address": "",
            "GlobalIPv6PrefixLen": 0,
            "IPAddress": "172.17.0.4",
            "IPPrefixLen": 16,
            "IPv6Gateway": "",
            "MacAddress": "02:42:ac:11:00:04",
            "Networks": {
                "bridge": {
                    "IPAMConfig": null,
                    "Links": null,
                    "Aliases": null,
                    "NetworkID": "edb61f382d1a1fb8d6e40e8756d5ae07fafb65e99869ed7feac3377f25a37bc1",
                    "EndpointID": "e1861e3fc4c92861dd349276395b38256ecddeb5cdc90c40d7e4340330c5c3e9",
                    "Gateway": "172.17.0.1",
                    "IPAddress": "172.17.0.4",
                    "IPPrefixLen": 16,
                    "IPv6Gateway": "",
                    "GlobalIPv6Address": "",
                    "GlobalIPv6PrefixLen": 0,
                    "MacAddress": "02:42:ac:11:00:04",
                    "DriverOpts": null
                }
            }
        }
    }
]
```
