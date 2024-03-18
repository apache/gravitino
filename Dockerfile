FROM micr.cloud.mioffice.cn/devops-public/maven-mi-repo:3-jdk-8-slim as builder
ARG BUILD_CLUSTER
RUN echo "Build cluster is: $BUILD_CLUSTER"

WORKDIR /opt
COPY . ./

RUN ./gradlew clean compileDistribution -Pcluster=$BUILD_CLUSTER -x test -g /root/.gradle/caches/$CI_COMMIT_BRANCH/

FROM micr.cloud.mioffice.cn/container/xiaomi_centos7:openjdk1.8
ENV TZ "Asia/Shanghai"

COPY --from=builder /opt/entrypoint.sh /opt/entrypoint.sh
COPY --from=builder /opt/distribution/package /opt/gravitino
ADD https://pkgs.d.xiaomi.net/artifactory/aliyun-maven-central/mysql/mysql-connector-java/5.1.49/mysql-connector-java-5.1.49.jar /opt/gravitino/libs/mysql-connector-java-5.1.49.jar

RUN chmod +x /opt/entrypoint.sh
RUN chmod -R +x /opt/gravitino

ENTRYPOINT ["/opt/entrypoint.sh"]