FROM tabulario/spark-iceberg

ARG LIVY_VERSION=0.8.0-incubating
ARG LIVY_SCALA=2.12

ENV SPARK_HOME=/opt/spark
ENV LIVY_HOME=/opt/livy
ENV LIVY_CONF_DIR=/opt/livy/conf

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends curl unzip; \
    rm -rf /var/lib/apt/lists/*; \
    curl -fL "https://archive.apache.org/dist/incubator/livy/${LIVY_VERSION}/apache-livy-${LIVY_VERSION}_${LIVY_SCALA}-bin.zip" -o /tmp/livy.zip; \
    unzip /tmp/livy.zip -d /opt; \
    mv "/opt/apache-livy-${LIVY_VERSION}_${LIVY_SCALA}-bin" "${LIVY_HOME}"; \
    mkdir -p "${LIVY_HOME}/logs" "${LIVY_HOME}/tmp"; \
    chmod -R 777 "${LIVY_HOME}/logs" "${LIVY_HOME}/tmp"; \
    rm -f /tmp/livy.zip

COPY livy.conf /opt/livy/conf/livy.conf
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

EXPOSE 8998

CMD ["/opt/livy/bin/livy-server"]
