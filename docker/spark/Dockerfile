ARG SPARK_BASE_IMAGE=apache/spark:3.5.8-scala2.12-java11-python3-r-ubuntu
FROM ${SPARK_BASE_IMAGE}

USER root

# Copy Spark jobs
COPY spark/jobs /opt/spark/jobs

# ✅ QUAN TRỌNG: chỉ cần copy vào jars mặc định của Spark
COPY jars_spark/*.jar /opt/spark/jars/

# Permissions (không cần jars-ext nữa)
RUN chmod -R 755 /opt/spark/jobs /opt/spark/jars || true

USER spark