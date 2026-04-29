ARG SPARK_BASE_IMAGE=apache/spark:3.5.8-scala2.12-java11-python3-r-ubuntu
FROM ${SPARK_BASE_IMAGE}

# ── Root: install deps and copy artifacts ────────────────────────────────────
USER root

ENV PATH="/opt/spark/bin:${PATH}"
# /opt/project is the mount-point for ./spark (volume in compose).
# Adding it to PYTHONPATH lets workers import spark.core / spark.pipeline etc.
# without needing an editable pip install.
ENV PYTHONPATH="/opt/project:${PYTHONPATH}"

# Copy Spark job modules into the image so the container works even when the
# host volume is not mounted (e.g. CI, standalone testing).
# In local dev the compose volume ./spark:/opt/project/spark shadows this.
COPY spark/ /opt/project/spark/

# Copy pre-downloaded JARs into Spark's default jar directory so spark-submit
# picks them up automatically without --jars flags.
# Required: hadoop-aws, aws-java-sdk-bundle, postgresql JDBC
COPY jars_spark/*.jar /opt/spark/jars/

# Fix permissions for the spark user (UID 185 in the base image)
RUN chmod -R 755 /opt/project/spark /opt/spark/jars

# ── Drop back to unprivileged spark user ─────────────────────────────────────
USER spark