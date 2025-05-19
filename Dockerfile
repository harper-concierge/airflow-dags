FROM quay.io/astronomer/astro-runtime:11.18.0

ENV AIRFLOW__LOGGING__LOGGING_LEVEL=DEBUG
ENV JAVA_HOME=/usr/lib/jvm/default-java
