FROM apache/hive:4.0.0

USER root

# COPY pre-downloaded JARs (build context = ./hive, nên path là drivers/)
COPY drivers/ /opt/hive/lib/

ENV HIVE_AUX_JARS_PATH=/opt/hive/lib/postgresql-42.7.1.jar:/opt/hive/lib/hadoop-aws-3.3.4.jar:/opt/hive/lib/aws-java-sdk-bundle-1.12.262.jar
