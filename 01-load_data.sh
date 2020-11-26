#!/bin/bash

mkdir -p /tmp/${HADOOP_USER_NAME}
rm -f /tmp/${HADOOP_USER_NAME}/employee.csv
wget https://raw.githubusercontent.com/project303/dataset/master/employee.csv -P /tmp/${HADOOP_USER_NAME}

if [ -f /tmp/${HADOOP_USER_NAME}/employee.csv ]; then
    hdfs dfs -mkdir -p /user/${HADOOP_USER_NAME}/data/oozie_data
    hdfs dfs -put /tmp/${HADOOP_USER_NAME}/employee.csv  /user/${HADOOP_USER_NAME}/data/oozie_data
    rm /tmp/${HADOOP_USER_NAME}/employee.csv
else
    hdfs dfs -touchz /user/${HADOOP_USER_NAME}/fail_load_data_${HADOOP_USER_NAME}
fi
