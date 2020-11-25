#!/bin/bash

#hdfs dfs -touchz /user/${HADOOP_USER_NAME}/start_oozie_${HADOOP_USER_NAME}

mkdir -p /tmp/${HADOOP_USER_NAME}
wget https://raw.githubusercontent.com/project303/dataset/master/employee.csv -P /tmp/${HADOOP_USER_NAME}
#wget https://raw.githubusercontent.com/project303/dataset/master/create_table-user01.hql -O /tmp/user01/create_table-user01.hql
if [ -f /tmp/${HADOOP_USER_NAME}/employee.csv ]; then
#    hdfs dfs -touchz /user/${HADOOP_USER_NAME}/done_oozie_${HADOOP_USER_NAME}
    hdfs dfs -mkdir -p /user/${HADOOP_USER_NAME}/data/oozie_data
    hdfs dfs -put /tmp/${HADOOP_USER_NAME}/employee.csv  /user/${HADOOP_USER_NAME}/data/oozie_data
    rm /tmp/${HADOOP_USER_NAME}/employee.csv
else
    hdfs dfs -touchz /user/${HADOOP_USER_NAME}/fail_load_data_${HADOOP_USER_NAME}
#    hdfs dfs -mkdir -p script/file-ga-ada
fi
