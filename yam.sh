#!/bin/bash

cd `dirname "$0"`

DATE=`date +%Y%m%d`
#DATE="20200715"
DIR="yam"

/opt/conda/bin/python3.7 yam.py /home/loader/_data $DATE &> yam.log

hdfs dfs -mkdir /user/loader/raw/${DIR}
hdfs dfs -put /home/loader/_data/yam_logs/* /user/loader/raw/${DIR}/

rm -rf /home/loader/_data/yam_logs/*
