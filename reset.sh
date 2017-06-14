#!/bin/bash

rm /Volumes/USB1/.cur_hdfs.xml
rm /Volumes/USB1/.last_sync.xml

hadoop fs -rm -r /cs219
hadoop fs -rm /.cur_hdfs.xml
hadoop fs -rm /.last_sync.xml
hadoop fs -rm -r /class

rm /Volumes/USB2/.cur_hdfs.xml
rm /Volumes/USB2/.last_sync.xml


rm -r /Volumes/USB2/school

rm -r /Volumes/USB2/class


rm /Volumes/USB2/exam.txt
rm /Volumes/USB2/homework.txt
