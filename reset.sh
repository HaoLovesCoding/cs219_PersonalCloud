#!/bin/bash

rm /Volumes/USB1/.cur_hdfs.xml
rm /Volumes/USB1/.last_sync.xml

hadoop fs -rm /.cur_hdfs.xml
hadoop fs -rm /.last_sync.xml
hadoop fs -rm -r /cs219

rm /Volumes/USB2/.cur_hdfs.xml
rm /Volumes/USB2/.last_sync.xml





