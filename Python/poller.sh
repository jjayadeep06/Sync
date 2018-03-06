#!/bin/bash

schemaname=$1
loadtype=$2
datapath=$3

count=`ps aux|grep "updateDriver.py $schemaname $loadtype $datapath" | wc -l`

if [ $count -gt 1 ]
then
        printf "\nNumber of running sessions : "
        printf $count
        printf "\nSession running already\n"
        echo `date`
        printf "\n===================================\n"
else
                printf "\nNo Session Running.... Spinning of new session\n"
                echo `date`
                nohup /usr/bin/python2.7 /data/analytics/datasync/scripts/datasync_driver.py $schemaname $loadtype $datapath 1 >> /data/analytics/datasync/logs/$schemaname-$(date +\%Y-\%m-\%d).log 2>&1 &
                printf "\n===================================\n"
fi
