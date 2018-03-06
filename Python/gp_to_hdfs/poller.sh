#!/bin/bash

schemaname=$1
loadtype=$2

count=`ps aux|grep "updateDriver.py $schemaname $loadtype" | wc -l`

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
                nohup /usr/bin/python2.7 /apps/gp2hdp_sync/updateDriver.py $schemaname $loadtype >> /apps/gp2hdp_sync/logs/$schemaname-$(date +\%Y-\%m-\%d).log 2>&1 &
                printf "\n===================================\n"
fi
