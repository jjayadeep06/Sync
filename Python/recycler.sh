#!/bin/bash
echo `date`
gpfdist=`ps aux|grep [g]pfdist|grep $1|wc -l`
full_load=`ps aux|grep "[d]atasync_driver.py $1 FULL" | wc -l`
incr_load=`ps aux|grep "[d]atasync_driver.py $1 INCREMENTAL" | wc -l`

if [ $gpfdist -gt 0 -a $full_load -gt 0 ] || [ $gpfdist -gt 0 -a $incr_load -gt 0 ]
then
            printf "\nLoads running... Dying....\n"
            printf "\n============================\n"
else
            printf "\nRecycling GPFDIST processes...\n"
            kill $(ps aux | grep [g]pfdist | grep hvr_staging | grep $1 | awk '{print $2}')
            source /usr/local/greenplum-loaders-4.3.8.1-build-1/greenplum_loaders_path.sh
            gpfdist -p $2 -d /data/staging/g00003/hvr_staging/$1 -l /data/staging/g00003/hvr_staging/$1.log -t 30 -m 268435456 &
            sleep 3s
            printf "\n============================\n"
fi

