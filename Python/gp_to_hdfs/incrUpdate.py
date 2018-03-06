import psycopg2
import ConfigParser
import os
import base64
import sys
import subprocess
from psycopg2.extras import RealDictCursor
from itertools import chain
from readCfg import read_config
import pandas as pd
import traceback
import datetime
import time
from datetime import datetime
import glob
import jaydebeapi
from jaydebeapi import _DEFAULT_CONVERTERS, _java_to_py
_DEFAULT_CONVERTERS.update({'BIGINT': _java_to_py('longValue')})
import smtplib
from smtplib import SMTPException
import textwrap
sys.path.append("/apps/common/")
from auditLog import audit_logging
#****************************************************************************************************************************************#
# Name                                          | Date              | Version                      | Comments                                            #
#****************************************************************************************************************************************#
# Jayadeep, Sanjeev, Mayukh     | 09-SEP-2017       | 1.0                          | Initial Implementation                              #
#****************************************************************************************************************************************#

def dbConnect (db_parm, username_parm, host_parm, pw_parm):
    # Parse in connection information
    credentials = {'host': host_parm, 'database': db_parm, 'user': username_parm, 'password': pw_parm}
    conn = psycopg2.connect(**credentials)
    conn.autocommit = True  # auto-commit each entry to the database
    print "Connected Successfully"
    conn.cursor_factory = RealDictCursor
    cur = conn.cursor()
    return conn, cur

def dbConnectHiveBeeline(server_parm, classpath_parm):
    # Parse in connection information
    try:
        classpath  = os.environ["CLASSPATH"]
        classpath   = classpath + classpath_parm
        os.environ["CLASSPATH"] = classpath
    except KeyError:
        classpath   = classpath_parm
        os.environ["CLASSPATH"] = classpath
    url         = ("jdbc:hive2://" + server_parm )
    conn        = jaydebeapi.connect("org.apache.hive.jdbc.HiveDriver",url)
    print "Connected Successfully to Hive"
    conn.cursor_factory = RealDictCursor
    cur = conn.cursor()
    return conn, cur

def dbConnectHive (host_parm, port_parm, auth_parm):
    # Parse in connection information
    credentials = {'host': host_parm, 'port': port_parm, 'authMechanism': auth_parm}
    conn = pyhs2.connect(**credentials)
    print "Connected Successfully to Hive"
    conn.cursor_factory = RealDictCursor
    cur = conn.cursor()
    return conn, cur

def txn_dbConnect (db_parm, username_parm, host_parm, pw_parm):
    # Parse in connection information
    credentials = {'host': host_parm, 'database': db_parm, 'user': username_parm, 'password': pw_parm}
    conn = psycopg2.connect(**credentials)
    conn.autocommit = False  # auto-commit each entry to the database
    print "Connected Successfully"
    conn.cursor_factory = RealDictCursor
    cur = conn.cursor()
    return conn, cur

def call_spark_submit(sparkVersion,executorMemory,executorCores,driverMemory,loadScript,source_schemaname,source_tablename,load_id,run_id,data_path,v_timestamp):
    spark_cmd      = "spark-submit"
    master         = "--master"
    yarn           = "yarn"
    deploy_mode    = "--deploy-mode"
    client         = "client"
    executor_mem   = "--executor-memory"
    executor_cores = "--executor-cores"
    driver_mem     = "--driver-memory"
    #data_path      = "HDFS2MIR"
    load_table     =  source_schemaname + "." + source_tablename.replace('$','_')
    timestamp      = v_timestamp
    #Setting spark version before submitting job
    os.environ["SPARK_MAJOR_VERSION"] = sparkVersion
    #Preparing Spark submit string and submitting
    print spark_cmd, master, yarn, deploy_mode, client, executor_mem, executorMemory, executor_cores, executorCores, driver_mem,driverMemory,loadScript,load_table,load_id,run_id, data_path, timestamp
    subprocess.call([ spark_cmd,master,yarn, deploy_mode,client, executor_mem, executorMemory, executor_cores, executorCores, driver_mem,driverMemory,loadScript,load_table,str(load_id),str(run_id),data_path,timestamp])


def count_validate(cur,source_schemaname,source_tablename,load_type):
    print "inside validate method"
    if load_type == 'FULL':
        ext_table_query        = "SELECT count(*) FROM " + source_schemaname + "." + source_tablename + "_ext"
        mgd_table_query        = "SELECT count(*) FROM " + source_schemaname + "." + source_tablename
        print ext_table_query
        print mgd_table_query
        ext_count = dbQuery(cur,ext_table_query)
        mgd_count = dbQuery(cur,mgd_table_query)
        if ext_count == mgd_count:
            print "counts match table load successful"
        else:
            raise Exception("Count Mismatch:External and final table counts do not match")
            return

def remove_files(paths,source_schemaname,source_tablename):
    kinit_cmd  = "/usr/bin/kinit"
    kinit_args = "-kt"
    keytab     = "/home/gpadmin/gphdfs.service.keytab"
    realm      = "gpadmin@TRANSPORTATION-HDPPROD.GE.COM"
    try:
        java_home  = os.environ["JAVA_HOME"]
    except KeyError:
        java_home  = "/usr/lib/jvm/java"
    path       = paths + "/" + source_schemaname + "/" + source_tablename.replace('$','_') + "_ext/*"
    print path
    try:
        os.environ["JAVA_HOME"] = java_home
        subprocess.call([kinit_cmd, kinit_args, keytab, realm])
        subprocess.call(["hadoop", "fs", "-rm", "-r", path])
    except OSError:
            pass

def dbQuery (cur, query):
    cur.execute(query)
    rows = cur.fetchall()
    return rows

def sendMail(sender_parm, receivers_parm,message,error_table_list,load_id_parm):
    sender          = sender_parm
    receivers       = receivers_parm.split(',')
    message         = textwrap.dedent("""\
From: %s
To: %s
Subject: PROD : Greenplum to HDFS DataSync

%s : %s

Load ID : %s

Best Regards,
HWX EDGE Node""" %(sender_parm, receivers_parm,message,error_table_list,load_id_parm))
    try:
       smtpObj = smtplib.SMTP('localhost')
       smtpObj.sendmail(sender, receivers, message)
       print "Successfully sent email"
    except SMTPException:
       print "Error: unable to send email"


plant_name  = 'DATASYNC'
system_name = 'predix_pan_ins'
job_name    = 'Panel Insights'
data_path   = 'GPDB-->Predix'
technology  = 'Python'
num_errors  = 0
rows_inserted = 0
rows_updated = 0
rows_deleted = 0

def merge_data (table):

    t=datetime.fromtimestamp(time.time())
    v_timestamp = str(t.strftime('%Y-%m-%d %H:%M:%S'))

    config = read_config(['/apps/gp2hdp_sync/environ.properties'])
    if(config == None):
        return
        # Send Email
    print table
    table_name = table
    input_schema_name, input_table_name, load_id = table_name.split('.')

    run_id_sql  = "select nextval('sbdt.edl_run_id_seq')"
    plant_name  = 'DATASYNC'
    system_name = 'GPDB'
    job_name    = 'GPDB-->HWX'
    tablename   = input_schema_name + "." + input_table_name
    data_path   = 'GP2HDFS'
    technology  = 'Python'
    num_errors  = 1
    rows_inserted = 0
    rows_updated = 0
    rows_deleted = 0

    # get the current branch (from local.properties)
    env             = config.get('branch','env')

    # proceed to point everything at the 'branched' resources
    metastore_dbName           = config.get(env+'.meta_db','dbName')
    dbmeta_Url                 = config.get(env+'.meta_db','dbUrl')
    dbmeta_User                = config.get(env+'.meta_db','dbUser')
    dbmeta_Pwd                 = base64.b64decode(config.get(env+'.meta_db','dbPwd'))
    src_dbName                 = config.get(env+'.src_db','dbName')
    dbsrc_Url                  = config.get(env+'.src_db','dbUrl')
    dbsrc_User                 = config.get(env+'.src_db','dbUser')
    dbsrc_Pwd                  = base64.b64decode(config.get(env+'.src_db','dbPwd'))
    dbtgt_Url                  = config.get(env+'.tgt_db_beeline','dbUrl')
#    dbtgt_Port                 = config.get(env+'.tgt_db','dbPort')
#    dbtgt_Auth                 = config.get(env+'.tgt_db','authMech')
    dbtgt_classpath            = config.get(env+'.tgt_db_beeline','classPath')
    emailSender                = config.get(env+'.email','sender')
    emailReceiver              = config.get(env+'.email','receivers')
    executorMemory             = config.get(env+'.spark_params','executorMemory')
    executorCores              = config.get(env+'.spark_params','executorCores')
    driverMemory               = config.get(env+'.spark_params','driverMemory')
    loadScript                 = config.get(env+'.spark_params','loadScript')
    sparkVersion               = config.get(env+'.spark_params','sparkVersion')
    paths                      = "/apps/staging/"
    t=datetime.fromtimestamp(time.time())
    v_timestamp = str(t.strftime('%Y-%m-%d %H:%M:%S'))

    print metastore_dbName, dbmeta_Url, dbmeta_User, dbmeta_Pwd
    try:
        conn_metadata, cur_metadata    = dbConnect(metastore_dbName, dbmeta_User, dbmeta_Url, dbmeta_Pwd)
        run_id_lists                   = dbQuery(cur_metadata,run_id_sql)
        run_id_list                    = run_id_lists[0]
        run_id                         = run_id_list['nextval']
        print "Run ID for the table ", table_name, " is: ", run_id
    except Exception as e:
        err_msg = "Error connecting to database while fetching  metadata"
        sendMail(emailSender,emailReceiver,err_msg)
        return

    #Audit entry at start of job
    err_msg = ''
    err_msg = ''
    status = 'Job Started'
    output_msg = ''
    audit_logging(cur_metadata, load_id,run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)

    try:
        metadata_sql    = "SELECT * FROM sync.control_table WHERE source_tablename = '" + input_table_name + "' AND source_schemaname = '" + input_schema_name +"' AND data_path = 'GP2HDFS' "
        print metadata_sql
        controls        = dbQuery(cur_metadata, metadata_sql)
        print controls
    except psycopg2.Error as e:
        err_msg = "Errror "
        status = 'Job Error'
        output_msg = traceback.format_exc()
        audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
        return
    if not controls:
        err_msg = "No Entry found in metadata table"
        error = 2
        err_msg = "Errror "
        status = 'Job Error'
        output_msg = "No Entry found in metadata table"
        audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
        return
    for control in range(len(controls)):
        id                                = str(controls[control]['id'])
        source_schemaname                 = str(controls[control]['source_schemaname'])
        source_tablename                  = str(controls[control]['source_tablename'])
        target_schema                     = str(controls[control]['target_schemaname'])
        target_tablename                  = str(controls[control]['target_tablename'])
        load_type                         = str(controls[control]['load_type'])
        incremental_column                = str(controls[control]['incremental_column'])
        last_run_time                     = str(controls[control]['last_run_time'])
        join_columns                      = str(controls[control]['join_columns'])
        log_mode                          = str(controls[control]['log_mode'])
        data_path                         = str(controls[control]['data_path'])
    try:
        conn_source, cur_source      = dbConnect(src_dbName, dbsrc_User, dbsrc_Url, dbsrc_Pwd)
        if log_mode == 'DEBUG':
            status  = "Connected to Source database"
            audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
    except psycopg2.Error as e:
        err_msg = "Error connecting to source database"
        status = 'Job Error'
        output_msg = traceback.format_exc()
        audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
        #continue
        return

    try:
        drop_src_table   = "DROP EXTERNAL TABLE IF EXISTS " + source_schemaname + "." + source_tablename + "_wext"
        print drop_src_table
        cur_source.execute(drop_src_table)
        if log_mode == 'DEBUG':
            status  = "Dropped Writable External Table on Source"
            audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
    except psycopg2.Error as e:
        err_msg = "Error while dropping Writable External table in source"
        status = 'Job Error'
        output_msg = traceback.format_exc()
        audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
        conn_metadata.close()
        #continue
        return

    try:
        create_writable_sql = "CREATE WRITABLE EXTERNAL TABLE " + source_schemaname + "." + source_tablename + "_wext (LIKE " + source_schemaname + "." + source_tablename + ") LOCATION('gphdfs://getnamenode:8020/apps/staging/" + source_schemaname + "/" + source_tablename.replace('$','_') + "_ext') FORMAT 'TEXT' (DELIMITER E'\x01' NULL '')"
        print create_writable_sql
        cur_source.execute(create_writable_sql)
        if log_mode == 'DEBUG':
            status  = "Created Writable External Table on Source"
            audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)

    except psycopg2.Error as e:
        err_msg = "Error while creating Writable External table in source"
        status = 'Job Error'
        output_msg = traceback.format_exc()
        audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
    #    continue
        return

    try:
        hive_upd_by_sql    = "ALTER EXTERNAL TABLE " + source_schemaname + "." + source_tablename + "_wext ADD COLUMN hive_updated_by text"
        print hive_upd_by_sql
        cur_source.execute(hive_upd_by_sql)
        if log_mode == 'DEBUG':
            status  = "Added column hive_updated_by to Writable External Table"
            audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)

    except psycopg2.Error as e:
        err_msg = "Error while adding column hive_updated_by to Writable External Table"
        status = 'Job Error'
        output_msg = traceback.format_exc()
        audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
    #    continue
        return

    try:
        hive_upd_date_sql    = "ALTER EXTERNAL TABLE " + source_schemaname + "." + source_tablename + "_wext ADD COLUMN hive_updated_date timestamp without time zone"
        print hive_upd_date_sql
        cur_source.execute(hive_upd_date_sql)
        if log_mode == 'DEBUG':
            status  = "Added column hive_updated_date to Writable External Table"
            audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)

    except psycopg2.Error as e:
        err_msg = "Error while adding column hive_updated_date to Writable External Table"
        status = 'Job Error'
        output_msg = traceback.format_exc()
        audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
    #    continue
        return
    print load_type
    if load_type == 'FULL' :
        incremental_sql = "INSERT INTO " + source_schemaname + "." + source_tablename + "_wext" + " SELECT *, 'datasync', now()  FROM " + source_schemaname + "." + source_tablename
    elif load_type == 'INCREMENTAL':
        incremental_sql = "INSERT INTO " + source_schemaname + "." + source_tablename + "_wext" + " SELECT *, 'datasync', now() FROM " + source_schemaname + "." + source_tablename + " WHERE " + incremental_column + " > '"  + last_run_time + "' AND " + incremental_column + " <= '" + v_timestamp + "'"
    else:
        incremental_sql = "INSERT INTO " + source_schemaname + "." + source_tablename + "_wext" + " SELECT *, 'datasync', now() FROM " + source_schemaname + "." + source_tablename + " WHERE " + incremental_column + " > '"  + last_run_time + "' AND " + incremental_column + " <= '" + v_timestamp + "' AND hvr_is_deleted = 0"

    try:
        remove_files(paths,source_schemaname,source_tablename)
        if log_mode == 'DEBUG':
            status  = "Removed files from HDFS before dumping data from GPDB"
            audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)

        print incremental_sql
        cur_source.execute(incremental_sql)
        rows_inserted  = cur_source.rowcount
        if log_mode == 'DEBUG':
            status  = "Dumped data into HDFS through Writable External Table"
            audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)

        print "Rows inserted into Writable External Table : ", rows_inserted
    except psycopg2.Error as e:
        err_msg = "Error while inserting data into Writable External table in source"
        status = 'Job Error'
        remove_files(paths,source_schemaname,source_tablename)
        output_msg = traceback.format_exc()
        audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
        conn_metadata.close()
#                continue
        return

    try:
        drop_src_table   = "DROP EXTERNAL TABLE IF EXISTS " + source_schemaname + "." + source_tablename + "_wext"
        print drop_src_table
        cur_source.execute(drop_src_table)
        if log_mode == 'DEBUG':
            status  = "Dropped Writable External Table on Source"
            audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)

    except psycopg2.Error as e:
        err_msg = "Error while dropping Writable External table in source"
        status = 'Job Error'
        remove_files(paths,source_schemaname,source_tablename)
        output_msg = traceback.format_exc()
        audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
        conn_metadata.close()
        #continue
        return

    try:
        status     = "Finished GPDB part"
        num_errors = 0
        err_msg    = "No Errors"
        output_msg = "No Errors"
        audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
    except psycopg2.Error as e:
        err_msg = "Error while updating Log Table after GPDB INSERT/DROP"
        status = 'Job Error'
        remove_files(paths,source_schemaname,source_tablename)
        output_msg = traceback.format_exc()
        audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
        conn_target.rollback()
        conn_target.close()
        conn_metadata.close()
        return

#HIVE
    print "HIVE............"

    try:
            conn_target, cur_target   = dbConnectHiveBeeline(dbtgt_Url,dbtgt_classpath)
            if log_mode == 'DEBUG':
                status  = "Connected to Target Database"
                audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)

    except Exception as e:
        err_msg      = "Error while connecting to target database"
        status       = 'Job Error'
        remove_files(paths,source_schemaname,source_tablename)
        print e
        output_msg   = e
        audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
        return

    hive_columns_sql = "SELECT ARRAY_TO_STRING(ARRAY(SELECT '`' || COLUMN_NAME||'` '||(case when data_type in('numeric') and numeric_scale > 0 then 'double' when data_type in('numeric') and (numeric_scale=0 or numeric_scale is null) then 'bigint' when data_type in('character varying','character','text') then 'string' when data_type in('timestamp without time zone') then 'timestamp' when data_type in('bigint') then 'bigint' when data_type in('integer') then 'int' when data_type in('smallint') then 'smallint' when data_type in('date') then 'date' when data_type in('name') then 'string' when data_type in ('ARRAY') then 'array<string>' else 'string' end) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='" + source_tablename + "' and table_schema='" + source_schemaname + "' ORDER BY ORDINAL_POSITION), ', ')"
    try:
        columns      = dbQuery(cur_source,hive_columns_sql)
        if log_mode == 'DEBUG':
            status  = "Got column list from GPDB with data type conversion"
            audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)

    except psycopg2.Error as e:
        err_msg      = "Error while getting column list with data type for target table from GPDB"
        status       = 'Job Error'
        remove_files(paths,source_schemaname,source_tablename)
        output_msg   = traceback.format_exc()
        audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
        return

    column_list_orig                = ",".join(d['array_to_string'] for d in columns)
    column_list                     = column_list_orig + ",`hive_updated_by` string, `hive_updated_date` timestamp "
    print column_list

    drop_hive_ext_table             = "DROP TABLE IF EXISTS `" + source_schemaname + "." + source_tablename.replace('$','_') + "_ext`"
    print drop_hive_ext_table
    try:
            cur_target.execute(drop_hive_ext_table)
            if log_mode == 'DEBUG':
                status  = "Dropped Hive External table"
                audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)

    except Exception as e:
        print e
        err_msg      = "Error while dropping Hive External table"
        status       = 'Job Error'
        remove_files(paths,source_schemaname,source_tablename)
        output_msg   = traceback.format_exc()
        audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
        return

    create_hive_ext_table           = "CREATE EXTERNAL TABLE `" + source_schemaname + "." + source_tablename.replace('$','_') + "_ext`(" + column_list.replace('$','_') + ") ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n' STORED AS TEXTFILE LOCATION '" + paths + source_schemaname + "/" + source_tablename.replace('$','_') + "_ext' "
    print       create_hive_ext_table

    try:
            cur_target.execute(create_hive_ext_table)
            if log_mode == 'DEBUG':
                status  = "Created Hive External Table"
                audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)

    except Exception as e:
        err_msg      = "Error while creating Hive External table"
        status       = 'Job Error'
        remove_files(paths,source_schemaname,source_tablename)
        output_msg   = traceback.format_exc()
        audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
        return

    if load_type == 'FULL':
        drop_hive_mngd_table        = "DROP TABLE IF EXISTS `" + source_schemaname + "." + source_tablename.replace('$','_') + "`"
        print drop_hive_mngd_table
        try:
                cur_target.execute(drop_hive_mngd_table)
                if log_mode == 'DEBUG':
                    status  = "Dropped Managed Table in Hive for Full Load"
                    audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)

        except Exception as e:
            err_msg      = "Error while dropping Hive Managed table"
            status       = 'Job Error'
            remove_files(paths,source_schemaname,source_tablename)
            output_msg   = traceback.format_exc()
            audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            return


        create_hive_mngd_table      = "CREATE EXTERNAL TABLE `" + source_schemaname + "." + source_tablename.replace('$','_') + "`(" + column_list.replace('$','_') + ") STORED AS ORC LOCATION '/apps/hive/warehouse/" + source_schemaname + ".db/" + source_tablename.replace('$','_') + "' TBLPROPERTIES ('orc.compress'='ZLIB')"
        print create_hive_mngd_table
        try:
                cur_target.execute(create_hive_mngd_table)
                if log_mode == 'DEBUG':
                    status  = "Created Table in Hive for Full Load"
                    audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)

        except Exception as e:
            err_msg      = "Error while Creating Hive External table"
            status       = 'Job Error'
            remove_files(paths,source_schemaname,source_tablename)
            output_msg   = traceback.format_exc()
            audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            return

    try:
        print "before call of spark submit"
        data_path = "HDFS2MIR"
        call_spark_submit(sparkVersion,executorMemory,executorCores,driverMemory,loadScript,source_schemaname,source_tablename,load_id,run_id,data_path,v_timestamp)
        if log_mode == 'DEBUG':
            status  = "Started Spark Job"
            audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
    except Exception as e:
        err_msg      = "Error while running spark submit"
        status       = 'Job Error'
        remove_files(paths,source_schemaname,source_tablename)
        output_msg   = traceback.format_exc()
        audit_logging(cur_metadata, load_id,run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
        return

#    try:
#        #count_validate(cur_target,source_schemaname,source_tablename,load_type)
#        if load_type == 'FULL':
#            count_sql        = "SELECT count(*) FROM " + source_schemaname + "." + source_tablename
#            print count_sql
#            if log_mode == 'DEBUG':
#                status  = "Starting count validation of Source and Target"
#                audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
#            source_count = dbQuery(cur_source,count_sql)
#            target_count = dbQuery(cur_target,count_sql)
#            if (source_count[0])['count'] == target_count[0][0]:
#                print "counts match table load successful"
#                if log_mode == 'DEBUG':
#                    status  = "Counts validation successfull"
#                    audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
#            else:
#                raise Exception("Count Mismatch:External and final table counts do not match")
#        else:
#            count_sql = "SELECT count(*) as count FROM " + source_schemaname + "." + source_tablename + " WHERE " + incremental_column + "> '" + last_run_time + "'"
#            print count_sql
#            if log_mode == 'DEBUG':
#                status  = "Starting count validation of Source and Target"
#                audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
#            source_count = dbQuery(cur_source,count_sql)
#            target_count = dbQuery(cur_target,count_sql)
#            if (source_count[0])['count'] == target_count[0][0]:
#                print "counts match table load successful"
#                if log_mode == 'DEBUG':
#                    status  = "Counts validation successfull"
#                    audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
#            else:
#                raise Exception("Count Mismatch:External and final table counts do not match")
#
#    except Exception as e:
#        err_msg      = "Error while validating counts of table"
#        status       = 'Job Error'
#        remove_files(paths,source_schemaname,source_tablename)
#        output_msg   = traceback.format_exc()
#        audit_logging(cur_metadata, load_id,run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
#        return
#    update_control_table_sql = "UPDATE sync.control_table SET last_run_time = '" + v_timestamp +"', status_flag = 'Y' WHERE source_schemaname = '" + input_schema_name + "' AND source_tablename = '" + input_table_name + "' AND id = " + id
#    print update_control_table_sql
#    try:
#        cur_metadata.execute(update_control_table_sql)
#        if log_mode == 'DEBUG':
#            status  = "Updated Control Table"
#            audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
#    except psycopg2.Error as e:
#        err_msg      = "Error while updating the control table"
#        status       = 'Job Error'
#        remove_files(paths,source_schemaname,source_tablename)
#        output_msg   = traceback.format_exc()
#        audit_logging(cur_metadata, load_id,run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
#        return

#    try:
#        err_msg    = 'No Errors'
#        status     = 'Job Finished'
#        output_msg = 'Job Finished successfully'
#        remove_files(paths,source_schemaname,source_tablename)
#        audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
#    except psycopg2.Error as e:
#        err_msg    = 'Error while updating log table for Job Finshed entry'
#        status     = 'Job Error'
#        remove_files(paths,source_schemaname,source_tablename)
#        output_msg = traceback.format_exc()
#        audit_logging(cur_metadata, load_id,run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)


#    conn_metadata.close()




if __name__ == "__main__":
    merge_data('shop_supt.dmr_desc.78912')

