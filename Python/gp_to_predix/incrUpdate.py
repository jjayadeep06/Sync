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

#****************************************************************************************************************************************#
# Name       | Date              | Version                      | Comments                                                               #
#****************************************************************************************************************************************#
# Jayadeep   | 17-JUL-2017       | 1.0                          | Initial Implementation                                                 #
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


def txn_dbConnect (db_parm, username_parm, host_parm, pw_parm):
    # Parse in connection information
    credentials = {'host': host_parm, 'database': db_parm, 'user': username_parm, 'password': pw_parm}
    conn = psycopg2.connect(**credentials)
    conn.autocommit = False  # auto-commit each entry to the database
    print "Connected Successfully"
    conn.cursor_factory = RealDictCursor
    cur = conn.cursor()
    return conn, cur

def remove_files(paths,table_name,source_gpfdist_processes):
    m = 0
    for path in paths:
        file_name = table_name
        for m in range(source_gpfdist_processes):
            abs_path  = path + file_name + "-" + str(m)
            file_list = glob.glob(abs_path + '*.dat')
            for file in file_list:
                try:
                    os.remove(file)
                except OSError:
                    pass
def dbQuery (cur, query):
    cur.execute(query)
    rows = cur.fetchall()
    return rows

def audit_logging (cur,run_id, plant_name, system_name , job_name, tablename, status, data_path, technology, rows_inserted, rows_updated, rows_deleted, number_of_errors, error_message ,source_row_count, target_row_count, error_category):
    try:
        #run_id = 'admin.datasync_seq'
        query = "select * from sbdt.edl_log(" + str(run_id) +",'"+ plant_name+"','"+ system_name +"','"+ job_name+"','"+ tablename+"','"+ status+"','"+ data_path+"','"+ technology+"',"+ str(rows_inserted) +"," \
                 + str(rows_updated) +","+ str(rows_deleted) +","+ str(number_of_errors) +",'"+ error_message +"',"+str(source_row_count)+","+ str(target_row_count)+",'"+ error_category.replace('"','').replace('\'','') + "')"
        print query
        cur.execute(query)
    except psycopg2.Error as e:
        err_msg = "Error connecting while inserting audit message"
        status = 'Job Error'
        rows_inserted = 0
        rows_updated = 0
        rows_deleted = 0
        try:
           output_msg = traceback.format_exc()
           print output_msg
           audit_logging(cur, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
        except KeyError as e:
           sql_state = e.pgcode
           audit_logging(cur,run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,sql_state)
           #continue

plant_name  = 'DATASYNC'
system_name = 'GPDB'
job_name    = 'GPDB-->GPDB'
data_path   = 'COMM2BUS'
technology  = 'Python'
num_errors  = 1
rows_inserted = 0
rows_updated = 0
rows_deleted = 0

def merge_data (table):

    config = read_config(['/pwcloud/app/gpsync/environ.properties'])
    if(config == None):
        return
        # Send Email
    print table
    table_name = table
    input_schema_name, input_table_name = table_name.split('.')

    run_id_sql  = "select nextval('admin.datasync_seq')"
    plant_name  = 'DATASYNC'
    system_name = 'GPDB'
    job_name    = 'GPDB-->GPDB'
    tablename   = table_name
    data_path   = 'COMM2BUS'
    technology  = 'Python'
    num_errors  = 1

    # get the current branch (from local.properties)
    env             = config.get('branch','env')

    # proceed to point everything at the 'branched' resources
    metastore_dbName           = config.get(env+'.meta_db','dbName')
    dbmeta_Url                 = config.get(env+'.meta_db','dbUrl')
    dbmeta_User                = config.get(env+'.meta_db','dbUser')
    dbmeta_Pwd                 = base64.b64decode(config.get(env+'.meta_db','dbPwd'))

    source_gpfdist_processes          = 20
    target_gpfdist_processes          = 20
    try:
        conn_metadata, cur_metadata    = dbConnect(metastore_dbName, dbmeta_User, dbmeta_Url, dbmeta_Pwd)
        run_id_lists                   = dbQuery(cur_metadata,run_id_sql)
        run_id_list                    = run_id_lists[0]
        run_id                         = run_id_list['nextval']

    except Exception as e:
        err_msg = "Error connecting to database while fetching  metadata"
        # Send Email
        print e
        return

    #Audit entry at start of job
    err_msg = ''
    err_msg = ''
    status = 'Job Started'
    output_msg = ''
    audit_logging(cur_metadata, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)

    # Get control table access
    try:
        metadata_sql    = "SELECT a.id, source_schema, source_tablename, target_schema, target_tablename, \
                           load_type, incremental_column, last_processed_value, join_columns, b.host as source_host, b.port as source_port, \
                           b.username as source_username, b.password as source_password, b.dbname as source_dbname, a.target_bu_id,c.host as target_host, c.port as target_port, \
                           c.username as target_username, c.password as target_password, c.dbname as target_dbname \
                           FROM admin.application_control a, admin.connection_control b, admin.connection_control c WHERE a.source_bu_id = b.id and a.target_bu_id = c.id and source_schema = " \
                          + "'" + input_schema_name + "'" + " and source_tablename = " + "'"   \
                          + input_table_name + "'"
        controls        = dbQuery(cur_metadata, metadata_sql)
    except psycopg2.Error as e:
        err_msg = "Errror "
        status = 'Job Error'
        output_msg = traceback.format_exc()
        audit_logging(cur_metadata, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
        return

    print controls

    if not controls:
        err_msg = "No Entry found in metadata table"
        error = 2
        err_msg = "Errror "
        status = 'Job Error'
        output_msg = "No Entry found in metadata table"
        audit_logging(cur_metadata, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
        return

    prev_load_type = ''

    for control in range(len(controls)):
        source_row_count                  = 0
        target_row_count                  = 0
        id                                = str(controls[control]['id'])
        source_schema                     = str(controls[control]['source_schema'])
        source_tablename                  = str(controls[control]['source_tablename'])
        target_schema                     = str(controls[control]['target_schema'])
        target_tablename                  = str(controls[control]['target_tablename'])
        load_type                         = str(controls[control]['load_type'])
        incremental_column                = str(controls[control]['incremental_column'])
        last_processed_value              = str(controls[control]['last_processed_value'])
        join_columns                      = str(controls[control]['join_columns'])
        src_dbName                        = str(controls[control]['source_dbname'])
        dbsrc_Url                         = str(controls[control]['source_host'])
        dbsrc_User                        = str(controls[control]['source_username'])
        dbsrc_Pwd                         = str(controls[control]['source_password'])
        dbsrc_Port                        = str(controls[control]['source_port'])

        source_port_range                 = config.get(env+'.src_db','portRange')
        source_gpfdist_host               = config.get(env+'.src_db','gpfdistHost')

        tgt_dbName                        = str(controls[control]['target_dbname'])
        dbtgt_Url                         = str(controls[control]['target_host'])
        dbtgt_User                        = str(controls[control]['target_username'])
        dbtgt_Pwd                         = str(controls[control]['target_password'])
        dbsrc_Port                        = str(controls[control]['target_port'])

        target_port_range                 = config.get(env+'.tgt_db','portRange')
        target_gpfdist_host               = config.get(env+'.tgt_db','gpfdistHost')

        try:
           conn_target, cur_target      = txn_dbConnect(tgt_dbName, dbtgt_User, dbtgt_Url, dbtgt_Pwd)
        except psycopg2.Error as e:
            err_msg = "Error connecting to Target database"
            status = 'Job Error'
            output_msg = traceback.format_exc()
            audit_logging(cur_metadata, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            conn_metadata.close()
            continue
        data_paths = config.get(env+'.misc','dataPath')
        paths = list (data_paths.split(','))

        if prev_load_type <> load_type:

            try:
                conn_source, cur_source      = dbConnect(src_dbName, dbsrc_User, dbsrc_Url, dbsrc_Pwd)
            except psycopg2.Error as e:
                err_msg = "Error connecting to source database"
                status = 'Job Error'
                output_msg = traceback.format_exc()
                audit_logging(cur_metadata, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
                continue

            try:
                column_list_sql = "SELECT attrelid::regclass, attnum, attname,format_type(a.atttypid, a.atttypmod) AS data_type FROM pg_attribute a WHERE  attrelid = " \
                                  + "'" + source_schema + "." + source_tablename + "'" + "::regclass"              \
                                  " AND attnum > 0 AND NOT attisdropped ORDER BY attnum"
                columns         = dbQuery(cur_source, column_list_sql)
            except psycopg2.Error as e:
                err_msg = "Error while getting column list from source table"
                status = 'Job Error'
                output_msg = traceback.format_exc()
                audit_logging(cur_metadata, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
                continue

            if not columns:
                err_msg = "Column list not found in pg_attribute system table "
                status = 'Job Error'
                output_msg = "Column list not found in pg_attribute system table "
                audit_logging(cur_metadata, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
                continue

            try:
                column_list_sql = "SELECT attrelid::regclass, attnum, attname,format_type(a.atttypid, a.atttypmod) AS data_type FROM pg_attribute a WHERE  attrelid = " \
                              + "'" + target_schema + "." + target_tablename + "'" + "::regclass"              \
                              " AND attnum > 0 AND NOT attisdropped ORDER BY attnum"
                target_columns         = dbQuery(cur_target, column_list_sql)
            except psycopg2.Error as e:
                err_msg = "Error while getting column list from target table"
                status = 'Job Error'
                remove_files(paths,table_name,source_gpfdist_processes)
                output_msg = traceback.format_exc()
                audit_logging(cur_metadata, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
                continue

            if not target_columns:
                err_msg = "Column list not found in pg_attribute system table "
                status = 'Job Error'
                remove_files(paths,table_name,source_gpfdist_processes)
                output_msg = "Column list not found in pg_attribute system table "
                audit_logging(cur_metadata, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
                continue

            select_list = '","'.join(d['attname'] for d in columns)
            select_list = '"' + select_list + '"'
            t=datetime.fromtimestamp(time.time())
            v_timestamp = str(t.strftime('%Y-%m-%d %H:%M:%S'))

            data_paths = config.get(env+'.misc','dataPath')
            paths = list (data_paths.split(','))

            remove_files(paths,table_name,source_gpfdist_processes)

            try:
               statement_mem_sql = "SET statement_mem to '500MB'"
               cur_source.execute(statement_mem_sql)
               cur_target.execute(statement_mem_sql)
            except psycopg2.Error as e:
                err_msg = "Error setting statement memory in source and target"
                status = 'Job Error'
                remove_files(paths,table_name,source_gpfdist_processes)
                output_msg = traceback.format_exc()
                audit_logging(cur_metadata, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
                continue

            try:
                drop_src_table   = "DROP EXTERNAL TABLE IF EXISTS " + source_schema + "." + source_tablename + "_wext"
                cur_source.execute(drop_src_table)
            except psycopg2.Error as e:
                err_msg = "Error while dropping writable external table in source"
                status = 'Job Error'
                remove_files(paths,table_name,source_gpfdist_processes)
                output_msg = traceback.format_exc()
                audit_logging(cur_metadata, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
                conn_metadata.close()
                continue

            try:
                create_writable_sql     = "CREATE WRITABLE EXTERNAL TABLE " + source_schema + "." + source_tablename  + "_wext (like " + source_schema + "." + source_tablename + ")" + " LOCATION ("
                a, b = source_port_range.split('-')
                start_port = int(a)
                end_port   = int(b)
                number     = 1
                file_counter = 1
                counter = 1
                host_range = list(source_gpfdist_host.split(','))
                for j in host_range:
                    for i in range(start_port,end_port):
                        if  number == 1:
                           create_writable_sql = create_writable_sql  + "'gpfdist://" + j + ":" + str(i) + "/" + table_name + "-" + str(counter%source_gpfdist_processes) + "-" + str(i) + ".dat"
                        else:
                           create_writable_sql = create_writable_sql  + "', 'gpfdist://" + j + ":" + str(i) + "/" + table_name + "-" +  str(counter%source_gpfdist_processes) + "-" + str(i) + ".dat"
                        number = number + 1
                        counter = counter + 1
                    final_port = i+1
                    create_writable_sql     = create_writable_sql  + "', 'gpfdist://" + j + ":" + str(final_port) + "/" + table_name + "-" + str(counter%source_gpfdist_processes) + "-" + str(final_port) + ".dat"
                create_writable_sql = create_writable_sql + "') FORMAT 'TEXT' (DELIMITER E'\x01') "
                print create_writable_sql
                cur_source.execute(create_writable_sql)
            except psycopg2.Error as e:
                err_msg = "Error while creating writable external table in source"
                status = 'Job Error'
                remove_files(paths,table_name,source_gpfdist_processes)
                output_msg = traceback.format_exc()
                audit_logging(cur_metadata, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
                continue

            if load_type == 'FULL' :
                incremental_sql = "INSERT INTO " + source_schema + "." + source_tablename + "_wext" + " SELECT " + select_list + " FROM " + source_schema + "." + source_tablename
            else:
                incremental_sql = "INSERT INTO " + source_schema + "." + source_tablename + "_wext" + " SELECT " + select_list + " FROM " + source_schema + "." + source_tablename \
                                  + " WHERE " + incremental_column + " > '"  + str(last_processed_value) + "' AND " + incremental_column + " <= '" + str(v_timestamp) + "'"
            print incremental_sql

            try:
                cur_source.execute(incremental_sql)
            except psycopg2.Error as e:
                err_msg = "Error while inserting data into writable external table in source"
                status = 'Job Error'
                #remove_files(paths,table_name,source_gpfdist_processes)
                output_msg = traceback.format_exc()
                audit_logging(cur_metadata, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
                conn_metadata.close()
                continue
            source_row_count = cur_source.rowcount
            prev_load_type = load_type

        try:
            conn_source.close()
        except:
            pass

        #paths = list (data_paths.split(','))

        file_not_found = 0
        for path in paths:
            file_name = table_name
            abs_path  = path + file_name
            file_check = glob.glob(abs_path + '-*.dat')
            if not file_check :
               file_not_found = file_not_found + 1

        if file_not_found > 0 :
            err_msg = "No data dumped from source @:" + abs_path
            status = 'Job Finished'
            conn_metadata, cur_metadata    = dbConnect(metastore_dbName, dbmeta_User, dbmeta_Url, dbmeta_Pwd)
            output_msg = "No data dumped from source @:" + abs_path
            audit_logging(cur_metadata, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            print "Finished Data"
            conn_metadata.close()
            continue

        try:
            drop_table   = "DROP EXTERNAL TABLE IF EXISTS " + target_schema + "." + target_tablename + "_ext"
            cur_target.execute(drop_table)
        except psycopg2.Error as e:
            err_msg = "Error while dropping external table in target"
            status = 'Job Error'
            remove_files(paths,table_name,source_gpfdist_processes)
            output_msg = traceback.format_exc()
            audit_logging(cur_metadata, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,source_row_count,0,output_msg)
            conn_target.rollback()
            conn_target.close()
            conn_metadata.close()
            continue

        print drop_table

        target_select_list = ', '.join(d['attname'] for d in target_columns)
        target_create_list = ',"'.join(d['attname']+'" '+d['data_type'] for d in target_columns)
        target_create_list = '"' + target_create_list

        target_insert_list = '","'.join(d['attname'] for d in target_columns)
        target_insert_list = '"' + target_insert_list + '"'

        source_create_list = ',"'.join(d['attname']+'" '+d['data_type'] for d in columns)
        source_create_list = '"' + source_create_list

        try:
            create_table_sql     = "CREATE EXTERNAL TABLE " + target_schema + "." + target_tablename  + "_ext ( " + source_create_list + " )" + " LOCATION ("
            a, b = target_port_range.split('-')
            tgt_start_port = int(a)
            tgt_end_port   = int(b) + 1
            number         = 1
            target_counter = 1
            tgt_host_range = list(target_gpfdist_host.split(','))
            for x in range(len(tgt_host_range)):
                for y in range(tgt_start_port,tgt_end_port):
                    check = data_paths + source_schema + "." + source_tablename + "-" + str(target_counter%target_gpfdist_processes)  + '-*.dat'
                    if  glob.glob(check):
                        if  number == 1:
                            create_table_sql = create_table_sql  + "'gpfdist://" + tgt_host_range[x] + ":" + str(y) + "/" + source_schema + "." + source_tablename + "-" + str(target_counter%target_gpfdist_processes) +  '-*.dat'
                        else:
                            create_table_sql = create_table_sql  + "', 'gpfdist://" + tgt_host_range[x] + ":" + str(y) + "/" + source_schema + "." + source_tablename + "-" + str(target_counter%target_gpfdist_processes) + '-*.dat'
                        number = number + 1
                    target_counter = target_counter + 1
            create_table_sql = create_table_sql + " ') FORMAT 'TEXT' (DELIMITER E'\x01') "
            cur_target.execute(create_table_sql)
        except psycopg2.Error as e:
            err_msg = "Error while creating external table in target"
            status = 'Job Error'
            remove_files(paths,table_name,source_gpfdist_processes)
            output_msg = traceback.format_exc()
            audit_logging(cur_metadata, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,source_row_count,0,output_msg)
            conn_target.rollback()
            conn_target.close()
            conn_metadata.close()
            continue

        print create_table_sql

        if load_type == 'FULL' :
            delete_sql   = "TRUNCATE TABLE " + target_schema + "." + target_tablename
        else:
            delete_sql   = "DELETE FROM "  + target_schema + "." + target_tablename + " a WHERE EXISTS (SELECT 1 FROM  " \
                   + target_schema + "." + target_tablename + "_ext b WHERE 1=1 "
            for i in join_columns.split(','):

                delete_sql = delete_sql + " and coalesce(cast(a."+ i + " as text),'99999999999999999') = " + "coalesce(cast(b." + i + " as text),'99999999999999999')"
            delete_sql = delete_sql + ")"
            print delete_sql
        try:
            cur_target.execute(delete_sql)
        except psycopg2.Error as e:
            err_msg = "Error while deleting records that match the new data at target"
            status = 'Job Error'
            remove_files(paths,table_name,source_gpfdist_processes)
            output_msg = traceback.format_exc()
            audit_logging(cur_metadata, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,source_row_count,0,output_msg)
            conn_target.rollback()
            conn_target.close()
            conn_metadata.close()
            continue

        print delete_sql

        try:
            insert_sql   = "INSERT INTO " + target_schema + "." + target_tablename + " SELECT " + target_insert_list + " FROM " \
                   + target_schema + "." + target_tablename + "_ext"
            print insert_sql
            cur_target.execute(insert_sql)
        except psycopg2.Error as e:
            err_msg = "Error while inserting new records in target"
            status = 'Job Error'
            remove_files(paths,table_name,source_gpfdist_processes)
            output_msg = traceback.format_exc()
            audit_logging(cur_metadata, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,source_row_count,0,output_msg)
            conn_target.rollback()
            conn_target.close()
            conn_metadata.close()
            continue
        target_row_count = cur_source.rowcount

        print insert_sql

        try:
            drop_table   = "DROP EXTERNAL TABLE " + target_schema + "." + target_tablename + "_ext"
            cur_target.execute(drop_table)
        except psycopg2.Error as e:
            err_msg = "Error while dropping external table in target"
            status = 'Job Error'
            remove_files(paths,table_name,source_gpfdist_processes)
            output_msg = traceback.format_exc()
            audit_logging(cur_metadata, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,source_row_count,target_row_count,output_msg)
            conn_target.rollback()
            conn_target.close()
            conn_metadata.close()
            continue

        print drop_table

        try:
            update_control_info_sql = "UPDATE admin.application_control set last_processed_value = '" + v_timestamp + "' where id = " + id
            cur_metadata.execute(update_control_info_sql)
        except psycopg2.Error as e:
            err_msg = "Error while dropping external table in target"
            status = 'Job Error'
            remove_files(paths,table_name,source_gpfdist_processes)
            output_msg = traceback.format_exc()
            audit_logging(cur_metadata, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            conn_target.rollback()
            conn_target.close()
            conn_metadata.close()
            continue

        try:
            err_msg    = 'No Errors'
            status     = 'Job Finished'
            output_msg = 'Job Finished successfully'
            audit_logging(cur_metadata, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,source_row_count,target_row_count,output_msg)
        except psycopg2.Error as e:
            err_msg = "Error while dropping external table in target"
            status = 'Job Error'
            remove_files(paths,table_name,source_gpfdist_processes)
            output_msg = traceback.format_exc()
            audit_logging(cur_metadata, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            conn_target.rollback()
            conn_target.close()
            conn_metadata.close()
            continue

        conn_target.commit()
        conn_target.close()
        conn_metadata.close()

        remove_files(paths,table_name,source_gpfdist_processes)

if __name__ == "__main__":
    merge_data('gec_sfdc.e1scontrol')
