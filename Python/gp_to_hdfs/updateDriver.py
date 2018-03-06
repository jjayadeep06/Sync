from readCfg import read_config
import base64
from incrUpdate import dbConnect
from incrUpdate import dbQuery
from math import ceil
from incrUpdate import merge_data
import sys
import pandas as pd
import multiprocessing
from subprocess import check_output
import os
import subprocess
from incrUpdate import sendMail
sys.path.append("/apps/common")
from auditLog import audit_logging

def main(input_source_schema,load_type):
    config = read_config(['/apps/gp2hdp_sync/environ.properties'])
    env             = config.get('branch','env')
    print env
    print config.get(env+'.meta_db','dbPwd')
    metastore_dbName           = config.get(env+'.meta_db','dbName')
    dbmeta_Url                 = config.get(env+'.meta_db','dbUrl')
    dbmeta_User                = config.get(env+'.meta_db','dbUser')
    dbmeta_Pwd                 = base64.b64decode(config.get(env+'.meta_db','dbPwd'))
    dbmeta_Port                = config.get(env+'.meta_db','dbPort')
    emailSender                = config.get(env+'.email','sender')
    emailReceiver              = config.get(env+'.email','receivers')
    input_source_schema        = sys.argv[1]
    load_type                  = sys.argv[2]
    dbtgt_classpath            = config.get(env+'.tgt_db_beeline','classPath')
    conn_metadata, cur_metadata    = dbConnect(metastore_dbName, dbmeta_User, dbmeta_Url, dbmeta_Pwd)

    metadata_sql    = "SELECT source_schemaname||'.'||source_tablename as table_name FROM sync.control_table  WHERE data_path = 'GP2HDFS' AND status_flag = 'Y' AND source_schemaname = '" + input_source_schema + "' AND (hvr_last_processed_value > last_run_time OR last_run_time IS NULL) AND load_type = '" + load_type + "'"
    print metadata_sql
    load_id_sql     = "select nextval('sbdt.edl_load_id_seq')"
    control         = dbQuery(cur_metadata, metadata_sql)
    load_id_lists   = dbQuery(cur_metadata,load_id_sql)
    load_id_list    = load_id_lists[0]
    load_id         = load_id_list['nextval']
    print "Load ID for this run is : " , load_id

    try:
        classpath  = os.environ["CLASSPATH"]
        classpath   = classpath + dbtgt_classpath
        os.environ["CLASSPATH"] = classpath
    except KeyError:
        classpath   = dbtgt_classpath
        os.environ["CLASSPATH"] = classpath

    control_df = pd.DataFrame(control)
    if len(control) > 0:
        control_df.columns = ['table_name']
    else:
        err_msg = "No tables to load in this cycle for schema : "
        error_table_list = input_source_schema
        sendMail(emailSender,emailReceiver,err_msg,error_table_list,load_id)
        sys.exit(1)

    new_control = control_df['table_name'].tolist()
    print new_control
    new_control2= [x + "." + str(load_id) for x in new_control]
    print new_control2
    pool = multiprocessing.Pool(processes=1)
    result = pool.map(merge_data,new_control2)
    pool.close()
    pool.join()

    error_log_sql   = "SELECT table_name FROM sbdt.edl_log WHERE load_id = " + str(load_id) + " AND status = 'Job Error' "
    error_tables    = dbQuery(cur_metadata, error_log_sql)
    print error_tables
    error_table_list = ','.join(d['table_name'] for d in error_tables)
    print error_table_list
    if len(error_table_list) > 0:
        err_msg = "Failed to load tables : "
        sendMail(emailSender,emailReceiver,err_msg,error_table_list,load_id)
    else:
        err_msg = "Job Finished successfully for Schema : "
        error_table_list = input_source_schema
        sendMail(emailSender,emailReceiver,err_msg,error_table_list,load_id)
if __name__ == "__main__":
    main(sys.argv[1],sys.argv[2])
