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
import subprocess

def main(load_type):
    config = read_config(['/pwcloud/app/gpsync/environ.properties'])
    env             = config.get('branch','env')
    print env
    print config.get(env+'.meta_db','dbPwd')
    metastore_dbName           = config.get(env+'.meta_db','dbName')
    dbmeta_Url                 = config.get(env+'.meta_db','dbUrl')
    dbmeta_User                = config.get(env+'.meta_db','dbUser')
    dbmeta_Pwd                 = base64.b64decode(config.get(env+'.meta_db','dbPwd'))

    schema_list                = config.get(env+'.misc','schemaList')
    conn_metadata, cur_metadata    = dbConnect(metastore_dbName, dbmeta_User, dbmeta_Url, dbmeta_Pwd)

    metadata_sql    = "SELECT source_schema||'.'||source_tablename as table_name FROM admin.application_control where (hvr_last_processed_value > last_processed_value OR last_processed_value IS NULL) "
    if schema_list != 'None':
       metadata_sql = metadata_sql + " AND target_schema IN ( " + str(schema_list) + " )"
    if load_type:
       metadata_sql = metadata_sql + " AND load_type = '" + str(load_type) + "'"

    metadata_sql =  metadata_sql
    print metadata_sql

    control         = dbQuery(cur_metadata, metadata_sql)

    if len(control) > 0:
        control_df = pd.DataFrame(control)
        control_df.columns = ['table_name']

        new_control = control_df['table_name'].tolist()

        try:
            pool = multiprocessing.Pool(processes=20)
            result = pool.map(merge_data,new_control,1)
            pool.close()
            pool.join()
        except:
            pass

if __name__ == "__main__":
    main(sys.argv[1])
