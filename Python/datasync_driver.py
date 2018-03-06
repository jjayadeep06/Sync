
import sys
sys.path.append("/apps/common/")
sys.path.append("/data/analytics/common")
import base64
from utils import dbConnect, dbQuery, sendMail
import sys
import multiprocessing
import traceback
from datasync_init import DataSyncInit
from db2file import Db2FileSync
from file2db import File2DBSync


def run_sync_process(input):
    print "[datasync_driver: run_sync_process] - Entered"

    data_path = input["data_path"]
    if data_path.find("MIR2") <> -1:
        table_name = input['target_schemaname'] + "." + input['target_tablename']
    else:
        table_name = input['source_schemaname'] + "." + input['source_tablename']

    print_hdr = "[datasync_driver: run_sync_process: " + data_path + ": " + table_name + ": " + str(input["load_id"]) + "] - "
    print (print_hdr + "Entered")

    if data_path.find("GP2HDFS") <> -1:
        db2file_sync = Db2FileSync(input)
        error, err_msg, output = db2file_sync.extract_gp2file_hive()
        if error == 0:
            print (print_hdr + "GP Extraction successful")

            file2db_sync = File2DBSync(input)
            # Overwrite data path for GP-->Hive load
            file2db_sync.data_path = "HDFS2MIR"
            h_error, h_err_msg = file2db_sync.load_hive(output)
            if h_error > 1:
                print (print_hdr + "ERROR: " + h_err_msg)
        else:
            print (print_hdr + "ERROR: " + err_msg)

    elif data_path == "SRC2GP":
        file2db_sync = File2DBSync(input)
        error, err_msg, tablename = file2db_sync.load_file2gp_hvr()

    elif data_path == 'SRC2RDS':
        file2db_sync = File2DBSync(input)
        error, err_msg, tablename = file2db_sync.file2rds_hvr()

    elif data_path == 'GP2Postgres':
        db2file_sync  = Db2FileSync(input)
        error, err_msg, tablename = db2file_sync.gp2file()
        if error > 0:
            print "Error while running GP2File"
            return
        else:
            file2db_sync    = File2DBSync(input)
            error, err_msg, tablename = file2db_sync.file2rds()

    elif data_path.find("MIR2") <> -1:
        file2db_sync = File2DBSync(input)
        # Overwrite data path for MIR-->Hive load
        file2db_sync.source_schemaname = input['target_schemaname']
        file2db_sync.source_tablename = input['target_tablename']

        h_error, h_err_msg = file2db_sync.load_hive()
        if h_error > 1:
            print (print_hdr + "ERROR: " + h_err_msg)



if __name__ == "__main__":
    print_hdr = "[datasync_driver: main] - Entered"
    data_sync = DataSyncInit()
    config_list = data_sync.config_list
    # print (print_hdr + "config_list: ", config_list)

    metastore_dbName           = config_list['meta_db_dbName']
    dbmeta_Url                 = config_list['meta_db_dbUrl']
    dbmeta_User                = config_list['meta_db_dbUser']
    dbmeta_Pwd                 = base64.b64decode(config_list['meta_db_dbPwd'])
    dbmeta_Port                = config_list['meta_db_dbPort']
    emailSender                = config_list['email_sender']
    emailReceiver              = config_list['email_receivers']
    # dbtgt_classpath            = config_list['tgt_db_beeline_classPath']

    if len(sys.argv) < 4:
        error = 1
        err_msg = "datasync_driver: main[{0}]: ERROR: Mandatory input arguments not passed".format(error)
        print "ERROR: " + err_msg
        error_table_list = ""
        sendMail(emailSender, emailReceiver, err_msg, error_table_list, 0, config_list['env'])
        sys.exit(1)

    input_schema                = sys.argv[1]
    load_type                   = sys.argv[2]
    data_path                   = sys.argv[3]
    load_group_id               = None
    input_tablename_list        = None
    input_multiprocessor        = None
    # Special logic to mirror table from one schema in GP to a different schema in HIVE
    is_special_logic            = False

    # input_source_schema         = "eservice"
    # load_type                   = "INCREMENTAL"
    # data_path                   = "GP2HDFS"
    # load_group__id              = 1

    print_hdr = "[datasync_driver: main: " + data_path + ": " + input_schema + "] - "

    if (len(sys.argv) > 4 is not None) and (str(sys.argv[4]).upper() <> 'NONE'):
        load_group_id = int(sys.argv[4])

    if (len(sys.argv) > 5 is not None) and (str(sys.argv[5]).upper() <> 'NONE'):
        input_tablename_list = sys.argv[5].split(',')
        tablename_filter = ",".join("'" + l + "'" for l in input_tablename_list)
        print print_hdr + "table list: " + tablename_filter

    if len(sys.argv) > 6 is not None:
        input_multiprocessor = int(sys.argv[6])

    if input_multiprocessor is None:
        input_multiprocessor = int(config_list['misc_multiprocessor_run'])
    else:
        input_multiprocessor = input_multiprocessor if input_multiprocessor < int(config_list['misc_multiprocessor_max']) else int(config_list['misc_multiprocessor_max'])

    # Special logic to mirror table from one schema in GP to a different schema in HIVE
    if len(sys.argv) > 7 is not None:
        arg = str(sys.argv[7])
        if arg == 'Y':
            is_special_logic = True

    print (print_hdr + "load_type: " + load_type + " with " + str(input_multiprocessor) + " multiprocessor")

    error = 0
    load_id = -1
    conn_metadata = None
    try:
        conn_metadata, cur_metadata = dbConnect(metastore_dbName, dbmeta_User, dbmeta_Url, dbmeta_Pwd)

        metadata_sql = "SELECT id, source_schemaname, source_tablename, target_schemaname, target_tablename, load_type, \
                incremental_column, last_run_time, second_last_run_time, join_columns, log_mode, data_path \
                FROM sync.control_table \
                WHERE data_path = '" + data_path + "' \
                    AND status_flag = 'Y'"
        if load_group_id is not None:
            metadata_sql = metadata_sql + " AND load_group_id = " + str(load_group_id)
        if data_path.find("MIR2") <> -1:
            metadata_sql = metadata_sql + " AND target_schemaname = '" + input_schema + "'"
        elif data_path.find("GP2HDFS") <> -1:
            metadata_sql = metadata_sql + " AND source_schemaname = '" + input_schema + "'" \
                           + " AND (hvr_last_processed_value > last_run_time OR last_run_time IS NULL)"
        else:
            metadata_sql = metadata_sql + " AND source_schemaname = '" + input_schema + "'"
        if input_tablename_list is not None:
            if data_path.find("MIR2") <> -1:
                metadata_sql = metadata_sql + " AND target_tablename in (" + tablename_filter + ")"
            else:
                metadata_sql = metadata_sql + " AND source_tablename in (" + tablename_filter + ")"
        metadata_sql = metadata_sql + " AND load_type = '" + load_type + "'"

        print (print_hdr + "metadata_sql: " + metadata_sql)

        load_id_sql     = "select nextval('sbdt.edl_load_id_seq')"
        controls        = dbQuery(cur_metadata, metadata_sql)
        print (print_hdr + "controls: ", controls)
        load_id_lists   = dbQuery(cur_metadata,load_id_sql)
        load_id_list    = load_id_lists[0]
        load_id         = load_id_list['nextval']
        print (print_hdr + "load_id: " + str(load_id))

        input = []
        if len(controls) > 0:
            for control in controls:
                # Special logic to mirror table from one schema in GP to a different schema in HIVE
                if data_path.find("GP2HDFS") <> -1 and control['source_schemaname'] <> control['target_schemaname'] and not is_special_logic:
                    error = 2
                    err_msg = "datasync_driver: main[{0}]: ERROR: Mirror loading between different schemas is not allowed: " \
                              " input schema: ".format(error)
                    print err_msg + input_schema
                    if conn_metadata is not None and not conn_metadata.closed:
                        conn_metadata.close()
                    sendMail(emailSender, emailReceiver, err_msg, input_schema, load_id, config_list['env'])
                    sys.exit(error)

                # input.append({'table': control['source_tablename'], 'data_path': data_path, 'load_id' : load_id})
                input.append({'id': control['id'], 'source_schemaname': control['source_schemaname'], 'source_tablename' : control['source_tablename'], \
                              'target_schemaname': control['target_schemaname'], 'target_tablename':control['target_tablename'], 'load_type': control['load_type'],\
                              'incremental_column': control['incremental_column'], 'last_run_time': control['last_run_time'], \
                              'second_last_run_time': control['second_last_run_time'], 'join_columns': control['join_columns'], 'log_mode': control['log_mode'], \
                              'data_path': control['data_path'], 'load_id' : load_id, 'plant_name':"DATASYNC", 'is_special_logic': is_special_logic})
        else:
            error = 3
            err_msg = "datasync_driver: main[{0}]: No ".format(error) + load_type + " " + data_path + " tables" + \
                        " to load in this cycle for schema: "
            print err_msg + input_schema
            if conn_metadata is not None and not conn_metadata.closed:
                conn_metadata.close()
            sendMail(emailSender, emailReceiver, err_msg, input_schema, load_id, config_list['env'])
            sys.exit(error)

        try:
            pool = multiprocessing.Pool(processes=input_multiprocessor)
            result = pool.map(run_sync_process, input, 1)
            print (print_hdr + "result: ", result)
            pool.close()
            pool.join()
        except Exception as e:
            error = 4
            err_msg = "datasync_driver: main[{0}]: ERROR in multiprocessing load jobs for Schema: ".format(error)
            print err_msg + input_schema
            print ("ERROR details: " + traceback.format_exc())
            if conn_metadata is not None and not conn_metadata.closed:
                conn_metadata.close()
            sendMail(emailSender, emailReceiver, err_msg, input_schema, load_id, config_list['env'])
            sys.exit(error)

        error_log_sql   = "SELECT table_name FROM sbdt.edl_log WHERE load_id = " + str(load_id) + " AND status = 'Job Error' "
        error_tables    = dbQuery(cur_metadata, error_log_sql)
        error_table_list = ','.join(d['table_name'] for d in error_tables)
        if len(error_table_list) > 0:
            print (print_hdr + "ERROR encountered for tables: ", error_tables)
            err_msg = "datasync_driver: main: Failed to load tables : "
            sendMail(emailSender, emailReceiver, err_msg, error_table_list, load_id, config_list['env'])
        else:
            err_msg = "datasync_driver: main: " + load_type + " " + data_path +  " Finished successfully for Schema: "
            error_table_list = input_schema
            sendMail(emailSender, emailReceiver, err_msg, error_table_list, load_id, config_list['env'])

    except Exception as e:
        error = 5
        err_msg = "datasync_driver: main[{0}]: ERROR while loading data for Schema: ".format(error)
        print err_msg + input_schema
        print ("ERROR details: " + traceback.format_exc())
        sendMail(emailSender, emailReceiver, err_msg, input_schema, load_id, config_list['env'])
    finally:
        if conn_metadata is not None and not conn_metadata.closed:
            conn_metadata.close()

    sys.exit(error)
