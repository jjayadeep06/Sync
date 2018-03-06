
import psycopg2
import base64
import sys
import traceback
import datetime
import time
from datetime import datetime
sys.path.append("/apps/common/")
from utils import remove_files, dbConnect, dbQuery, sendMail, dbConnectHive, txn_dbConnect
from auditLog import audit_logging
from datasync_init import DataSyncInit
import os

class Db2FileSync(DataSyncInit):


    def __init__(self, input = None):
        print ("[File2DBSync: __init__] -  Entered")
        super(Db2FileSync, self).__init__()

        self.class_name = self.__class__.__name__
        method_name = self.class_name + ": " + "__init__"

        if input is not None:
            self.id = input['id']
            self.source_schemaname = input['source_schemaname']
            self.source_tablename = input['source_tablename']
            self.target_schemaname = input['target_schemaname']
            self.target_tablename = input['target_tablename']
            self.load_type = input['load_type']
            self.incremental_column = input['incremental_column']
            self.last_run_time = input['last_run_time']
            self.second_last_run_time = str(input['second_last_run_time'])
            self.join_columns = input['join_columns']
            self.log_mode = input['log_mode']
            self.data_path = input['data_path']
            self.load_id = input['load_id']
            self.plant_name = input['plant_name']
            # Special logic to mirror table from one schema in GP to a different schema in HIVE
            self.is_special_logic = input.get('is_special_logic', False)
        else:
            print (method_name + "No data defined")
        # print (method_name, self.config_list)


    def extract_gp2file_hive(self):

        method_name = self.class_name + ": " + "extract_gp2file_hive"
        print_hdr = "[" + method_name + ": " + self.data_path + ": " + str(self.load_id) + "] - "
        print (print_hdr + "Entered")

        t=datetime.fromtimestamp(time.time())
        v_timestamp = str(t.strftime('%Y-%m-%d %H:%M:%S'))

        tablename = self.source_schemaname + '.' + self.source_tablename
        # self.input_schema_name, self.source_table_name = table.split('.')

        run_id_sql  = "select nextval('sbdt.edl_run_id_seq')"
        self.technology  = 'Python'
        self.system_name = 'GPDB'
        self.job_name    = 'GPDB-->HDFS'

        num_errors  = 1
        rows_inserted = 0
        rows_updated = 0
        rows_deleted = 0

        metastore_dbName           = self.config_list['meta_db_dbName']
        dbmeta_Url                 = self.config_list['meta_db_dbUrl']
        dbmeta_User                = self.config_list['meta_db_dbUser']
        dbmeta_Pwd                 = base64.b64decode(self.config_list['meta_db_dbPwd'])
        src_dbName                 = self.config_list['src_db_gp_dbName']
        dbsrc_Url                  = self.config_list['src_db_gp_dbUrl']
        dbsrc_User                 = self.config_list['src_db_gp_dbUser']
        dbsrc_Pwd                  = base64.b64decode(self.config_list['src_db_gp_dbPwd'])
        dbtgt_host                 = self.config_list['tgt_db_hive_dbUrl']
        dbtgt_host2                 = self.config_list['tgt_db_hive_dbUrl2']
        # dbtgt_Url                  = self.config_list['tgt_db_beeline_dbUrl']
        dbtgt_Port                 = self.config_list['tgt_db_hive_dbPort']
        dbtgt_Auth                 = self.config_list['tgt_db_hive_authMech']
        # dbtgt_classpath            = self.config_list['tgt_db_beeline_classPath']
        emailSender                = self.config_list['email_sender']
        emailReceiver              = self.config_list['email_receivers']
        executorMemory             = self.config_list['spark_params_executorMemory']
        executorCores              = self.config_list['spark_params_executorCores']
        driverMemory               = self.config_list['spark_params_driverMemory']
        loadScript                 = self.config_list['spark_params_loadScript']
        sparkVersion               = self.config_list['spark_params_sparkVersion']
        # paths                      = "/apps/staging/"
        paths                      = self.config_list['misc_hdfsStagingPath']
        t=datetime.fromtimestamp(time.time())
        v_timestamp = str(t.strftime('%Y-%m-%d %H:%M:%S'))

        error = 0
        err_msg = ''
        output = {}
        conn_metadata = None
        conn_source = None
        conn_target = None

        print (print_hdr + "MetaDB details: ", metastore_dbName, dbmeta_Url, dbmeta_User, dbmeta_Pwd, paths)
        try:
            conn_metadata, cur_metadata    = dbConnect(metastore_dbName, dbmeta_User, dbmeta_Url, dbmeta_Pwd)
            run_id_lists                   = dbQuery(cur_metadata,run_id_sql)
            run_id_list                    = run_id_lists[0]
            run_id                         = run_id_list['nextval']
            print (print_hdr + "Run ID for the table " + tablename, " is: " + str(run_id))
        except Exception as e:
            error = 1
            err_msg = method_name + "[{0}]: Error connecting to database while fetching metadata".format(error)
            self.error_cleanup(self.source_schemaname, self.source_tablename, run_id, paths, conn_metadata)
            sendMail(emailSender, emailReceiver, err_msg, tablename, self.load_id, self.config_list['env'])
            return error, err_msg, output

        #Audit entry at start of job

        print_hdr = "[" + method_name + ": " + self.data_path + ": " + str(self.load_id) + ": " + tablename + ": " + str(run_id) + "] - "
        status = 'Job Started'
        output_msg = ''
        audit_logging(cur_metadata, self.load_id,run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                      self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)

        # try:
        #     metadata_sql    = "SELECT * FROM sync.control_table \
        #             WHERE self.source_tablename = '" + self.source_tablename + "' \
        #             AND self.source_schemaname = '" + self.source_schemaname +"' \
        #             AND data_path = 'GP2HDFS' "
        #     print metadata_sql
        #     controls        = dbQuery(cur_metadata, metadata_sql)
        #     print controls
        #
        # except psycopg2.Error as e:
        #     error = 2
        #     err_msg = "Db2FileSync: extract_gp2file_hive[{0}]: Error connecting to control table database".format(error)
        #     status = 'Job Error'
        #     output_msg = traceback.format_exc()
        #     audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, table,status, self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
        #     return error, err_msg, output
        #
        # if not controls:
        #     error = 3
        #     err_msg = "Db2FileSync: extract_gp2file_hive[{0}]: No Entry found in metadata table".format(error)
        #     status = 'Job Error'
        #     output_msg = "No Entry found in metadata table"
        #     audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, table,status, self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
        #     return error, err_msg, output

        # for control in range(len(controls)):
        #     id                                = str(controls[control]['id'])
        #     self.source_schemaname                 = str(controls[control]['self.source_schemaname'])
        #     self.source_tablename                  = str(controls[control]['self.source_tablename'])
        #     self.target_schema                     = str(controls[control]['self.target_schemaname'])
        #     self.target_tablename                  = str(controls[control]['self.target_tablename'])
        #     self.load_type                         = str(controls[control]['self.load_type'])
        #     self.incremental_column                = str(controls[control]['self.incremental_column'])
        #     last_run_time                     = str(controls[control]['last_run_time'])
        #     self.second_last_run_time              = str(controls[control]['self.second_last_run_time'])
        #     join_columns                      = str(controls[control]['join_columns'])
        #     self.log_mode                          = str(controls[control]['self.log_mode'])
        #     data_path                         = str(controls[control]['data_path'])

        try:
            conn_source, cur_source      = dbConnect(src_dbName, dbsrc_User, dbsrc_Url, dbsrc_Pwd)
            if self.log_mode == 'DEBUG':
                status  = "Connected to Source database"
                audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                              self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)

        except psycopg2.Error as e:
            error = 4
            err_msg = method_name + "[{0}]: Error connecting to source database".format(error)
            status = 'Job Error'
            output_msg = traceback.format_exc()
            audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                          self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            self.error_cleanup(self.source_schemaname, self.source_tablename, run_id, paths, conn_metadata, conn_source)
            #continue
            return error, err_msg, output

        try:
            drop_src_table   = "DROP EXTERNAL TABLE IF EXISTS " + self.source_schemaname + "." + self.source_tablename + "_wext"
            print (print_hdr + "drop_src_table: " + drop_src_table)
            cur_source.execute(drop_src_table)
            if self.log_mode == 'DEBUG':
                status  = "Dropped Writable External Table on Source"
                audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                              self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)

        except psycopg2.Error as e:
            error = 5
            err_msg = method_name + "[{0}]: Error while dropping Writable External table in source".format(error)
            status = 'Job Error'
            output_msg = traceback.format_exc()
            audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                          self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            # conn_metadata.close()
            self.error_cleanup(self.source_schemaname, self.source_tablename, run_id, paths, conn_metadata, conn_source)
            #continue
            return error, err_msg, output

        try:
            # create_writable_sql = "CREATE WRITABLE EXTERNAL TABLE " + self.source_schemaname + "." + self.source_tablename + "_wext \
            #             (LIKE " + self.source_schemaname + "." + self.source_tablename + ") \
            #             LOCATION('gphdfs://getnamenode:8020/apps/staging/" + self.source_schemaname + "/" + self.source_tablename.replace('$','_') + "_ext') \
            #             FORMAT 'TEXT' (DELIMITER E'\x1A' NULL '' ESCAPE '\\\\')"

            # Special logic to mirror table from one schema in GP to a different schema in HIVE
            tmp_source_schemaname = self.source_schemaname
            if self.data_path.find("GP2HDFS") <> -1 and self.target_schemaname <> self.source_schemaname and self.is_special_logic:
                tmp_source_schemaname = self.target_schemaname

            create_writable_sql = "CREATE WRITABLE EXTERNAL TABLE " + self.source_schemaname + "." + self.source_tablename + "_wext \
                        (LIKE " + self.source_schemaname + "." + self.source_tablename + ") \
                        LOCATION('gphdfs://getnamenode:8020/apps/staging/" + tmp_source_schemaname + "/" + self.source_tablename.replace('$','_') + "_ext') \
                        FORMAT 'TEXT' (DELIMITER E'\x1A' NULL '' ESCAPE '\\\\')"

            print (print_hdr + "create_writable_sql: " + create_writable_sql)
            cur_source.execute(create_writable_sql)
            if self.log_mode == 'DEBUG':
                status  = "Created Writable External Table on Source"
                audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                              self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)

        except psycopg2.Error as e:
            error = 6
            err_msg = method_name + "[{0}]: Error while creating Writable External table in source".format(error)
            status = 'Job Error'
            output_msg = traceback.format_exc()
            audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                          self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            self.error_cleanup(self.source_schemaname, self.source_tablename, run_id, paths, conn_metadata, conn_source)
            # continue
            return error, err_msg, output

        # try:
        #     hive_upd_by_sql    = "ALTER EXTERNAL TABLE " + self.source_schemaname + "." + self.source_tablename + "_wext ADD COLUMN hive_updated_by text"
        #     print (print_hdr + "hive_upd_by_sql: " + hive_upd_by_sql)
        #     cur_source.execute(hive_upd_by_sql)
        #     if self.log_mode == 'DEBUG':
        #         status  = "Added column hive_updated_by to Writable External Table"
        #         audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
        #                       self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
        #
        # except psycopg2.Error as e:
        #     error = 7
        #     err_msg = method_name + "[{0}]: Error while adding column hive_updated_by to Writable External Table".format(error)
        #     status = 'Job Error'
        #     output_msg = traceback.format_exc()
        #     audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
        #                   self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
        #     self.error_cleanup(self.source_schemaname, self.source_tablename, run_id, paths, conn_metadata, conn_source)
        #     # continue
        #     return error, err_msg, output
        #
        # try:
        #     hive_upd_date_sql    = "ALTER EXTERNAL TABLE " + self.source_schemaname + "." + self.source_tablename + "_wext ADD COLUMN hive_updated_date timestamp without time zone"
        #     print (print_hdr + "hive_upd_date_sql: " + hive_upd_date_sql)
        #     cur_source.execute(hive_upd_date_sql)
        #     if self.log_mode == 'DEBUG':
        #         status  = "Added column hive_updated_date to Writable External Table"
        #         audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
        #                       self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
        #
        # except psycopg2.Error as e:
        #     error = 8
        #     err_msg = method_name + "[{0}]: Error while adding column hive_updated_date to Writable External Table".format(error)
        #     status = 'Job Error'
        #     output_msg = traceback.format_exc()
        #     audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
        #                   self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
        #     self.error_cleanup(self.source_schemaname, self.source_tablename, run_id, paths, conn_metadata, conn_source)
        #     # continue
        #     return error, err_msg, output
        #
        # try:
        #     hive_op_code_sql    = "ALTER EXTERNAL TABLE " + self.source_schemaname + "." + self.source_tablename + "_wext ADD COLUMN op_code integer"
        #     print (print_hdr + "hive_op_code_sql: " + hive_op_code_sql)
        #     cur_source.execute(hive_op_code_sql)
        #     if self.log_mode == 'DEBUG':
        #         status  = "Added column op_code to Writable External Table"
        #         audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
        #                       self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
        #
        # except psycopg2.Error as e:
        #     error = 9
        #     err_msg = method_name + "[{0}]: Error while adding column op_code to Writable External Table".format(error)
        #     status = 'Job Error'
        #     output_msg = traceback.format_exc()
        #     audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
        #                   self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
        #     self.error_cleanup(self.source_schemaname, self.source_tablename, run_id, paths, conn_metadata, conn_source)
        #     # continue
        #     return error, err_msg, output
        #
        # try:
        #     hive_time_key_sql    = "ALTER EXTERNAL TABLE " + self.source_schemaname + "." + self.source_tablename + "_wext ADD COLUMN time_key varchar(100)"
        #     print (print_hdr + "hive_time_key_sql: " + hive_time_key_sql)
        #     cur_source.execute(hive_time_key_sql)
        #     if self.log_mode == 'DEBUG':
        #         status  = "Added column time_key to Writable External Table"
        #         audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
        #                       self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
        #
        # except psycopg2.Error as e:
        #     error = 10
        #     err_msg = method_name + "[{0}]: Error while adding column time_key to Writable External Table".format(error)
        #     status = 'Job Error'
        #     output_msg = traceback.format_exc()
        #     audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
        #                   self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
        #     self.error_cleanup(self.source_schemaname, self.source_tablename, run_id, paths, conn_metadata, conn_source)
        #     # continue
        #     return error, err_msg, output

        try:
            add_column_list = ["hive_updated_by text", "hive_updated_date timestamp without time zone", "op_code integer", "time_key varchar(100)"]

            for add_column in add_column_list:
                hive_add_colum_sql = "ALTER EXTERNAL TABLE " + self.source_schemaname + "." + self.source_tablename + "_wext ADD COLUMN " + str(add_column)
                print (print_hdr + "hive_add_colum_sql: " + str(add_column) + ": " + hive_add_colum_sql)
                cur_source.execute(hive_add_colum_sql)
                if self.log_mode == 'DEBUG':
                    status  = "Added column" + str(add_column) + " to Writable External Table"
                    audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                                  self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
        except psycopg2.Error as e:
            error = 7
            err_msg = method_name + "[{0}]: Error while adding columns to Writable External Table".format(error)
            status = 'Job Error'
            output_msg = traceback.format_exc()
            audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                          self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            self.error_cleanup(self.source_schemaname, self.source_tablename, run_id, paths, conn_metadata, conn_source)
            # continue
            return error, err_msg, output

        print (print_hdr + "load_type: " + self.load_type)

        try:
            if self.load_type == 'FULL':
                incremental_sql = "INSERT INTO " + self.source_schemaname + "." + self.source_tablename + "_wext" + " SELECT *, 'datasync', now(), 1, 1  FROM " + self.source_schemaname + "." + self.source_tablename
            elif self.load_type == 'INCREMENTAL':
                incremental_sql = "INSERT INTO " + self.source_schemaname + "." + self.source_tablename + "_wext" + " SELECT *, 'datasync', now(), 1, 1 FROM " + self.source_schemaname + "." + self.source_tablename + " WHERE " + self.incremental_column + " > '" + self.second_last_run_time + "' AND " + self.incremental_column + " <= '" + v_timestamp + "'"
            else:
                incremental_sql = "INSERT INTO " + self.source_schemaname + "." + self.source_tablename + "_wext" + " SELECT *, 'datasync', now(), 1, 1 FROM " + self.source_schemaname + "." + self.source_tablename + " WHERE " + self.incremental_column + " > '" + self.second_last_run_time + "' AND " + self.incremental_column + " <= '" + v_timestamp + "' AND hvr_is_deleted = 0"

            print (print_hdr + "incremental_sql: " + incremental_sql)

            # Special logic to mirror table from one schema in GP to a different schema in HIVE
            # remove_files(paths,self.source_schemaname,self.source_tablename)
            remove_files(paths, tmp_source_schemaname, self.source_tablename)
            if self.log_mode == 'DEBUG':
                status  = "Removed files from HDFS before dumping data from GPDB"
                audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                              self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)

            cur_source.execute(incremental_sql)
            rows_inserted  = cur_source.rowcount
            if self.log_mode == 'DEBUG':
                status  = "Dumped data into HDFS through Writable External Table"
                audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                              self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)

            print (print_hdr + "Rows inserted into Writable External Table : " + str(rows_inserted))

        except psycopg2.Error as e:
            error = 11
            err_msg = method_name + "[{0}]: Error while inserting data into Writable External table in source".format(error)
            status = 'Job Error'
            # remove_files(paths,self.source_schemaname,self.source_tablename)
            output_msg = traceback.format_exc()
            audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                          self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            # conn_metadata.close()
            self.error_cleanup(self.source_schemaname, self.source_tablename, run_id, paths, conn_metadata, conn_source)
            # continue
            return error, err_msg, output
        except Exception as e:
            error = 12
            err_msg = method_name + "[{0}]: Error before inserting data into Writable External table in source".format(error)
            status = 'Job Error'
            # remove_files(paths,self.source_schemaname,self.source_tablename)
            output_msg = traceback.format_exc()
            audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                          self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            # conn_metadata.close()
            self.error_cleanup(self.source_schemaname, self.source_tablename, run_id, paths, conn_metadata, conn_source)
            # continue
            return error, err_msg, output

        try:
            drop_src_table   = "DROP EXTERNAL TABLE IF EXISTS " + self.source_schemaname + "." + self.source_tablename + "_wext"
            print (print_hdr + "drop_src_table: " + drop_src_table)
            cur_source.execute(drop_src_table)
            if self.log_mode == 'DEBUG':
                status  = "Dropped Writable External Table on Source"
                audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                              self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)

        except psycopg2.Error as e:
            error = 13
            err_msg = method_name + "[{0}]: Error while dropping Writable External table in source".format(error)
            status = 'Job Error'
            # remove_files(paths,self.source_schemaname,self.source_tablename)
            output_msg = traceback.format_exc()
            audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                          self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            # conn_metadata.close()
            self.error_cleanup(self.source_schemaname, self.source_tablename, run_id, paths, conn_metadata, conn_source)
            #continue
            return error, err_msg, output

        try:
            status     = "Finished GPDB part"
            num_errors = 0
            err_msg    = "No Errors"
            output_msg = "No Errors"
            audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                          self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)

        except psycopg2.Error as e:
            error = 14
            err_msg = method_name + "[{0}]: Error while updating Log Table after GPDB part".format(error)
            status = 'Job Error'
            conn_source.rollback()
            # remove_files(paths,self.source_schemaname,self.source_tablename)
            output_msg = traceback.format_exc()
            audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                          self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            # conn_target.rollback()
            # conn_target.close()
            # conn_source.close()
            # conn_metadata.close()
            self.error_cleanup(self.source_schemaname, self.source_tablename, run_id, paths, conn_metadata, conn_source)
            return error, err_msg, output

        # HIVE
        print (print_hdr + "HIVE............")

        try:
                #conn_target, cur_target   = dbConnectHiveBeeline(dbtgt_Url,dbtgt_classpath)
                conn_target, cur_target   = dbConnectHive(dbtgt_host, dbtgt_Port, dbtgt_Auth)
                if self.log_mode == 'DEBUG':
                    status  = "Connected to Target Database"
                    audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                                  self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)

        except Exception as e:
            try:
                conn_target, cur_target   = dbConnectHive(dbtgt_host2, dbtgt_Port, dbtgt_Auth)
            except Exception as e:
                error = 15
                err_msg      = method_name + "[{0}]: Error while connecting to target database".format(error)
                status       = 'Job Error'
                # remove_files(paths,self.source_schemaname,self.source_tablename)
                # print (print_hdr + "Exception: ", e)
                # output_msg   = e
                output_msg = traceback.format_exc()
                audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                              self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
                self.error_cleanup(self.source_schemaname, self.source_tablename, run_id, paths, conn_metadata, conn_target)
                return error, err_msg, output

        hive_columns_sql = "SELECT ARRAY_TO_STRING(ARRAY(SELECT '`' || COLUMN_NAME||'` '||( \
                                case when data_type in('numeric') and numeric_precision is not null and numeric_scale=0 then 'bigint' \
                                when data_type in('numeric','double precision') or numeric_scale > 0 then 'double' \
                                when data_type in('character varying','character','text') then 'string' \
                                when data_type in('timestamp without time zone') then 'timestamp' \
                                when data_type in('bigint') then 'bigint' when data_type in('integer') then 'int' \
                                when data_type in('smallint') then 'smallint' \
                                when data_type in('date') then 'date' \
                                when data_type in('name') then 'string' \
                                when data_type in ('ARRAY') then 'array<string>' else 'string' end) \
                            FROM INFORMATION_SCHEMA.COLUMNS \
                            WHERE TABLE_NAME='" + self.source_tablename + "' \
                            and table_schema='" + self.source_schemaname + "' ORDER BY ORDINAL_POSITION), ', ')"

        print (print_hdr + "hive_columns_sql: " + hive_columns_sql)

        try:
            columns      = dbQuery(cur_source,hive_columns_sql)
            if self.log_mode == 'DEBUG':
                status  = "Got column list from GPDB with data type conversion"
                audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                              self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)

        except psycopg2.Error as e:
            error = 16
            err_msg      = method_name + "[{0}]: Error while getting column list with data type for target table from GPDB".format(error)
            status       = 'Job Error'
            # remove_files(paths,self.source_schemaname,self.source_tablename)
            output_msg   = traceback.format_exc()
            audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                          self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            self.error_cleanup(self.source_schemaname, self.source_tablename, run_id, paths, conn_metadata, conn_target)
            return error, err_msg, output

        column_list_orig                = ",".join(d['array_to_string'] for d in columns)
        column_list                     = column_list_orig + ",`hive_updated_by` string, `hive_updated_date` timestamp, `op_code` int, `time_key` string "
        print (print_hdr, column_list)

        # Special logic to mirror table from one schema in GP to a different schema in HIVE
        self.source_schemaname = tmp_source_schemaname

        drop_hive_ext_table             = "DROP TABLE IF EXISTS `" + self.source_schemaname + "." + self.source_tablename.replace('$','_') + "_ext`"
        print (print_hdr + "drop_hive_ext_table: " + drop_hive_ext_table)
        try:
                cur_target.execute(drop_hive_ext_table)
                if self.log_mode == 'DEBUG':
                    status  = "Dropped Hive External table"
                    audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                                  self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)

        except Exception as e:
            # print e
            error = 17
            err_msg      = method_name + "[{0}]: Error while dropping Hive External table".format(error)
            status       = 'Job Error'
            # remove_files(paths,self.source_schemaname,self.source_tablename)
            output_msg   = traceback.format_exc()
            audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                          self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            self.error_cleanup(self.source_schemaname, self.source_tablename, run_id, paths, conn_metadata, conn_target)
            return error, err_msg, output

        create_hive_ext_table           = "CREATE EXTERNAL TABLE `" + self.source_schemaname + "." + self.source_tablename.replace('$','_') + "_ext`(" + column_list.replace('$','_') + ") \
                                    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u001A' ESCAPED BY '\\\\' LINES TERMINATED BY '\n'  \
                                    STORED AS TEXTFILE LOCATION '" + paths + self.source_schemaname + "/" + self.source_tablename.replace('$','_') + "_ext' TBLPROPERTIES('serialization.null.format'='')"

        print (print_hdr + "create_hive_ext_table: " + create_hive_ext_table)

        try:
                cur_target.execute(create_hive_ext_table)
                if self.log_mode == 'DEBUG':
                    status  = "Created Hive External Table"
                    audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                                  self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)

        except Exception as e:
            error = 18
            err_msg      = method_name + "[{0}]: Error while creating Hive External table".format(error)
            status       = 'Job Error'
            # remove_files(paths,self.source_schemaname,self.source_tablename)
            output_msg   = traceback.format_exc()
            audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, self.data_path, \
                          self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            self.error_cleanup(self.source_schemaname, self.source_tablename, run_id, paths, conn_metadata, conn_target)
            return error, err_msg, output

        print (print_hdr + "load_type: " + self.load_type)
        if self.load_type == 'FULL':
            drop_hive_mngd_table        = "DROP TABLE IF EXISTS `" + self.source_schemaname + "." + self.source_tablename.replace('$','_') + "`"
            print (print_hdr + "drop_hive_mngd_table: " + drop_hive_mngd_table)
            try:
                    cur_target.execute(drop_hive_mngd_table)
                    if self.log_mode == 'DEBUG':
                        status  = "Dropped Managed Table in Hive for Full Load"
                        audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                                      self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)

            except Exception as e:
                error = 19
                err_msg      = method_name + "[{0}]: Error while dropping Hive Managed table".format(error)
                status       = 'Job Error'
                # remove_files(paths,self.source_schemaname,self.source_tablename)
                output_msg   = traceback.format_exc()
                audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                              self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
                self.error_cleanup(self.source_schemaname, self.source_tablename, run_id, paths, conn_metadata, conn_target)
                return error, err_msg, output


            create_hive_mngd_table      = "CREATE EXTERNAL TABLE `" + self.source_schemaname + "." + self.source_tablename.replace('$','_') + "`(" + column_list.replace('$','_') + ") \
                                    STORED AS ORC LOCATION '/apps/hive/warehouse/" + self.source_schemaname + ".db/" + self.source_tablename.replace('$','_') + "' \
                                    TBLPROPERTIES ('orc.compress'='ZLIB')"

            print (print_hdr + "create_hive_mngd_table: " + create_hive_mngd_table)
            try:
                    cur_target.execute(create_hive_mngd_table)
                    if self.log_mode == 'DEBUG':
                        status  = "Created Table in Hive for Full Load"
                        audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                                      self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)

            except Exception as e:
                error = 20
                err_msg      = method_name + "[{0}]: Error while Creating Hive External table".format(error)
                status       = 'Job Error'
                # remove_files(paths,self.source_schemaname,self.source_tablename)
                output_msg   = traceback.format_exc()
                audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                              self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
                self.error_cleanup(self.source_schemaname, self.source_tablename, run_id, paths, conn_metadata,conn_target)
                return error, err_msg, output

        try:
            if conn_metadata is not None and not conn_metadata.closed:
                conn_metadata.close()

            if conn_source is not None and not conn_source.closed:
                conn_source.close()

            if conn_target is not None:
                conn_target.close()
        except Exception as e:
            error = 21
            err_msg = "Db2FileSync: extract_gp2file_hive[{0}]: Error while closing open DB connections".format(error)
            status = 'Job Error'
            # remove_files(paths, self.source_schemaname, self.source_tablename)
            output_msg = traceback.format_exc()
            audit_logging(None, self.load_id, run_id, self.plant_name, self.system_name, self.job_name,
                          tablename, status, self.data_path, self.technology, rows_inserted, rows_updated, rows_deleted,
                          num_errors, err_msg, 0, 0, output_msg)
            self.error_cleanup(self.source_schemaname, self.source_tablename, run_id, paths, conn_metadata, conn_source, conn_target)
            return error, err_msg, output

        output = {'run_id':run_id, 'v_timestamp':v_timestamp, 'rows_inserted':rows_inserted, 'rows_updated':rows_updated, 'rows_deleted':rows_deleted, 'num_errors':num_errors}

        return error, err_msg, output

        # try:
        #     print "before call of spark submit"
        #     data_path = "HDFS2MIR"
        #     call_spark_submit(sparkVersion,executorMemory,executorCores,driverMemory,loadScript,self.source_schemaname,self.source_tablename,load_id,run_id,data_path,v_timestamp)
        #     if self.log_mode == 'DEBUG':
        #         status  = "Started Spark Job"
        #         audit_logging(cur_metadata, load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
        # except Exception as e:
        #     err_msg      = "Error while running spark submit"
        #     status       = 'Job Error'
        #     remove_files(paths,self.source_schemaname,self.source_tablename)
        #     output_msg   = traceback.format_exc()
        #     audit_logging(cur_metadata, load_id,run_id, self.plant_name, self.system_name, self.job_name, tablename,status, data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
        #     return


    def gp2file(self):
        print "Inside GP2FILE"
        tablename           = self.source_schemaname + "." + self.source_tablename
        run_id_sql          = "select nextval('sync.datasync_seq')"
        plant_name          = 'DATASYNC'
        system_name         = 'GP - File'
        job_name            = 'GP-->File'
        technology          = 'Python'
        num_errors          = 0
        source_row_count    = 0
        target_row_count    = 0

        # proceed to point everything at the 'branched' resources
        metastore_dbName           = self.config_list['meta_db_dbName']
        dbmeta_Url                 = self.config_list['meta_db_dbUrl']
        dbmeta_User                = self.config_list['meta_db_dbUser']
        dbmeta_Pwd                 = base64.b64decode(self.config_list['meta_db_dbPwd'])

        dbtgt_Url                  = self.config_list['tgt_db_dbUrl']
        dbtgt_User                 = self.config_list['tgt_db_dbUser']

        dbsrc_Url                   = self.config_list['src_db_i360_dbUrl']
        dbsrc_User                  = self.config_list['src_db_i360_dbUser']
        dbsrc_dbName                = self.config_list['src_db_i360_dbName']
        dbsrc_Pwd                   = base64.b64decode(self.config_list['src_db_i360_dbPwd'])

        dbtgt_Url                  = self.config_list['tgt_db_i360_dbUrl']
        dbtgt_User                 = self.config_list['tgt_db_i360_dbUser']
        dbtgt_dbName                = self.config_list['tgt_db_i360_dbName']
        dbtgt_Pwd                 = base64.b64decode(self.config_list['tgt_db_i360_dbPwd'])

        data_paths                 = self.config_list['misc_dataPath']
        hdfs_data_path             = self.config_list['misc_hdfsPath']
        data_paths_i360             = self.config_list['misc_dataPathi360']

        source_gpfdist_host         = self.config_list['src_db_i360_gpfdistHost']
        source_gpfdist_port         = self.config_list['src_db_i360_portRange']
        print "GPFDIST DETAILS:" , source_gpfdist_host, source_gpfdist_port


        t                          = datetime.fromtimestamp(time.time())
        v_timestamp                = str(t.strftime('%Y-%m-%d %H:%M:%S'))

        try:
            conn_metadata, cur_metadata    = dbConnect(metastore_dbName, dbmeta_User, dbmeta_Url, dbmeta_Pwd)
            run_id_lists                   = dbQuery(cur_metadata,run_id_sql)
            run_id_list                    = run_id_lists[0]
            run_id                         = run_id_list['nextval']
            print "Run ID for the table", tablename , " is : ", run_id
        except Exception as e:
            err_msg = "Error connecting to database while fetching  metadata"
            error   = 1
            print e
            return

#Audit entry at start of job
        err_msg       = ''
        err_msg       = ''
        status        = 'Job Started'
        output_msg    = ''
        rows_inserted = 0
        rows_deleted  = 0
        rows_updated  = 0
        num_errors    = 0
        audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)

# Get source database access

        try:
           conn_source, cur_source  = txn_dbConnect(dbsrc_dbName, dbsrc_User, dbsrc_Url, dbsrc_Pwd)
        except psycopg2.Error as e:
            error   = 1
            err_msg = "Error connecting to Source Database"
            print err_msg
            status = 'Job Error'
            output_msg = traceback.format_exc()
            print output_msg
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            conn_metadata.close()
            return error, err_msg, self.source_schemaname + "." + self.source_tablename

# Get columns for Writable External Table
        try:
            column_list_sql = "SELECT attrelid::regclass, attnum, attname,format_type(a.atttypid, a.atttypmod) AS data_type FROM pg_attribute a WHERE  attrelid = " \
                                  + "'" + self.source_schemaname + "." + self.source_tablename + "'" + "::regclass"              \
                                  " AND attnum > 0 AND NOT attisdropped ORDER BY attnum"
            columns         = dbQuery(cur_source, column_list_sql)
        except psycopg2.Error as e:
            error   = 2
            err_msg = "Error while getting column list for Heap table"
            print err_msg
            status = 'Job Error'
            output_msg = traceback.format_exc()
            print output_msg
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            return error, err_msg, self.source_schemaname + "." + self.source_tablename

        if not columns:
            err_msg = "Column list not found in pg_attribute system table "
            status = 'Job Error'
            output_msg = "Column list not found in pg_attribute system table "
            print output_msg
            error   = 3
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            return error, err_msg, self.source_schemaname + "." + self.source_tablename

        abs_file_name   = data_paths_i360 + self.source_schemaname + "." + self.source_tablename + ".dat"
        print abs_file_name

# Remove data files from gpfdist directory
        try:
            os.remove(abs_file_name)
        except OSError:
            pass

# Drop Writable External Table if it already exists

        try:
            drop_src_table   = "DROP EXTERNAL TABLE IF EXISTS " + self.target_schemaname + "." + self.target_tablename + "_wext"
            cur_source.execute(drop_src_table)
        except psycopg2.Error as e:
            error=4
            err_msg = "Error while dropping writable external table in source"
            print e
            status = 'Job Error'
            sql_state = e.pgcode
            sql_error_msg = e.pgerror
            print sql_state
            print sql_error_msg
            if not sql_state:
                output_msg = sql_state + ':' + sql_error_msg
            else:
                output_msg = traceback.format_exc()
            audit_logging(cur_metadata, self.load_id, run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            conn_source.rollback()
            conn_source.close()
            conn_metadata.close()
            return error, err_msg, self.source_schemaname + "." + self.source_tablename

# Prepare column list for Writable External Table
        create_list     = ',"'.join(d['attname']+'" '+d['data_type'] for d in columns)
        create_list     = '"' + create_list

        try:
            create_writable_sql     = "CREATE WRITABLE EXTERNAL TABLE " + self.target_schemaname + "." + self.target_tablename + "_wext (" + create_list + ") LOCATION ('gpfdist://" + source_gpfdist_host + ":" + source_gpfdist_port + "/" + self.source_schemaname + "." + self.source_tablename + ".dat"
            create_writable_sql = create_writable_sql + " ') FORMAT 'TEXT' (DELIMITER E'\x01')"
            print create_writable_sql
            cur_source.execute(create_writable_sql)
        except psycopg2.Error as e:
            error=5
            err_msg = "Error while creating writable external table in source"
            status = 'Job Error'
            sql_state = e.pgcode
            sql_error_msg = e.pgerror
            if not sql_state:
                output_msg = sql_state + ':' + sql_error_msg
            else:
                output_msg = traceback.format_exc()
            audit_logging(cur_metadata, self.load_id, run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            conn_source.rollback()
            conn_source.close()
            conn_metadata.close()
            return error, err_msg, self.source_schemaname + "." + self.source_tablename

        if self.load_type == 'FULL' :
            incremental_sql = "INSERT INTO " + self.target_schemaname + "." + self.target_tablename + "_wext" + " SELECT * FROM " + self.source_schemaname + "." + self.source_tablename
        else:
            incremental_sql = "INSERT INTO " + self.target_schemaname+ "." + self.target_tablename+ "_wext" + " SELECT * FROM " + self.source_schemaname + "." + self.source_tablename \
                              + " WHERE " + self.incremental_column + " > '"  + str(self.last_run_time) + "' AND " + self.incremental_column + " <= '" + v_timestamp + "'"
        try:
            print incremental_sql
            cur_source.execute(incremental_sql)
            rows_inserted = cur_source.rowcount
            print "Rows Inserted : ", rows_inserted
        except psycopg2.Error as e:
            error= 6
            err_msg = "Error while inserting data into writable external table in source"
            status = 'Job Error'
            sql_state = e.pgcode
            sql_error_msg = e.pgerror
            if not sql_state:
                output_msg = sql_state + ':' + sql_error_msg
            else:
                output_msg = traceback.format_exc()
            audit_logging(cur_metadata, self.load_id, run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            conn_source.rollback()
            conn_source.close()
            conn_metadata.close()
            return error, err_msg, self.source_schemaname + "." + self.source_tablename

# Create the Target Table if the Load Type is FULL

        if self.load_type == 'FULL':
# Get target database access
            try:
               conn_target, cur_target      = dbConnect(dbtgt_dbName, dbtgt_User, dbtgt_Url, dbtgt_Pwd)
            except psycopg2.Error as e:
                error   = 7
                err_msg = "Error connecting to Target database"
                print err_msg
                status = 'Job Error'
                output_msg = traceback.format_exc()
                print output_msg
                audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
                conn_metadata.close()
                return error, err_msg, self.source_schemaname + "." + self.source_tablename

            try:
                create_tgt_table_sql    = "CREATE TABLE " + self.target_schemaname + "." + self.target_tablename + "(" + create_list + ")"
                print create_tgt_table_sql
                cur_target.execute(create_tgt_table_sql)
                conn_target.close()
            except psycopg2.Error as e:
                error   = 8
                if e.pgcode == '42P07':
                    err_msg =  "Table already exists in Target"
                    print err_msg
                    # audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
                    pass
                else:
                    err_msg = "Error while creating Target Table for FULL Load"
                    print err_msg
                    status = 'Job Error'
                    output_msg = traceback.format_exc()
                    print output_msg
                    audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
                    conn_metadata.close()
                    return error, err_msg, self.source_schemaname + "." + self.source_tablename

# Final log entry
        try:
            error= 0
            err_msg     = 'No Errors'
            status      = 'Job Finished'
            output_msg  = 'Job Finished successfully'
            print output_msg
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,source_row_count,target_row_count,output_msg)
        except psycopg2.Error as e:
            error= 15
            err_msg = "Error while dropping external table in target"
            print err_msg
            status = 'Job Error'
            output_msg = traceback.format_exc()
            print output_msg
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            conn_target.rollback()
            conn_target.close()
            conn_metadata.close()
            return error, err_msg, self.source_schemaname + "." + self.source_tablename

        conn_metadata.close()
        conn_source.close()
        error =0
        return error, err_msg, self.source_schemaname + "." + self.source_tablename


if __name__ == "__main__":
    print "ERROR: Direct execution on this file is not allowed"
