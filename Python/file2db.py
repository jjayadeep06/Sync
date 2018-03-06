
import mysql.connector
import base64
import sys
import traceback
import datetime
from datetime import datetime
sys.path.append("/apps/gp2hdp_sync/")
sys.path.append("/apps/common/")
from utils import remove_files, dbConnect, dbQuery, sendMail, run_cmd, call_spark_submit, move_files, txn_dbConnect, call_datasync_method
from auditLog import audit_logging
import psycopg2
import time
from datasync_init import DataSyncInit
import os
import glob


class File2DBSync(DataSyncInit):

    def __init__(self, input=None):
        print ("[File2DBSync: __init__] -  Entered")
        super(File2DBSync, self).__init__()

        self.class_name = self.__class__.__name__

        if input is not None:
            self.id = input['id']
            self.source_schemaname = input['source_schemaname']
            self.source_tablename = input['source_tablename']
            self.target_schemaname = input['target_schemaname']
            self.target_tablename = input['target_tablename']
            self.load_type = input['load_type']
            self.incremental_column = input['incremental_column']
            self.last_run_time = input['last_run_time']
            self.second_last_run_time = input['second_last_run_time']
            self.join_columns = input['join_columns']
            self.log_mode = input['log_mode']
            self.data_path = input['data_path']
            self.load_id = input['load_id']
            self.plant_name = input['plant_name']
            # Special logic to mirror table from one schema in GP to a different schema in HIVE
            self.is_special_logic = input.get('is_special_logic', False)
            if self.data_path.find("GP2HDFS") <> -1 and self.target_schemaname <> self.source_schemaname and self.is_special_logic:
                self.source_schemaname = self.target_schemaname
        else:
            print "[File2DBSync: __init__] - No data defined"


    def load_hive(self, control=None):
        method_name = self.class_name + ": " + 'load_hive'

        self.technology = 'Python'
        self.system_name = 'HIVE'
        self.job_name = 'HDFS-->HIVE'
        # status = "Started Spark Job"
        # Overwrite data path for Hive load
        # self.data_path = "HDFS2MIR"

        print_hdr = "[" + method_name + ": " + self.data_path + ": " + str(self.load_id) + "] - "
        print (method_name + ": Entered")

        error = 0
        err_msg = ''

        metastore_dbName        = self.config_list['meta_db_dbName']
        dbmeta_Url              = self.config_list['meta_db_dbUrl']
        dbmeta_User             = self.config_list['meta_db_dbUser']
        dbmeta_Pwd              = base64.b64decode(self.config_list['meta_db_dbPwd'])
        emailSender             = self.config_list['email_sender']
        emailReceiver           = self.config_list['email_receivers']
        path                    = self.config_list['misc_hdfsStagingPath']

        if control is None:
            control = {'run_id': None, 'v_timestamp': None, 'rows_inserted': None, 'rows_updated': None, 'rows_deleted': None, 'num_errors': None}

        if (self.source_schemaname is None) or (self.source_tablename is None):
            error = 1
            err_msg = method_name + "[{0}]: No table name provided".format(error)
            status = 'Job Error'
            output_msg = "No table name provided"
            audit_logging(None, self.load_id, None, self.plant_name, self.system_name, self.job_name, None, status, self.data_path,
                          self.technology, 0, 0, 0, error, err_msg, 0, 0, output_msg)
            # remove_files(self.config_list['misc_hdfsStagingPath'], self.source_schemaname, self.source_tablename)
            self.error_cleanup(self.source_schemaname, self.source_tablename, control['run_id'], path)
            return error, err_msg
        else:
            tablename       = self.source_schemaname + '.' + self.source_tablename

        rows_inserted       = control['rows_inserted'] if control.has_key('rows_inserted') and control['rows_inserted'] is not None else 0
        rows_updated        = control['rows_updated'] if control.has_key('rows_updated') and control['rows_updated'] is not None else 0
        rows_deleted        = control['rows_deleted'] if control.has_key('rows_deleted') and control['rows_deleted'] is not None else 0
        # num_errors          = control['num_errors'] if control.has_key('num_errors') and control['num_errors'] is not None else 1
        output_msg          = ''
        conn_metadata       = None

        try:
            conn_metadata, cur_metadata = dbConnect(metastore_dbName, dbmeta_User, dbmeta_Url, dbmeta_Pwd)
        except Exception as e:
            error = 2
            err_msg = method_name + "[{0}]: Error getting connection from database".format(error)
            # remove_files(self.config_list['misc_hdfsStagingPath'], self.source_schemaname, self.source_tablename)
            status = 'Job Error'
            output_msg   = traceback.format_exc()
            audit_logging(cur_metadata, self.load_id, 0, self.plant_name, self.system_name, self.job_name, tablename,status, \
                          self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, error, err_msg ,0,0,output_msg)
            self.error_cleanup(self.source_schemaname, self.source_tablename, control['run_id'], path, conn_metadata)
            return error, err_msg

        run_id = 0
        if control.has_key('run_id') and control['run_id'] is not None:
            run_id          = control['run_id']
        else:
            try:
                run_id_sql = "select nextval('sbdt.edl_run_id_seq')"
                run_id_lists = dbQuery(cur_metadata, run_id_sql)
                run_id_list = run_id_lists[0]
                run_id = run_id_list['nextval']
                print (print_hdr + "Run ID for the table " + tablename + " is: " + str(run_id))
            except Exception as e:
                error = 3
                err_msg = method_name + "[{0}]: Error connecting to database while fetching metadata".format(error)
                # remove_files(self.config_list['misc_hdfsStagingPath'], self.source_schemaname, self.source_tablename)
                status = 'Job Error'
                output_msg = traceback.format_exc()
                audit_logging(cur_metadata, self.load_id, 0, self.plant_name, self.system_name, self.job_name,
                              tablename, status, self.data_path, self.technology, rows_inserted, rows_updated,
                              rows_deleted, error, err_msg, 0, 0, output_msg)
                self.error_cleanup(self.source_schemaname, self.source_tablename, run_id, path, conn_metadata)
                return error, err_msg

        if control.has_key('v_timestamp') and  control['v_timestamp'] is not None:
            v_timestamp     = control['v_timestamp']
        else:
            t = datetime.fromtimestamp(time.time())
            v_timestamp = str(t.strftime('%Y-%m-%d %H:%M:%S'))

        try:
            # print "before call of spark submit"

            call_spark_submit(self.config_list['spark_params_sparkVersion'], self.config_list['spark_params_executorMemory'], \
                              self.config_list['spark_params_executorCores'], self.config_list['spark_params_driverMemory'], \
                              self.config_list['spark_params_loadScript'], self.source_schemaname, self.source_tablename, \
                              self.load_id, run_id, self.data_path, v_timestamp)

            status = ''
            if self.log_mode == 'DEBUG':
                audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename, status, \
                              self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, error, err_msg ,0,0,output_msg)
        except Exception as e:
            error        = 4
            err_msg      = method_name + "[{0}]: Error while running spark submit".format(error)
            status       = 'Job Error'
            # remove_files(self.config_list['misc_hdfsStagingPath'], self.source_schemaname, self.source_tablename)
            output_msg   = traceback.format_exc()
            audit_logging(cur_metadata, self.load_id,run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                          self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, error, err_msg ,0,0,output_msg)
            self.error_cleanup(self.source_schemaname, self.source_tablename, run_id, path, conn_metadata)
            return error, err_msg
        return error, err_msg


    def sync_hive_wrapper(self, sc, table_name, load_id, run_id, data_path, v_timestamp):
        method_name = self.class_name + ": " + 'sync_hive_wrapper'
        print_hdr = "[" + method_name + ": " + data_path + ": " + str(load_id) + ": " + table_name + ": " + str(run_id) + "] - "
        print (print_hdr + "Entered")

        output = {}
        self.plant_name = 'DATASYNC'
        self.technology = 'PySpark'
        self.system_name = 'HIVE'

        self.data_path = data_path if data_path is not None else ""
        self.load_id = load_id if load_id is not None else -1
        input_schema_name = ''
        input_table_name = ''
        error = 0
        err_msg = ''

        if self.data_path.find("HDFS2MIR") <> -1:
            self.job_name = 'HDFS-->HIVE'
        elif self.data_path.find("MIR2") <> -1:
            self.job_name = 'HIVE-->HIVE'

        status = "Job Started"
        audit_logging('', self.load_id, run_id, self.plant_name, self.system_name, self.job_name, table_name, status, self.data_path,
                      self.technology, 0, 0, 0, error, err_msg, 0, 0, '')

        if table_name is not None and table_name.find('.') <> -1:
            input_schema_name, input_table_name = table_name.split('.')

        if (sc is None) or (table_name is None) or (load_id is None) or (run_id is None) or (data_path is None) or (v_timestamp is None) or (table_name.find('.') == -1):
            error = 1
            err_msg = method_name + "[{0}]: Input Entry Missing".format(error)
            status = 'Job Error'
            output_msg = "Input Entry Missing"

            output.update({'status': status, 'rows_inserted': 0, 'rows_updated': 0,'rows_deleted': 0,
                           'error': error, 'err_msg': err_msg,'output_msg': output_msg})
            self.error_cleanup(input_schema_name, input_table_name, run_id)

        else:
            module_path = str(os.path.dirname(os.path.realpath(__file__))) + "/"
            method = "sync_hive_spark"

            output = call_datasync_method(module_path, self, method, sc, table_name, load_id, run_id, self.data_path, v_timestamp)

            if output['error'] > 0:
                self.error_cleanup(input_schema_name, input_table_name, run_id)
            else:
                rows_inserted = output.get('rows_inserted', 0)
                rows_updated = output.get('rows_updated', 0)
                rows_deleted = output.get('rows_deleted', 0)
                conn_metadata = None

                try:
                    if self.archived_enabled:
                        archive_path = 's3a://' + self.config_list['s3_bucket_name'] + '/' + self.target_schema + '/' + self.target_tablename + '_bkp/'
                        if self.s3_backed:
                            source_path = 's3a://' + self.config_list['s3_bucket_name'] + '/' + self.target_schema + '/' + self.target_tablename + '/'
                        else:
                            source_path = 'hdfs:/' + self.config_list['misc_hiveWarehousePath'] + self.target_schema + '.db/' + self.target_tablename + '/*'

                        print (print_hdr + "source_path: " + source_path)
                        print (print_hdr + "archive_path: " + archive_path)

                        (ret, out, err) = run_cmd(['hadoop', 'distcp', source_path, archive_path])
                        print (print_hdr + "distcp files between source and archive:" + str(ret))

                        if err:
                            error = 1
                            err_msg = method_name + "[{0}]: Error while loading data in archive location".format(error)
                            status = 'Job Error'
                            output_msg = traceback.format_exc()
                            output.update({'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated,'rows_deleted': rows_deleted,
                                           'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
                            self.error_cleanup(input_schema_name, input_table_name, run_id,self.config_list['misc_hdfsStagingPath'], conn_metadata)
                            return output

                    conn_metadata, cur_metadata = dbConnect(self.config_list['meta_db_dbName'],self.config_list['meta_db_dbUser'],
                                                            self.config_list['meta_db_dbUrl'],base64.b64decode(self.config_list['meta_db_dbPwd']))

                    update_control_table_sql = "UPDATE sync.control_table \
                                            SET last_run_time = '" + v_timestamp + "', \
                                                second_last_run_time = last_run_time, \
                                                status_flag = 'Y', \
                                                load_status_cd = '" + self.CONTROL_STATUS_COMPLETED + "' \
                                            WHERE target_schemaname = '" + input_schema_name + "'"

                    if self.data_path == 'HDFS2MIR':
                        update_control_table_sql = update_control_table_sql \
                                                   + " AND target_tablename in( '" + input_table_name + "','" + input_table_name + "_ext')" \
                                                   + " AND data_path in ('GP2HDFS','HDFS2MIR')"
                    else:
                        update_control_table_sql = update_control_table_sql \
                                                   + " AND target_tablename in( '" + input_table_name + "')" \
                                                   + " AND data_path in ('" + self.data_path + "')"

                    print (print_hdr + "update_control_table_sql: " + update_control_table_sql)
                    cur_metadata.execute(update_control_table_sql)
                    if self.log_mode == 'DEBUG':
                        status = "Updated Control Table"
                        audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name,table_name, status,
                                      self.data_path, self.technology, rows_inserted, rows_updated, rows_deleted, error,err_msg, 0, 0, None)

                    error = 0
                    err_msg = method_name + "[{0}]: No Errors".format(error)
                    status = 'Job Finished'
                    output_msg = 'Job Finished successfully'
                    output.update({'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated,'rows_deleted': rows_deleted,
                                   'error': error, 'err_msg': err_msg, 'output_msg': output_msg})

                except Exception as e:
                    error = 2
                    err_msg = method_name + "[{0}]: Error while updating the control table".format(error)
                    status = 'Job Error'
                    output_msg = traceback.format_exc()
                    output.update({'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated,'rows_deleted': rows_deleted,
                                   'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
                finally:
                    if conn_metadata is not None:
                        conn_metadata.close()

        remove_files(self.config_list['misc_hdfsStagingPath'], input_schema_name, input_table_name)

        audit_logging('', load_id, run_id, self.plant_name, self.system_name, self.job_name, table_name, output["status"], self.data_path,
                      self.technology, output["rows_inserted"], output["rows_updated"], output["rows_deleted"], output["error"], output["err_msg"], 0, 0, output["output_msg"])

        if len(output) == 0:
            output = {"error": 0, "err_msg": ''}
        return output


    def sync_hive_spark(self, sc, table_name, load_id, run_id, data_path, v_timestamp):
        # from pyspark import SparkConf, SparkContext
        # from pyspark.sql import SQLContext
        from pyspark.sql import HiveContext

        method_name = self.class_name + ": " + 'sync_hive_spark'
        print_hdr = "[" + method_name + ": " + data_path + ": " + str(load_id) + ": " + table_name + ": " + str(run_id) + "] - "
        print (print_hdr + "Entered")

        # mirror_load_flag = False
        # if data_path.find("HDFS2MIR") <> -1:
        #     mirror_load_flag = True

        error = 0
        err_msg = ''
        output = {}

        # num_errors = 1
        rows_inserted = 0
        rows_updated = 0
        rows_deleted = 0
        output = {}
        conn_metadata = None

        # self.data_path = data_path if data_path is not None else ""
        # self.load_id = load_id if load_id is not None else -1
        # input_schema_name = ''
        # input_table_name = ''

        # if table_name is not None and table_name.find('.') <> -1:
        #     input_schema_name, input_table_name = table_name.split('.')
        #
        # if (sc is None) or (table_name is None) or (load_id is None) or (run_id is None) or (data_path is None) or (v_timestamp is None) or (table_name.find('.') == -1):
        #     error = 1
        #     err_msg = method_name + "[{0}]: Input Entry Missing".format(error)
        #     status = 'Job Error'
        #     output_msg = "Input Entry Missing"
        #
        #     output.update({'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated,'rows_deleted': rows_deleted,
        #                    'error': error, 'err_msg': err_msg,'output_msg': output_msg})
        #     self.error_cleanup(input_schema_name, input_table_name, run_id)
        #     return output

        input_schema_name, input_table_name = table_name.split('.')
        try:
            sqlContext = HiveContext(sc)

            if (self.config_list == None):
                # print (print_hdr + "Configuration Entry Missing")
                error = 2
                err_msg = method_name + "[{0}]: Configuration Entry Missing".format(error)
                status = 'Job Error'
                output_msg = "Configuration Entry Missing"
                # if mirror_load_flag:
                #     audit_logging(None, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, table_name, status, self.data_path,
                #                   self.technology, rows_inserted, rows_updated, rows_deleted, error, err_msg, 0, 0, output_msg)
                #     output.update({'error': error, 'err_msg': err_msg})
                # else:
                output.update({'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated, 'rows_deleted': rows_deleted,
                               'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
                self.error_cleanup(input_schema_name, input_table_name, run_id)
                return output


            # get the current branch (from local.properties)
            # env = config.get('branch', 'env')

            # proceed to point everything at the 'branched' resources
            dbUrl                       = self.config_list['mysql_dbUrl']
            dbUser                      = self.config_list['mysql_dbUser']
            dbPwd                       = base64.b64decode(self.config_list['mysql_dbPwd'])
            dbMetastore_dbName          = self.config_list['mysql_dbMetastore_dbName']
            dbApp_dbName                = self.config_list['mysql_dbApp_dbName']
            bucket_name                 = self.config_list['s3_bucket_name']
            # Adding psql details
            metastore_dbName            = self.config_list['meta_db_dbName']
            dbmeta_Url                  = self.config_list['meta_db_dbUrl']
            dbmeta_User                 = self.config_list['meta_db_dbUser']
            dbmeta_Pwd                  = base64.b64decode(self.config_list['meta_db_dbPwd'])
            paths                       = self.config_list['misc_hdfsStagingPath']

            # print (print_hdr, dbUrl, dbUser, dbMetastore_dbName, dbApp_dbName)

            run_id_sql = "select nextval('sbdt.edl_run_id_seq')"
            # if mirror_load_flag:
            #     self.plant_name = 'DATASYNC'
            #     self.technology = 'PySpark'
            #     self.system_name = 'HIVE'
            #     self.job_name = 'HDFS-->HIVE'

            tablename = input_schema_name + "." + input_table_name
            conn_metadata = None

            # Adding psql connection for control table data
            try:
                metadata_sql = "SELECT * FROM sync.control_table \
                        WHERE target_tablename = '" + input_table_name + "' \
                            AND target_schemaname = '" + input_schema_name + "'" + " \
                            AND data_path = " + "'" + self.data_path + "'"

                print (print_hdr + "metadata_sql: " + metadata_sql)
                conn_metadata, cur_metadata = dbConnect(metastore_dbName, dbmeta_User, dbmeta_Url, dbmeta_Pwd)
                print (print_hdr + "before connecting to metastore controls")
                controls = dbQuery(cur_metadata, metadata_sql)
                # controls = cur_metadata.execute(metadata_sql)
                # print (controls.fetchall())
                print (print_hdr + "metastore controls:", controls)
            except psycopg2.Error as e:
                error = 3
                err_msg = method_name + "[{0}]: Error connecting to control table database".format(error)
                status = 'Job Error'
                output_msg = traceback.format_exc()
                # if mirror_load_flag:
                #     audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename, status, self.data_path,
                #                   self.technology, rows_inserted, rows_updated, rows_deleted, error, err_msg, 0, 0, output_msg)
                #     output.update({'error': error, 'err_msg': err_msg})
                # else:
                output.update({'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated, 'rows_deleted': rows_deleted,
                               'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
                # remove_files(paths, input_schema_name, input_table_name)
                self.error_cleanup(input_schema_name, input_table_name, run_id, paths, conn_metadata)
                return output

            if not controls:
                # err_msg = "No Entry found in metadata table"
                error = 4
                # err_msg = "no entry in psql metastore"
                err_msg = method_name + "[{0}]: No Entry found in control table".format(error)
                status = 'Job Error'
                output_msg = "No Entry found in control table"
                # if mirror_load_flag:
                #     audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename, status, self.data_path,
                #                   self.technology, rows_inserted, rows_updated, rows_deleted, error, err_msg, 0, 0, output_msg)
                #     output.update({'error': error, 'err_msg': err_msg})
                # else:
                output.update({'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated, 'rows_deleted': rows_deleted,
                               'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
                # remove_files(paths, input_schema_name, input_table_name)
                self.error_cleanup(input_schema_name, input_table_name, run_id, paths, conn_metadata)
                return output

            self.id = str(controls[0]['id'])
            self.source_schema = str(controls[0]['source_schemaname'])
            self.source_tablename = str(controls[0]['source_tablename'])
            self.target_schema = str(controls[0]['target_schemaname'])
            self.target_tablename = str(controls[0]['target_tablename'])
            partitioned = controls[0]['is_partitioned']
            self.load_type = str(controls[0]['load_type'])
            self.s3_backed = controls[0]['s3_backed']
            first_partitioned_column = str(controls[0]['first_partitioned_column'])
            second_partitioned_column = str(controls[0]['second_partitioned_column'])
            partitioned_column_transformation = str(controls[0]['partition_column_transformation'])
            custom_sql = str(controls[0]['custom_sql'])
            self.join_columns = str(controls[0]['join_columns'])
            self.archived_enabled = controls[0]['archived_enabled']
            distribution_columns = str(controls[0]['distribution_columns'])
            dist_col_transformation = str(controls[0]['dist_col_transformation'])
            self.log_mode = str(controls[0]['log_mode'])

            #######################################################################################################
            # print distribution_columns, dist_col_transformation, partitioned
            print (print_hdr + "distribution_columns :: dist_col_transformation :: partitioned", distribution_columns, dist_col_transformation, partitioned)

            # Connection to the Hive Metastore to get column and partition list
            try:
                connection = mysql.connector.connect(user=dbUser, password=dbPwd, host=dbUrl, database=dbMetastore_dbName)
            except Exception as e:
                error = 5
                err_msg = method_name + "[{0}]: Error connecting to Hive Metastore".format(error)
                status = 'Job Error'
                output_msg = traceback.format_exc()
                # if mirror_load_flag:
                #     audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename, status, self.data_path,
                #                   self.technology, rows_inserted, rows_updated, rows_deleted, error, err_msg, 0, 0, output_msg)
                #     output.update({'error': error, 'err_msg': err_msg})
                # else:
                output.update({'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated, 'rows_deleted': rows_deleted,
                               'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
                # remove_files(paths, input_schema_name, input_table_name)
                self.error_cleanup(input_schema_name, input_table_name, run_id, paths, conn_metadata)
                return output

            # Establish connection to the hive metastore to get the list of columns
            try:
                cursor = connection.cursor()
                # cursor.execute("""SELECT COLUMN_NAME, TBL_NAME FROM COLUMNS_V2 c JOIN TBLS a ON c.CD_ID=a.TBL_ID where a.TBL_ID = 52""")
                # cursor.execute("""SELECT COLUMN_NAME FROM COLUMNS_V2 c JOIN TBLS a ON c.CD_ID=a.TBL_ID where a.TBL_ID = 52""")
                sql_query = "SELECT                                                                   \
                                    c.COLUMN_NAME                                                     \
                            FROM                                                                      \
                                TBLS t                                                                \
                                JOIN DBS d                                                            \
                                    ON t.DB_ID = d.DB_ID                                              \
                                JOIN SDS s                                                            \
                                    ON t.SD_ID = s.SD_ID                                              \
                                JOIN COLUMNS_V2 c                                                     \
                                    ON s.CD_ID = c.CD_ID                                              \
                            WHERE                                                                     \
                                TBL_NAME = " + "'" + self.target_tablename + "' " + "                 \
                                AND d.NAME=" + " '" + self.target_schema + "' " + "                   \
                                ORDER by c.INTEGER_IDX"

                # print (print_hdr + "hive_meta_target_sql: " + sql_query)
                cursor.execute(sql_query)
                target_result = cursor.fetchall()
                print (print_hdr + "target_result:", target_result)

                sql_query = "SELECT                                                                   \
                                    c.COLUMN_NAME                                                     \
                            FROM                                                                      \
                                TBLS t                                                                \
                                JOIN DBS d                                                            \
                                    ON t.DB_ID = d.DB_ID                                              \
                                JOIN SDS s                                                            \
                                    ON t.SD_ID = s.SD_ID                                              \
                                JOIN COLUMNS_V2 c                                                     \
                                    ON s.CD_ID = c.CD_ID                                              \
                            WHERE                                                                     \
                                TBL_NAME = " + "'" + self.source_tablename + "' " + "                 \
                                AND d.NAME=" + " '" + self.source_schema + "' " + "                   \
                                ORDER by c.INTEGER_IDX"

                # print (print_hdr + "hive_meta_source_sql: " + sql_query)
                cursor.execute(sql_query)
                source_result = cursor.fetchall()
                print (print_hdr + "source_result:", source_result)


            except Exception as e:
                error = 6
                err_msg = method_name + "[{0}]: Issue running SQL in hive metadata database:".format(error)
                status = 'Job Error'
                output_msg = traceback.format_exc()
                # if mirror_load_flag:
                #     audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename, status, self.data_path,
                #                   self.technology, rows_inserted, rows_updated, rows_deleted, error, err_msg, 0, 0, output_msg)
                #     output.update({'error': error, 'err_msg': err_msg})
                # else:
                output.update({'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated, 'rows_deleted': rows_deleted,
                               'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
                # remove_files(paths, self.source_schema, input_table_name)
                self.error_cleanup(self.source_schema, input_table_name, run_id, paths, conn_metadata)
                return output
            finally:
                connection.close()

            new_target_result = target_result

            from ast import literal_eval

            if distribution_columns != 'None':
                col_check = literal_eval("('" + distribution_columns + "',)")
                print (print_hdr + "col_check:", col_check)
                if col_check in target_result:
                    target_result.remove(col_check)
                    print (print_hdr + "Distribution Column Available",)
                else:
                    print (print_hdr + "Didn't find distribution column")
            else:
                print (print_hdr + "No distribution column")

            # Get the column on which the table is partitioned
            source_select_list = ', '.join(map(''.join, target_result))
            target_select_list = ', '.join(map(''.join, target_result))

            if not source_select_list:
                error = 7
                err_msg = method_name + "[{0}]: Hive Table Not Found in metadata database".format(error)
                status = 'Job Error'
                output_msg = traceback.format_exc()
                # if mirror_load_flag:
                #     audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename, status, self.data_path,
                #                   self.technology, rows_inserted, rows_updated, rows_deleted, error, err_msg, 0, 0, output_msg)
                #     output.update({'error': error, 'err_msg': err_msg})
                # else:
                output.update({'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated, 'rows_deleted': rows_deleted,
                               'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
                # remove_files(paths, self.source_schema, input_table_name)
                self.error_cleanup(self.source_schema, input_table_name, run_id, paths, conn_metadata)
                return output

            last_updated_by = str('data_sync')

            audit_date_column = "'" + str(datetime.now()) + "' as hive_updated_date "
            audit_updated_by_column = "'" + last_updated_by + "' as hive_updated_by "

            # in_temp = source_select_list.replace("hive_updated_by",audit_updated_by_column)
            # in_temp_1 = in_temp.replace("hive_updated_date",audit_date_column)
            # source_select_list = in_temp_1

            # Create the SELECT query string for fetching data from the external table
            # if dist_col_transformation != 'None':
            if dist_col_transformation != 'None' and partitioned:
                target_select = target_select_list + ',' + distribution_columns
                # source_select = source_select_list + ' , ' + dist_col_transformation
                source_select = source_select_list + ', ' + dist_col_transformation
            else:
                target_select = target_select_list
                source_select = source_select_list

            print "======================================================"
            print (print_hdr + "printing source_select...")
            print source_select
            print "======================================================"
            print (print_hdr + "partitioned_column_transformation:", partitioned_column_transformation)

            tmp_source_tablename = self.source_tablename
            if self.data_path.find("MIR2") <> -1:
                tmp_source_tablename = self.source_tablename + '_tmp'

            if (partitioned):
                # incremental_sql_query = 'select ' + source_select + ', ' + partitioned_column_transformation + ' from ' + self.source_schema + '.' + self.source_tablename
                incremental_sql_query = 'select ' + source_select + ', ' + partitioned_column_transformation + ' from ' + self.source_schema + '.' + tmp_source_tablename
                if second_partitioned_column <> 'None':
                    target_sql_query = 'select ' + target_select + ', ' + first_partitioned_column + ', ' + second_partitioned_column + ' from ' + self.target_schema + '.' + self.target_tablename
                else:
                    target_sql_query = 'select ' + target_select + ', ' + first_partitioned_column + ' from ' + self.target_schema + '.' + self.target_tablename
            else:
                # incremental_sql_query = 'select ' + source_select + ' from ' + self.source_schema + '.' + self.source_tablename
                incremental_sql_query = 'select ' + source_select + ' from ' + self.source_schema + '.' + tmp_source_tablename
                target_sql_query = 'select ' + target_select + ' from ' + self.target_schema + '.' + self.target_tablename

            connection = mysql.connector.connect(user=dbUser, password=dbPwd, host=dbUrl, database=dbMetastore_dbName)
            try:
                cursor = connection.cursor()
                # cursor.execute("""SELECT COLUMN_NAME, TBL_NAME FROM COLUMNS_V2 c JOIN TBLS a ON c.CD_ID=a.TBL_ID where a.TBL_ID = 52""")
                # cursor.execute("""SELECT COLUMN_NAME FROM COLUMNS_V2 c JOIN TBLS a ON c.CD_ID=a.TBL_ID where a.TBL_ID = 52""")
                bloom_sql_query = "SELECT e.PARAM_VALUE                                                 \
                                   FROM                                                                 \
                                        TBLS t                                                          \
                                   JOIN DBS d                                                           \
                                      ON t.DB_ID = d.DB_ID                                              \
                                   LEFT OUTER JOIN TABLE_PARAMS e                                       \
                                        ON t.TBL_ID = e.TBL_ID                                          \
                                        AND PARAM_KEY = 'orc.bloom.filter.columns'                      \
                                   WHERE                                                                \
                                        TBL_NAME = " + "'" + self.target_tablename + "' " + "           \
                                        AND d.NAME=" + " '" + self.target_schema + "' "                 \

                cursor.execute(bloom_sql_query)
                bloom_filter = cursor.fetchall()
            except Exception as e:
                error = 8
                err_msg = method_name + "[{0}]: Issue running SQL in hive metadata database to get bloom filter details".format(error)
                status = 'Job Error'
                output_msg = traceback.format_exc()
                # if mirror_load_flag:
                #     audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename, status, self.data_path,
                #                   self.technology, rows_inserted, rows_updated, rows_deleted, error, err_msg, 0, 0, output_msg)
                #     output.update({'error': error, 'err_msg': err_msg})
                # else:
                output.update({'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated, 'rows_deleted': rows_deleted,
                               'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
                # remove_files(paths, self.source_schema, input_table_name)
                self.error_cleanup(self.source_schema, input_table_name, run_id, paths, conn_metadata)
                return output
            finally:
                connection.close()

            bloom_filters_columns = ''
            if len(bloom_filter) > 1:
                bloom_filters_columns = ','.join(map(''.join, bloom_filter))
                bloom_filter_list = bloom_filters_columns.split(",")
                print (print_hdr + "bloom_filter_list: ", bloom_filter_list)

            # Execute the query to get the data into Spark Memory

            # Figure out if it is incremental or full load process
            # If Full Load then truncate the target table and insert the entire incoming data
            # If Incremental Load then determine if the table is partitioned as the logic needs to be handled differently for partitioned and non-partitioned tables
            #      For Non Partitioned Tables merge the incoming data with the table date and save it to the database
            #      For Partitioned Tables identify the partitions for which there is incremental data and intelligently merge the data and save it to the database table
            table_name = self.target_schema + '.' + self.target_tablename
            sqlContext.setConf("hive.exec.dynamic.partition", "true")
            sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
            sqlContext.setConf("spark.sql.orc.filterPushdown", "true")
            sqlContext.setConf("mapred.input.dir.recursive", "true")
            sqlContext.setConf("hive.mapred.supports.subdirectories", "true")

            sc._jsc.hadoopConfiguration().set('fs.s3a.attempts.maximum', '30')

            partitioned_columns = ''
            # if self.s3_backed:
            #     # path = 's3a://' + bucket_name + '/' + self.target_schema + '/' + self.target_tablename + '/'
            #     target_path = 's3a://' + bucket_name + '/' + self.target_schema + '/' + self.target_tablename + '/'
            # else:
            #     # path = '/apps/hive/warehouse/' + self.target_schema + '.db/' + self.target_tablename + '/'
            #     target_path = '/apps/hive/warehouse/' + self.target_schema + '.db/' + self.target_tablename + '/'

            target_tmp_table = self.target_tablename + '_tmp'
            # temp_path = self.config_list['misc_hdfsStagingPath'] + self.target_schema + '/' + temp_table + '/'
            # target_tmp_path = self.config_list['misc_hdfsStagingPath'] + self.target_schema + '.db/' + target_tmp_table + '/'
            target_tmp_path = self.config_list['misc_hdfsStagingPath'] + self.target_schema + '/' + target_tmp_table + '/'

            # print (print_hdr + "target_path: " + target_path)
            print (print_hdr + "target_tmp_path: " + target_tmp_path)

            (ret_rm, out_rm, err_rm) = run_cmd(['hadoop', 'fs', '-rm', '-r', '-f', (target_tmp_path + "*")])
            print (print_hdr + "removing files from target tmp - STATUS: " + str(ret_rm))

            if second_partitioned_column <> 'None':
                partitioned_columns = first_partitioned_column + "," + second_partitioned_column
            else:
                partitioned_columns = first_partitioned_column

            if distribution_columns != 'None' and partitioned_columns != 'None':
                bucket_columns = partitioned_columns + "," + distribution_columns
                bucket_column_list = bucket_columns.split(",")
            elif partitioned_columns != 'None':
                bucket_columns = partitioned_columns
                bucket_column_list = bucket_columns.split(",")
            else:
                bucket_columns = distribution_columns
                bucket_column_list = bucket_columns.split(",")

            if partitioned_columns != 'None':
                partition_column_list = partitioned_columns.split(",")
                print (print_hdr + "Inside Partition Split: ", partition_column_list)

            print (print_hdr + "incremental_sql_query: " + incremental_sql_query)
            print "=================================================="
            print (print_hdr + "target_sql_query: " + target_sql_query)
            print "=================================================="

            from pyspark.sql.functions import col

            print (print_hdr + "load_type: " + self.load_type)
            if self.load_type == 'FULL':
                try:
                    merge_df = sqlContext.sql(incremental_sql_query)
                    rows_inserted = merge_df.count()

                    if partitioned:
                        merge_df.cache()
                        # rows_inserted = merge_df.count()
                        # print (print_hdr + "FULL: Partitioned: bucket_column_list: ", bucket_column_list)
                        # repartition_cnt = len(merge_df.select(bucket_column_list).distinct().collect())
                        # print (print_hdr + "FULL: Partitioned: repartition_cnt: " + str(repartition_cnt))

                        if bloom_filters_columns:
                            final_df = merge_df.repartition(len(merge_df.select(bucket_column_list).distinct().collect()),
                                                            bucket_column_list) \
                                .sortWithinPartitions(bloom_filter_list)
                        else:
                            final_df = merge_df.repartition(len(merge_df.select(bucket_column_list).distinct().collect()),
                                                            bucket_column_list)

                            # final_df = merge_df.repartition(repartition_cnt, bucket_column_list)

                        # final_df.write.option("compression", "zlib").mode("overwrite").format("orc").partitionBy(partition_column_list).save(path)
                        # final_df.write.option("compression", "zlib").mode("overwrite").format("orc").partitionBy(partition_column_list).save(target_path)
                        final_df.write.option("compression", "zlib").mode("overwrite").format("orc").partitionBy(partition_column_list).save(target_tmp_path)
                        print (print_hdr + "FULL: Partitioned: rows_inserted: " + str(rows_inserted))

                    else:
                        if merge_df.rdd.getNumPartitions() > 300:
                            merge_coalesce_df = merge_df.coalesce(300)
                        else:
                            merge_coalesce_df = merge_df

                        if bloom_filters_columns:
                            save_df = merge_coalesce_df.sortWithinPartitions(bloom_filter_list)
                            # save_df.write.option("compression", "zlib").mode("overwrite").format("orc").save(path)
                            # save_df.write.option("compression", "zlib").mode("overwrite").format("orc").save(target_path)
                            save_df.write.option("compression", "zlib").mode("overwrite").format("orc").save(target_tmp_path)
                            print (print_hdr + "FULL: Non-Partitioned: Bloom Filter: rows_inserted: " + str(rows_inserted))
                        else:
                            # print (print_hdr + "Inside Non Partitioned Non Bloom Filter Logic")
                            # merge_coalesce_df.write.option("compression", "zlib").mode("overwrite").format("orc").save(path)
                            # merge_coalesce_df.write.option("compression", "zlib").mode("overwrite").format("orc").save(target_path)
                            merge_coalesce_df.write.option("compression", "zlib").mode("overwrite").format("orc").save(target_tmp_path)
                            print (print_hdr + "FULL: Non-Partitioned: Non-Bloom Filter: rows_inserted: " + str(rows_inserted))

                    self.update_control(input_schema_name, input_table_name, self.CONTROL_STATUS_INPROGRESS, run_id)
                    # (ret_rm, out_rm, err_rm) = run_cmd(['hadoop', 'fs', '-rm', '-r', '-f', (target_path + "*")])
                    # print (print_hdr + "FULL: removing files from target - STATUS: " + str(ret_rm))
                    #
                    # # Use mv for inter-hdfs and distcp between hdfs and s3
                    # (ret_mv, out_mv, err_mv) = run_cmd(['hadoop', 'fs', '-mv', (temp_path + "*"), target_path])
                    # print (print_hdr + "FULL: moving files from temp to target - STATUS: " + str(ret_mv))

                    # if ret_rm <> 0 or ret_mv <> 0:
                    #     error = ret_rm if ret_mv == 0 else ret_mv
                    #     err_msg = method_name + "[{0}]: Error with unix commnds".format(error)
                    #     status = 'Job Error'
                    #     output_msg = err_rm[0:3997] if ret_mv == 0 else err_mv[0:3997]
                    #     # if mirror_load_flag:
                    #     #     audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename, status,
                    #     #                   self.data_path, self.technology, rows_inserted, rows_updated, rows_deleted, error,err_msg, 0, 0, output_msg)
                    #     #     output.update({'error': error, 'err_msg': err_msg})
                    #     # else:
                    #     output.update({'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated, 'rows_deleted': rows_deleted,
                    #                    'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
                    #     self.error_cleanup(self.source_schema, input_table_name, run_id, paths, conn_metadata)
                    #     return output

                    # if partitioned:
                    #     # analyze_table_sql = 'ANALYZE TABLE ' + table_name + 'partition (' + partition_column_list + ') COMPUTE STATISTICS'
                    #     analyze_table_sql = 'ANALYZE TABLE ' + table_name + ' partition (' + partitioned_columns + ') COMPUTE STATISTICS'
                    #     print (print_hdr + "analyze_table_sql: " + analyze_table_sql)
                    #     sqlContext.sql(analyze_table_sql)
                    #
                    #     repair_table_sql = 'MSCK REPAIR TABLE ' + table_name
                    #     print (print_hdr + "repair_table_sql: " + repair_table_sql)
                    #     sqlContext.sql(repair_table_sql)
                    # else:
                    #     analyze_table_sql = 'ANALYZE TABLE ' + table_name + ' COMPUTE STATISTICS'
                    #     print (print_hdr + "analyze_table_sql: " + analyze_table_sql)
                    #     sqlContext.sql(analyze_table_sql)

                except Exception as e:
                    error = 9
                    err_msg = method_name + "[{0}]: Error while doing a full load".format(error)
                    status = 'Job Error'
                    output_msg = traceback.format_exc()
                    # if mirror_load_flag:
                    #     audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename, status,
                    #                   self.data_path, self.technology, rows_inserted, rows_updated, rows_deleted, error, err_msg, 0, 0,
                    #                   output_msg)
                    #     output.update({'error': error, 'err_msg': err_msg})
                    # else:
                    output.update({'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated, 'rows_deleted': rows_deleted,
                                   'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
                    # remove_files(paths, self.source_schema, input_table_name)
                    self.error_cleanup(self.source_schema, input_table_name, run_id, paths, conn_metadata)
                    return output
            # Incremental Logic for Append Only table especially for S3
            elif self.load_type == 'APPEND_ONLY':
                try:
                    merge_df = sqlContext.sql(incremental_sql_query)
                    rows_inserted = merge_df.count()
                    # if s3_backed:
                    #     temp_table = self.target_tablename + '_tmp'
                    #     temp_path = '/apps/hive/warehouse/' + self.target_schema + '.db/' + temp_table + '/*'
                    #     #                write_path = '/apps/hive/warehouse/'  + target_schema + '.db/' + temp_table + '/'
                    #     (ret, out, err) = run_cmd(['hadoop', 'fs', '-rm', '-r', temp_path])
                    #     print (print_hdr + "Return for removing files from temp:" + str(ret))
                    # else:
                    #     temp_table = self.target_tablename
                    #     # temp_path = path
                    #     temp_path = target_path

                    # write_path = '/apps/hive/warehouse/' + self.target_schema + '.db/' + temp_table + '/'

                    # print (print_hdr + "temp_path: " + temp_path)
                    # print (print_hdr + "write_path: " + write_path)

                    # temp_tbl = self.target_schema + '.' + temp_table
                    if partitioned:
                        if bloom_filters_columns:
                            final_df = merge_df.repartition(len(merge_df.select(bucket_column_list).distinct().collect()),
                                                            bucket_column_list) \
                                .sortWithinPartitions(bloom_filter_list)
                        else:
                            final_df = merge_df.repartition(len(merge_df.select(bucket_column_list).distinct().collect()),
                                                            bucket_column_list)

                        # final_df.write.option("compression", "zlib").mode("append").format("orc").partitionBy(partition_column_list).save(write_path)
                        final_df.write.option("compression", "zlib").mode("overwrite").format("orc").partitionBy(partition_column_list).save(target_tmp_path)
                        print (print_hdr + "APPEND_ONLY: Partitioned: rows_inserted: " + str(rows_inserted))

                    else:
                        if merge_df.rdd.getNumPartitions() > 300:
                            merge_coalesce_df = merge_df.coalesce(300)
                        else:
                            merge_coalesce_df = merge_df

                        if bloom_filters_columns:
                            save_df = merge_coalesce_df.sortWithinPartitions(bloom_filter_list)
                            # save_df.write.option("compression", "zlib").mode("append").format("orc").save(write_path)
                            save_df.write.option("compression", "zlib").mode("overwrite").format("orc").save(target_tmp_path)
                            print (print_hdr + "APPEND_ONLY: Non-Partitioned: Bloom Filter: rows_inserted: " + str(rows_inserted))
                        else:
                            # merge_coalesce_df.write.option("compression", "zlib").mode("append").format("orc").save(write_path)
                            merge_coalesce_df.write.option("compression", "zlib").mode("overwrite").format("orc").save(target_tmp_path)
                            print (print_hdr + "APPEND_ONLY: Non-Partitioned: Non-Bloom Filter: rows_inserted: " + str(rows_inserted))

                except Exception as e:
                    error = 10
                    err_msg = method_name + "[{0}]: Error while loading data in temporary hdfs location for append only load".format(error)
                    status = 'Job Error'
                    output_msg = traceback.format_exc()
                    # if mirror_load_flag:
                    #     audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename, status,
                    #                   self.data_path, self.technology, rows_inserted, rows_updated, rows_deleted, error, err_msg, 0, 0,output_msg)
                    #     output.update({'error': error, 'err_msg': err_msg})
                    # else:
                    output.update({'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated, 'rows_deleted': rows_deleted,
                                   'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
                    # remove_files(paths, self.source_schema, input_table_name)
                    self.error_cleanup(self.source_schema, input_table_name, run_id, paths, conn_metadata)
                    return output

                self.update_control(input_schema_name, input_table_name, self.CONTROL_STATUS_INPROGRESS, run_id)
                # if self.s3_backed:
                #     # target_path = 's3a://' + bucket_name + '/' + self.target_schema + '/' + self.target_tablename
                #     # source_path = 'hdfs://getnamenode/apps/hive/warehouse/' + self.target_schema + '.db/' + self.target_tablename + '_tmp' + '/*'
                #     # print (print_hdr + "source_path: " + source_path)
                #     # print (print_hdr + "target_path: " + target_path)
                #
                #     # (ret, out, err) = run_cmd(['hadoop', 'distcp', source_path, target_path])
                #     # Use mv for inter-hdfs and distcp between hdfs and s3
                #     # (ret, out, err) = run_cmd(['hadoop', 'distcp', temp_path, target_path])
                #     (ret, out, err) = run_cmd(['hadoop', 'distcp', (temp_path + "*.orc"), target_path])
                #     print (print_hdr + "APPEND_ONLY S3: distcp files from temp to s3 target:" + str(ret))
                #
                #     if ret <> 0:
                #         error = 11
                #         err_msg = method_name + "[{0}]: Error while moving files to S3 location for Append Only Tables".format(error)
                #         status = 'Job Error'
                #         output_msg = err[0:3997]
                #         # if mirror_load_flag:
                #         #     audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename, status,
                #         #                   self.data_path, self.technology, rows_inserted, rows_updated, rows_deleted, error, err_msg, 0,
                #         #                   0, output_msg)
                #         #     output.update({'error': error, 'err_msg': err_msg})
                #         # else:
                #         output.update({'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated, 'rows_deleted': rows_deleted,
                #                        'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
                #         # remove_files(paths, self.source_schema, input_table_name)
                #         self.error_cleanup(self.source_schema, input_table_name, run_id, paths, conn_metadata)
                #         remove_files('/apps/hive/warehouse/', self.target_schema + '.db/', temp_table)
                #         return output
                #
                # else:
                #     # Use mv for inter-hdfs and distcp between hdfs and s3
                #     (ret_mv, out_mv, err_mv) = run_cmd(['hadoop', 'fs', '-mv', (temp_path + "*.orc"), target_path])
                #     print (print_hdr + "APPEND_ONLY: moving files from temp to target - STATUS: " + str(ret_mv))
                #
                #     if ret_mv <> 0:
                #         error = ret_mv
                #         err_msg = method_name + "[{0}]: Error while moving files to target location for Append Only Tables".format(error)
                #         status = 'Job Error'
                #         output_msg = err_mv[0:3997]
                #         # if mirror_load_flag:
                #         #     audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename, status,
                #         #                   self.data_path, self.technology, rows_inserted, rows_updated, rows_deleted, error,err_msg, 0, 0, output_msg)
                #         #     output.update({'error': error, 'err_msg': err_msg})
                #         # else:
                #         output.update({'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated, 'rows_deleted': rows_deleted,
                #                        'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
                #         self.error_cleanup(self.source_schema, input_table_name, run_id, paths, conn_metadata)
                #         return output

                # if partitioned:
                #
                #     # analyze_table_sql = 'ANALYZE TABLE ' + table_name + 'partition (' + partition_column_list + ') COMPUTE STATISTICS'
                #     analyze_table_sql = 'ANALYZE TABLE ' + table_name + ' partition (' + partitioned_columns + ') COMPUTE STATISTICS'
                #     print (print_hdr + "analyze_table_sql: " + analyze_table_sql)
                #     sqlContext.sql(analyze_table_sql)
                #
                #     repair_table_sql = 'MSCK REPAIR TABLE ' + table_name
                #     print (print_hdr + "repair_table_sql: " + repair_table_sql)
                #     sqlContext.sql(repair_table_sql)
                #
                # else:
                #     analyze_table_sql = 'ANALYZE TABLE ' + table_name + ' COMPUTE STATISTICS'
                #     print (print_hdr + "analyze_table_sql: " + analyze_table_sql)
                #     sqlContext.sql(analyze_table_sql)

            else:
                if (partitioned):
                    try:
                        incremental_df = sqlContext.sql(incremental_sql_query)
                        rows_inserted = incremental_df.count()
                        print (print_hdr + "INCREMENTAL: Partitioned: Incremental records: " + str(rows_inserted))

                        first_partitioned_list = incremental_df.select(first_partitioned_column) \
                            .rdd.flatMap(lambda x: x).distinct().collect()

                        # print (print_hdr + "first_partitioned_list: ", first_partitioned_list)
                        # print second_partitioned_list

                        if second_partitioned_column <> 'None':
                            second_partitioned_list = incremental_df.select(second_partitioned_column) \
                                .rdd.flatMap(lambda x: x).distinct().collect()
                            merge_df = sqlContext.sql(target_sql_query) \
                                .where(col(first_partitioned_column).isin(first_partitioned_list) & \
                                       col(second_partitioned_column).isin(second_partitioned_list))
                        else:
                             merge_df = sqlContext.sql(target_sql_query) \
                                .where(col(first_partitioned_column).isin(first_partitioned_list))

                        # print (print_hdr + "merge_df.count: " + str(merge_df.count()))
                        join_column_list = self.join_columns.split(",")
                        output_df = merge_df.join(incremental_df, join_column_list, "leftanti")
                        # print (print_hdr + "output_df.count: " + str(output_df.count()))

                        final_df = output_df.union(incremental_df)
                        # print (print_hdr + "final_df.count: " + str(final_df.count()))

                        sqlContext.sql("SET spark.sql.autoBroadcastJoinThreshold = -1")

                        if bloom_filters_columns:
                            save_df = final_df.repartition(len(final_df.select(bucket_column_list).distinct().collect()),
                                                           bucket_column_list) \
                                .sortWithinPartitions(bloom_filter_list)
                        else:
                            print (print_hdr + "Inside Non Bloom Filter...")
                            save_df = final_df.repartition(len(final_df.select(bucket_column_list).distinct().collect()),
                                                           bucket_column_list)

                        if second_partitioned_column <> 'None':
                            temp_table = self.target_tablename + '_stg'
                            save_df.createOrReplaceTempView(temp_table)

                            final_sql = 'INSERT OVERWRITE TABLE ' + self.target_schema + '.' + self.target_tablename + '_tmp' + \
                                        ' PARTITION ( ' + first_partitioned_column + ',' + second_partitioned_column + ' ) \
                                        SELECT * FROM ' + temp_table

                            print (print_hdr + "final_sql: " + final_sql)
                            sqlContext.sql(final_sql)

                            # repair_table_sql = 'MSCK REPAIR TABLE ' + table_name
                            # print (print_hdr + "repair_table_sql: " + repair_table_sql)
                            # sqlContext.sql(repair_table_sql)

                        else:
                            temp_table = self.target_tablename + '_stg'
                            save_df.createOrReplaceTempView(temp_table)

                            sqlContext.setConf("hive.hadoop.supports.splittable.combineinputformat", "true")
                            sqlContext.setConf("tez.grouping.min-size", "1073741824")
                            sqlContext.setConf("tez.grouping.max-size", "2147483648")
                            sqlContext.setConf("mapreduce.input.fileinputformat.split.minsize", "2147483648")
                            sqlContext.setConf("mapreduce.input.fileinputformat.split.maxsize", "2147483648")
                            sqlContext.setConf("hive.merge.smallfiles.avgsize", "2147483648")
                            sqlContext.setConf("hive.exec.reducers.bytes.per.reducer", "2147483648")

                            final_sql = 'INSERT OVERWRITE TABLE ' + self.target_schema + '.' + self.target_tablename + '_tmp' + \
                                        ' PARTITION ( ' + first_partitioned_column + ' ) \
                                        SELECT * FROM ' + temp_table

                            print (print_hdr + "final_sql: " + final_sql)
                            sqlContext.sql(final_sql)

                        print (print_hdr + self.load_type + ": INCREMENTAL: Partitioned: rows_inserted: " + str(rows_inserted))

                        # analyze_table_sql = 'ANALYZE TABLE ' + table_name + ' partition (' + partitioned_columns + ') COMPUTE STATISTICS'
                        # print (print_hdr + "analyze_table_sql: " + analyze_table_sql)
                        # sqlContext.sql(analyze_table_sql)
                        #
                        # repair_table_sql = 'MSCK REPAIR TABLE ' + table_name
                        # print (print_hdr + "repair_table_sql: " + repair_table_sql)
                        # sqlContext.sql(repair_table_sql)

                    except Exception as e:
                        error = 13
                        err_msg = method_name + "[{0}]: Error while performing incremental update".format(error)
                        status = 'Job Error'
                        output_msg = traceback.format_exc()
                        # if mirror_load_flag:
                        #     audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename, status,
                        #                   self.data_path, self.technology, rows_inserted, rows_updated, rows_deleted, error, err_msg, 0,
                        #                   0, output_msg)
                        #     output.update({'error': error, 'err_msg': err_msg})
                        # else:
                        output.update({'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated, 'rows_deleted': rows_deleted,
                                       'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
                        # remove_files(paths, self.source_schema, input_table_name)
                        self.error_cleanup(self.source_schema, input_table_name, run_id, paths, conn_metadata)
                        return output
                # Incremental Update of non-partitioned table
                else:
                    try:
                        incremental_df = sqlContext.sql(incremental_sql_query)
                        rows_inserted = incremental_df.count()
                        print (print_hdr + "INCREMENTAL: Non-Partitioned: Incremental records: " + str(rows_inserted))

                        current_df = sqlContext.sql(target_sql_query)
                        join_column_list = self.join_columns.split(",")
                        output_df = current_df.join(incremental_df, join_column_list, "leftanti")

                        output_df_reorder = output_df.select(list(source_select.split(", ")))
                        merge_df = output_df_reorder.union(incremental_df)
                        merge_cnt = merge_df.count()

                        # merge_df.persist()
                        # merge_df.count()
                        # print path
                        # print (print_hdr + self.load_type + ": Non-partitioned: NumPartitions: ", merge_df.rdd.getNumPartitions())
                        if merge_df.rdd.getNumPartitions() > 300:
                            merge_coalesce_df = merge_df.coalesce(300)
                        else:
                            merge_coalesce_df = merge_df
                        if bloom_filters_columns:
                            save_df = merge_coalesce_df.sortWithinPartitions(bloom_filter_list)
                            # save_df.write.option("compression", "zlib").mode("overwrite").format("orc").save(path)
                            # save_df.write.option("compression", "zlib").mode("overwrite").format("orc").save(target_path)
                            save_df.write.option("compression", "zlib").mode("overwrite").format("orc").save(target_tmp_path)

                            print (print_hdr + "INCREMENTAL: Non-partitioned: Bloom Filter: merge_cnt: " + str(merge_cnt))
                            # analyze_table_sql = 'ANALYZE TABLE ' + table_name + ' COMPUTE STATISTICS'
                            # print (print_hdr + "analyze_table_sql: " + analyze_table_sql)
                            # sqlContext.sql(analyze_table_sql)

                        else:
                            # print path
                            # merge_coalesce_df.write.option("compression","zlib").mode("overwrite").format("orc").save(path)
                            merge_coalesce_df.write.option("compression", "zlib").mode("overwrite").format("orc").save(target_tmp_path)

                            print (print_hdr + "INCREMENTAL: Non-partitioned: Non-Bloom Filter: merge_cnt: " + str(merge_cnt))
                            # temp_table = self.target_tablename + '_tmp'
                            # merge_coalesce_df.createOrReplaceTempView(temp_table)
                            #
                            # final_sql = 'INSERT OVERWRITE TABLE ' + self.target_schema + '.' + self.target_tablename + ' SELECT * FROM ' + temp_table
                            # print (print_hdr + "final_sql: " + final_sql)
                            # sqlContext.sql(final_sql)

                        self.update_control(input_schema_name, input_table_name, self.CONTROL_STATUS_INPROGRESS, run_id)
                        # (ret_rm, out_rm, err_rm) = run_cmd(['hadoop', 'fs', '-rm', '-r', '-f', (target_path + "*")])
                        # print (print_hdr + "INCREMENTAL: removing files from target - STATUS: " + str(ret_rm))
                        #
                        # # Use mv for inter-hdfs and distcp between hdfs and s3
                        # (ret_mv, out_mv, err_mv) = run_cmd(['hadoop', 'fs', '-mv', (temp_path + "*"), target_path])
                        # print (print_hdr + "INCREMENTAL: moving files from temp to target - STATUS: " + str(ret_mv))
                        #
                        # if ret_rm <> 0 or ret_mv <> 0:
                        #     error = ret_rm if ret_mv == 0 else ret_mv
                        #     err_msg = method_name + "[{0}]: Error with unix commnds".format(error)
                        #     status = 'Job Error'
                        #     output_msg = err_rm[0:3997] if ret_mv == 0 else err_mv[0:3997]
                        #     # if mirror_load_flag:
                        #     #     audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name,self.job_name, tablename, status,
                        #     #                   self.data_path, self.technology, rows_inserted, rows_updated, rows_deleted, error, err_msg, 0, 0, output_msg)
                        #     #     output.update({'error': error, 'err_msg': err_msg})
                        #     # else:
                        #     output.update({'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated, 'rows_deleted': rows_deleted,
                        #          'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
                        #     self.error_cleanup(self.source_schema, input_table_name, run_id, paths, conn_metadata)
                        #     return output
                        #
                        # analyze_table_sql = 'ANALYZE TABLE ' + table_name + ' COMPUTE STATISTICS'
                        # print (print_hdr + "analyze_table_sql: " + analyze_table_sql)
                        # sqlContext.sql(analyze_table_sql)
                    except Exception as e:
                        error = 14
                        err_msg = method_name + "[{0}]: Error while doing incremental update".format(error)
                        status = 'Job Error'
                        output_msg = traceback.format_exc()
                        # if mirror_load_flag:
                        #     audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename, status,
                        #                   self.data_path, self.technology, rows_inserted, rows_updated, rows_deleted, error, err_msg, 0,
                        #                   0, output_msg)
                        #     output.update({'error': error, 'err_msg': err_msg})
                        # else:
                        output.update({'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated, 'rows_deleted': rows_deleted,
                                       'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
                        # remove_files(paths, self.source_schema, input_table_name)
                        self.error_cleanup(self.source_schema, input_table_name, run_id, paths, conn_metadata)
                        return output

            if partitioned:
                repair_table_sql = 'MSCK REPAIR TABLE ' + self.target_schema + '.' + target_tmp_table
                print (print_hdr + "repair_table_sql: " + repair_table_sql)
                sqlContext.sql(repair_table_sql)

            # refresh_metadata_sql = 'REFRESH TABLE ' + table_name
            # print (print_hdr + "refresh_metadata_sql: " + refresh_metadata_sql)
            # sqlContext.sql(refresh_metadata_sql)

            # if self.archived_enabled:
            #     archive_path = 's3a://' + bucket_name + '/' + self.target_schema + '/' + self.target_tablename + '_bkp/'
            #     if self.s3_backed:
            #         source_path = 's3a://' + bucket_name + '/' + self.source_schema + '/' + self.source_tablename + '/'
            #     else:
            #         source_path = 'hdfs://apps/hive/warehouse/' + self.source_schema + '.db/' + self.source_tablename + '/*'
            #
            #     print (print_hdr + "source_path: " + source_path)
            #     print (print_hdr + "archive_path: " + archive_path)
            #
            #     (ret, out, err) = run_cmd(['hadoop', 'distcp', source_path, archive_path])
            #     print (print_hdr + "INCREMENTAL: distcp files between target and archive:" + str(ret))
            #
            #     if err:
            #         error = 15
            #         err_msg = method_name + "[{0}]: Error while loading data in temporary hdfs location for append only load".format(error)
            #         status = 'Job Error'
            #         output_msg = traceback.format_exc()
            #         # if mirror_load_flag:
            #         #     audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename, status,
            #         #                   self.data_path, self.technology, rows_inserted, rows_updated, rows_deleted, error, err_msg, 0, 0,
            #         #                   output_msg)
            #         #     output.update({'error': error, 'err_msg': err_msg})
            #         # else:
            #         output.update({'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated, 'rows_deleted': rows_deleted,
            #                        'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
            #         # remove_files(paths, self.source_schema, input_table_name)
            #         self.error_cleanup(self.source_schema, input_table_name, run_id, paths, conn_metadata)
            #         return output

            #    t=datetime.fromtimestamp(time.time())
            #    v_timestamp = str(t.strftime('%Y-%m-%d %H:%M:%S'))

            # update_control_table_sql = "UPDATE sync.control_table SET last_run_time = '" + v_timestamp + "', second_last_run_time = last_run_time, status_flag = 'Y' WHERE (target_schemaname = '" + input_schema_name + "' AND target_tablename in( '" + input_table_name + "','" + input_table_name + "_ext') AND data_path in ('GP2HDFS','HDFS2MIR'))"
            # update_control_table_sql = "UPDATE sync.control_table \
            #                     SET last_run_time = '" + v_timestamp + "', \
            #                         second_last_run_time = last_run_time, \
            #                         status_flag = 'Y', \
            #                         load_status_cd = '" + self.CONTROL_STATUS_COMPLETED + "' \
            #                     WHERE target_schemaname = '" + input_schema_name + "'"
            #
            # if self.data_path == 'HDFS2MIR':
            #     update_control_table_sql = update_control_table_sql \
            #                                 + " AND target_tablename in( '" + input_table_name + "','" + input_table_name + "_ext')" \
            #                                 + " AND data_path in ('GP2HDFS','HDFS2MIR')"
            # else:
            #     update_control_table_sql = update_control_table_sql \
            #                                 + " AND target_tablename in( '" + input_table_name + "')" \
            #                                 + " AND data_path in ('" + self.data_path + "')"
            #
            # print (print_hdr + "update_control_table_sql: " + update_control_table_sql)
            # try:
            #     cur_metadata.execute(update_control_table_sql)
            #     if self.log_mode == 'DEBUG':
            #         status = "Updated Control Table"
            #         audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename, status,
            #                       self.data_path, self.technology, rows_inserted, rows_updated, rows_deleted, error, err_msg, 0, 0,None)
            # except Exception as e:
            #     error = 16
            #     err_msg = method_name + "[{0}]: Error while updating the control table".format(error)
            #     status = 'Job Error'
            #     output_msg = traceback.format_exc()
            #     # if mirror_load_flag:
            #     #     audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename, status, self.data_path,
            #     #                   self.technology, rows_inserted, rows_updated, rows_deleted, error, err_msg, 0, 0, output_msg)
            #     #     output.update({'error': error, 'err_msg': err_msg})
            #     # else:
            #     output.update({'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated, 'rows_deleted': rows_deleted,
            #                    'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
            #     # remove_files(paths, self.source_schema, input_table_name)
            #     self.error_cleanup(self.source_schema, input_table_name, run_id, paths, conn_metadata)
            #     return output

            # try:
            error = 0
            err_msg = method_name + "[{0}]: No Errors".format(error)
            status = 'Job Finished'
            output_msg = 'Job Finished successfully'
            remove_files(paths, self.source_schema, input_table_name)
            #
            #     # if mirror_load_flag:
            #     #     audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename, status, self.data_path,
            #     #                   self.technology, rows_inserted, rows_updated, rows_deleted, error, err_msg, 0, 0, output_msg)
            #     #     output.update({'error': error, 'err_msg': err_msg})
            #     # else:
            output.update({'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated, 'rows_deleted': rows_deleted,
                           'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
            #
            #     if conn_metadata is not None:
            #         conn_metadata.close()
            #     return output
            # except psycopg2.Error as e:
            #     error = 17
            #     err_msg = method_name + "[{0}]: Error while updating log table for Job Finshed entry".format(error)
            #     status = 'Job Error'
            #     # remove_files(paths, self.source_schema, input_table_name)
            #     output_msg = traceback.format_exc()
            #     # if mirror_load_flag:
            #     #     audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename, status, self.data_path,
            #     #                   self.technology, rows_inserted, rows_updated, rows_deleted, error, err_msg, 0, 0, output_msg)
            #     #     output.update({'error': error, 'err_msg': err_msg})
            #     # else:
            #     output.update({'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated, 'rows_deleted': rows_deleted,
            #                    'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
            #     self.error_cleanup(self.source_schema, input_table_name, run_id, paths, conn_metadata)
            #     return output

        except Exception as e:
            error = 17
            err_msg = method_name + "[{0}]: Error while loading data".format(error)
            status = 'Job Error'
            # remove_files(paths, self.source_schema, input_table_name)
            output_msg = traceback.format_exc()
            # if mirror_load_flag:
            #     audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,
            #                   status, self.data_path,
            #                   self.technology, rows_inserted, rows_updated, rows_deleted, error, err_msg, 0, 0, output_msg)
            #     output.update({'error': error, 'err_msg': err_msg})
            # else:
            output.update({'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated, 'rows_deleted': rows_deleted,
                           'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
            self.error_cleanup(input_schema_name, input_table_name, run_id, paths, conn_metadata)

        if len(output) == 0:
            output = {'error': error, 'err_msg': err_msg}
        return output


    def load_file2gp_hvr(self):
        print "Inside file2GP_HVR"
        tablename           = self.source_schemaname + "." + self.source_tablename
        run_id_sql          = "select nextval('sync.datasync_seq')"
        plant_name          = 'DATASYNC'
        system_name         = 'GPDB'
        job_name            = 'SRC-->GPDB'
        technology          = 'Python'
        num_errors          = 0
        source_row_count    = 0
        target_row_count    = 0

# proceed to point everything at the 'branched' resources
        metastore_dbName            = self.config_list['meta_db_dbName']
        dbmeta_Url                  = self.config_list['meta_db_dbUrl']
        dbmeta_User                 = self.config_list['meta_db_dbUser']
        dbmeta_Pwd                  = base64.b64decode(self.config_list['meta_db_dbPwd'])

        dbtgt_Url                   = self.config_list['tgt_db_dbUrl']
        dbtgt_User                  = self.config_list['tgt_db_dbUser']

        dbtgt_Pwd                   = base64.b64decode(self.config_list['tgt_db_dbPwd'])
        dbtgt_dbName                = self.config_list['tgt_db_dbName']

        data_paths                  = self.config_list['misc_dataPath']
        hdfs_data_path              = self.config_list['misc_hdfsPath']

        env                         = self.config_list['env']

        t                           = datetime.fromtimestamp(time.time())
        v_timestamp                 = str(t.strftime('%Y-%m-%d %H:%M:%S'))

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


# Check if HVR has dumped files. If we not we will exit from here. No need to continue the process

        path               = data_paths + self.source_schemaname + "/" + "*-" + self.source_tablename + ".csv"
        files              = glob.glob(path)

        if len(files) == 0:
            error           = 0
            err_msg         = "No data dumped from source @:" + data_paths + self.source_schemaname + "/"
            status          = 'Job Finished'
            output_msg      = "No data dumped from source @:" +  data_paths + self.source_schemaname + "/"
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status,self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            print "Finished No Data"
            conn_metadata.close()
            return error, err_msg, self.source_schemaname + "." + self.source_tablename
        else:
            in_progress_path = data_paths + self.source_schemaname + "/in_progress"
            move_files(path,in_progress_path)

# Get target database access

        try:
           conn_target, cur_target      = txn_dbConnect(dbtgt_dbName, dbtgt_User, dbtgt_Url, dbtgt_Pwd)
        except psycopg2.Error as e:
            error   = 2
            err_msg = "Error connecting to Target database"
            print err_msg
            status = 'Job Error'
            output_msg = traceback.format_exc()
            print output_msg
            path_from = in_progress_path + "/*-" + self.source_tablename + ".csv"
            path_to   = data_paths + self.source_schemaname
            move_files(path_from,path_to)
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            conn_metadata.close()
            return error, err_msg, self.source_schemaname + "." + self.source_tablename

# Check for the existence of External table in target. If present move on to further checks regarding this

        try:
            check_table_sql = "SELECT n.nspname||'.'||c.relname as ext_table_name FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid WHERE n.nspname = '" + self.source_schemaname + "' AND c.relname = '" + self.source_tablename + "_ext_hvr' AND c.relstorage = 'x' "
            found_table     = dbQuery(cur_target, check_table_sql)
        except psycopg2.Error as e:
            error   = 3
            err_msg = "Error while checking for External table"
            print err_msg
            status = 'Job Error'
            output_msg = traceback.format_exc()
            print output_msg
            path_from = in_progress_path + "/*-" + self.source_tablename + ".csv"
            path_to   = data_paths + self.source_schemaname
            move_files(path_from,path_to)
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            return error, err_msg, self.source_schemaname + "." + self.source_tablename

# If External table is found in target database, compare the DDL of the Heap and the External
        try:
            if len(found_table) == 1:
                column_list_sql_ext = "SELECT attrelid::regclass, attnum, attname,format_type(a.atttypid, a.atttypmod) AS data_type FROM pg_attribute a WHERE  attrelid = '" + self.source_schemaname + "." + self.source_tablename + "_ext_hvr'" + "::regclass AND attnum > 0 AND NOT attisdropped ORDER BY attnum"
                columns_ext         = dbQuery(cur_target, column_list_sql_ext)
        except psycopg2.Error as e:
            error   = 4
            err_msg = "Error while getting column list for External table"
            print err_msg
            status = 'Job Error'
            output_msg = traceback.format_exc()
            print output_msg
            path_from = in_progress_path + "/*-" + self.source_tablename + ".csv"
            path_to   = data_paths + self.source_schemaname
            move_files(path_from,path_to)
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            return error, err_msg, self.source_schemaname + "." + self.source_tablename

# Get columns for Heap Table
        try:
            column_list_sql = "SELECT attrelid::regclass, attnum, attname,format_type(a.atttypid, a.atttypmod) AS data_type FROM pg_attribute a WHERE  attrelid = " \
                                  + "'" + self.source_schemaname + "." + self.source_tablename + "'" + "::regclass"              \
                                  " AND attnum > 0 AND NOT attisdropped ORDER BY attnum"
            columns         = dbQuery(cur_target, column_list_sql)
        except psycopg2.Error as e:
            error   = 5
            err_msg = "Error while getting column list for Heap table"
            print err_msg
            status = 'Job Error'
            output_msg = traceback.format_exc()
            print output_msg
            path_from = in_progress_path + "/*-" + self.source_tablename + ".csv"
            path_to   = data_paths + self.source_schemaname
            move_files(path_from,path_to)
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            return error, err_msg, self.source_schemaname + "." + self.source_tablename

        if not columns:
            err_msg = "Column list not found in pg_attribute system table "
            status = 'Job Error'
            output_msg = "Column list not found in pg_attribute system table "
            print output_msg
            path_from = in_progress_path + "/*-" + self.source_tablename + ".csv"
            path_to   = data_paths + self.source_schemaname
            move_files(path_from,path_to)
            error   = 6
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            return error, err_msg, self.source_schemaname + "." + self.source_tablename

# Prepare column list for Heap Table
        try:

            len_cols    = len(columns) - 2
            columns     = columns[:len_cols]
            select_list = '","'.join(d['attname'] for d in columns)
            select_list = '"' + select_list + '"'
            target_create_list = ',"'.join(d['attname']+'" '+d['data_type'] for d in columns)
            target_create_list = '"' + target_create_list + ", op_code integer, time_key varchar(100)"

            insert_list        = ',a.'.join(d['attname'] for d in columns)
            insert_list        = 'a.' + insert_list
        except Exception as e:
            error   = 7
            print e
            err_msg = "Error while preparing column list for Heap table"
            print err_msg
            status = 'Job Error'
            output_msg = traceback.format_exc()
            print output_msg
            path_from = in_progress_path + "/*-" + self.source_tablename + ".csv"
            path_to   = data_paths + self.source_schemaname
            move_files(path_from,path_to)
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            return error, err_msg, self.source_schemaname + "." + self.source_tablename

# Prepare column list for External Table
        if len(found_table) == 1:
            try:
                len_cols_ext    = len(columns_ext) - 2
                columns_ext     = columns_ext[:len_cols_ext]
                select_list_ext = '","'.join(d['attname'] for d in columns_ext)
                select_list_ext = '"' + select_list_ext + '"'
            except Exception as e:
                error   = 8
                print e
                err_msg = "Error while preparing column list for External table"
                print err_msg
                status = 'Job Error'
                output_msg = traceback.format_exc()
                print output_msg
                path_from = in_progress_path + "/*-" + self.source_tablename + ".csv"
                path_to   = data_paths + self.source_schemaname
                move_files(path_from,path_to)
                audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
                return error, err_msg, self.source_schemaname + "." + self.source_tablename
        else:
            select_list_ext = []


        if env == 'dev':
            if self.source_schemaname == 'eservice':
                port_number = 8998
                gpfdistHost = "10.228.10.46"
            elif self.source_schemaname == 'rmd':
                port_number = 8999
                gpfdistHost = "10.228.5.150"
            elif self.source_schemaname == 'proficy':
                port_number = 8997
                gpfdistHost = "10.228.5.150"
            elif self.source_schemaname == 'proficy_grr':
                port_number = 8996
                gpfdistHost = "10.228.10.46"
            elif self.source_schemaname == 'odw':
                port_number = 8995
                gpfdistHost = "10.228.10.46"
        else:
            if self.source_schemaname == 'eservice':
                port_number = 8998
                gpfdistHost = "10.230.4.64"
            elif self.source_schemaname == 'rmd':
                port_number = 8999
                gpfdistHost = "10.230.11.201"
            elif self.source_schemaname == 'proficy':
                port_number = 8997
                gpfdistHost = "10.230.11.201"
            elif self.source_schemaname == 'proficy_grr':
                port_number = 8996
                gpfdistHost = "10.230.4.64"
            elif self.source_schemaname == 'odw':
                port_number = 8995
                gpfdistHost = "10.230.4.64"

        if select_list <> select_list_ext:

            try:
                drop_table   = "DROP EXTERNAL TABLE IF EXISTS " + self.target_schemaname + "." + self.target_tablename + "_ext_hvr"
                cur_target.execute(drop_table)
            except psycopg2.Error as e:
                error   = 9
                err_msg = "Error while dropping external table in target"
                print err_msg
                status = 'Job Error'
                output_msg = traceback.format_exc()
                print output_msg
                path_from = in_progress_path + "/*-" + self.source_tablename + ".csv"
                path_to   = data_paths + self.source_schemaname
                move_files(path_from,path_to)
                audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,source_row_count,0,output_msg)
                conn_target.rollback()
                conn_target.close()
                conn_metadata.close()
                return error, err_msg, self.source_schemaname + "." + self.source_tablename

            try:
                create_table_sql     = "CREATE EXTERNAL TABLE " + self.target_schemaname + "." + self.target_tablename  + "_ext_hvr ( " + target_create_list + " )" + " LOCATION ('gpfdist://" + gpfdistHost + ":" + str(port_number) + "/in_progress/*-" +  self.target_tablename + ".csv') FORMAT 'CSV' (DELIMITER '|' NULL '\\\\N' ESCAPE '" + "\\\\" + "')"
                print create_table_sql
                cur_target.execute(create_table_sql)
            except psycopg2.Error as e:
                error   = 10
                err_msg = "Error while creating external table in target"
                print err_msg
                status = 'Job Error'
                output_msg = traceback.format_exc()
                print output_msg
                path_from = in_progress_path + "/*-" + self.source_tablename + ".csv"
                path_to   = data_paths + self.source_schemaname
                move_files(path_from,path_to)
                audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,source_row_count,0,output_msg)
                conn_target.rollback()
                conn_target.close()
                conn_metadata.close()
                return error, err_msg, self.source_schemaname + "." + self.source_tablename

        if (self.target_tablename == 'gets_tool_fault' and self.source_schemaname == 'rmd') or (self.target_schemaname== 'rmd' and self.target_tablename == 'gets_tool_mp_ac_cca') or (self.target_schemaname== 'rmd' and self.target_tablename == 'gets_tool_mp_ac_cca2') or (self.target_schemaname == 'rmd' and self.target_tablename == 'gets_tool_mp_ac_cca3') or (self.target_schemaname == 'rmd' and self.target_tablename == 'gets_tool_mp_dc_cca') or (self.target_schemaname  == 'rmd' and self.target_tablename  == 'gets_tool_mp_dc_cca2'):
            create_temp_table_sql = "CREATE TEMP TABLE " + self.target_tablename+ "_temp (" + target_create_list + ") DISTRIBUTED BY (" + self.join_columns + ")"
            insert_temp_table_sql = "INSERT INTO " + self.target_tablename+ "_temp SELECT * FROM " + self.target_schemaname + "." + self.target_tablename + "_ext_hvr"
            # print create_temp_table_sql
            try:
                # cur_target.execute("SET gp_autostats_mode = NONE")
                print "CREATING TEMP TABLE"
                cur_target.execute(create_temp_table_sql)
                print "INSERTING INTO TEMP TABLE"
                cur_target.execute(insert_temp_table_sql)
                rows_inserted = cur_target.rowcount
                print "Rows Inserted into Temp Table : ", rows_inserted
            except psycopg2.Error as e:
                error=11
                err_msg = "Error while creating temp table and inserting in target"
                status = 'Job Error'
                path_from = in_progress_path + "/*-" + self.source_tablename + ".csv"
                path_to   = data_paths + self.source_schemaname
                move_files(path_from,path_to)
                output_msg = traceback.format_exc()
                print output_msg
                audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path , technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,source_row_count,0,output_msg)
                conn_target.rollback()
                conn_target.close()
                conn_metadata.close()
                return error, err_msg, self.source_schemaname + "." + self.source_tablename

# Delete logic

        if self.load_type == 'FULL':
            delete_sql   = "TRUNCATE TABLE " + self.target_schemaname + "." + self.target_tablename
        elif (self.target_tablename == 'gets_tool_fault' and self.target_schemaname == 'rmd') or (self.target_schemaname == 'rmd' and self.target_tablename == 'gets_tool_mp_ac_cca') or (self.target_schemaname == 'rmd' and self.target_tablename == 'gets_tool_mp_ac_cca2') or (self.target_schemaname == 'rmd' and self.target_tablename  == 'gets_tool_mp_ac_cca3') or (self.target_schemaname == 'rmd' and self.target_tablename == 'gets_tool_mp_dc_cca') or (self.target_schemaname == 'rmd' and self.target_tablename == 'gets_tool_mp_dc_cca2'):
            delete_sql   = "DELETE FROM "  + self.target_schemaname + "." + self.target_tablename + " a WHERE EXISTS (SELECT 1 FROM  " \
                         + self.target_tablename + "_temp b WHERE 1=1 AND a.objid = b.objid )"
        else:
            delete_sql   = "DELETE FROM "  + self.target_schemaname + "." + self.target_tablename + " a WHERE EXISTS (SELECT 1 FROM  " \
                     + self.target_schemaname + "." + self.target_tablename + "_ext_hvr b WHERE 1=1 "
            for i in self.join_columns.split(','):
                delete_sql = delete_sql + " and coalesce(cast(a."+ i + " as text),'99999999999999999') = " + "coalesce(cast(b." + i + " as text),'99999999999999999')"
            delete_sql = delete_sql + " )"
        print delete_sql

        try:
            if (self.target_tablename == 'gets_tool_fault' and self.target_schemaname == 'rmd') or (self.target_schemaname == 'rmd' and self.target_tablename == 'gets_tool_mp_ac_cca') or (self.target_schemaname == 'rmd' and self.target_tablename == 'gets_tool_mp_ac_cca2') or (self.target_schemaname == 'rmd' and self.target_tablename == 'gets_tool_mp_ac_cca3') or (self.target_schemaname == 'rmd' and self.target_tablename == 'gets_tool_mp_dc_cca') or (self.target_schemaname== 'rmd' and self.target_tablename == 'gets_tool_mp_dc_cca2'):
                cur_target.execute("SET enable_seqscan = off")
                cur_target.execute("SET gp_autostats_mode = NONE")
            cur_target.execute(delete_sql)
            rows_deleted = cur_target.rowcount
            print "Rows deleted : ", rows_deleted
        except psycopg2.Error as e:
            error   = 12
            err_msg = "Error while deleting records that match the new data at target"
            print err_msg
            status = 'Job Error'
            output_msg = traceback.format_exc()
            print output_msg
            path_from = in_progress_path + "/*-" + self.source_tablename + ".csv"
            path_to   = data_paths + self.source_schemaname
            move_files(path_from,path_to)
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,source_row_count,0,output_msg)
            conn_target.rollback()
            conn_target.close()
            conn_metadata.close()
            return error, err_msg, self.source_schemaname + "." + self.source_tablename

# Insert logic
        if self.load_type == 'APPEND_ONLY':
            insert_sql   = "INSERT INTO "+ self.target_schemaname+ "." + self.target_tablename+ " SELECT " + insert_list + ", CASE WHEN op_code = 0 THEN 1 ELSE 0 END AS hvr_is_deleted, now()::timestamp without time zone FROM " + self.target_schemaname + "." + self.target_tablename + "_ext_hvr a WHERE a.op_code = 1"
        else:
            insert_sql   = "INSERT INTO " + self.target_schemaname + "." + self.target_tablename + " SELECT " + insert_list + ", CASE WHEN op_code = 0 THEN 1 ELSE 0 END AS hvr_is_deleted, now()::timestamp without time zone FROM ( SELECT *, row_number() OVER (PARTITION BY " + self.join_columns + " ORDER BY time_key DESC) as rownum FROM " \
                       + self.target_schemaname + "." + self.target_tablename + "_ext_hvr WHERE op_code <> 5) a WHERE a.rownum = 1"
        print insert_sql

        try:
            cur_target.execute(insert_sql)
            rows_inserted = cur_target.rowcount
            print "Rows inserted : ", rows_inserted
        except psycopg2.Error as e:
            error   = 13
            err_msg = "Error while inserting new records in target"
            print err_msg
            status = 'Job Error'
            output_msg = traceback.format_exc()
            print output_msg
            path_from = in_progress_path + "/*-" + self.source_tablename + ".csv"
            path_to   = data_paths + self.source_schemaname
            move_files(path_from,path_to)
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,source_row_count,0,output_msg)
            conn_target.rollback()
            conn_target.close()
            conn_metadata.close()
            return error, err_msg, self.source_schemaname + "." + self.source_tablename


# Update Control table after finishing loading table

        try:
            update_control_info_sql = "UPDATE sync.control_table set last_run_time = '" + v_timestamp + "' where source_schemaname = '" + self.source_schemaname  + "' AND source_tablename = '" + self.source_tablename+ "' AND data_path = 'SRC2GP'"
            update_control_info_sql2 = "UPDATE sync.control_table set hvr_last_processed_value = '" + v_timestamp + "' where source_schemaname = '" + self.source_schemaname + "' AND source_tablename = '" + self.source_tablename + "' AND data_path = 'GP2HDFS'"
            print update_control_info_sql
            print update_control_info_sql2
            cur_metadata.execute(update_control_info_sql)
            cur_metadata.execute(update_control_info_sql2)
        except psycopg2.Error as e:
            error   = 14
            err_msg = "Error while updating the control table"
            print err_msg
            status = 'Job Error'
            output_msg = traceback.format_exc()
            print output_msg
            path_from = in_progress_path + "/*-" + self.source_tablename + ".csv"
            path_to   = data_paths + self.source_schemaname
            move_files(path_from,path_to)
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            conn_target.rollback()
            conn_target.close()
            conn_metadata.close()
            return error, err_msg, self.source_schemaname + "." + self.source_tablename

# Move Files to HDFS Staging location

        try:
            from datetime import date
            today = date.today()
            todaystr = today.isoformat()
            path               = in_progress_path + "/" + "*-" + self.source_tablename + ".csv"
            hdfs_path          = hdfs_data_path + self.source_schemaname + "/" + todaystr + "/" + self.source_tablename + "/"
            move_files(path,hdfs_path)
        except Exception as e:
            error= 13
            err_msg = "Error while moving files to HDFS directory"
            print err_msg
            status  = "Job Error"
            output_msg = "Error while moving files to HDFS directory"
            print output_msg
            path_from = in_progress_path + "/*-" + self.source_tablename + ".csv"
            path_to   = data_paths + self.source_schemaname
            move_files(path_from,path_to)
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,source_row_count,0,output_msg)
            conn_target.rollback()
            conn_target.close()
            conn_metadata.close()
            return error, err_msg, self.source_schemaname + "." + self.source_tablename

# Final log entry
        try:
            error       = 0
            err_msg     = 'No Errors'
            status      = 'Job Finished'
            output_msg  = 'Job Finished successfully'
            print output_msg
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,source_row_count,target_row_count,output_msg)
        except psycopg2.Error as e:
            error   = 15
            err_msg = "Error while dropping external table in target"
            print err_msg
            status = 'Job Error'
            output_msg = traceback.format_exc()
            print output_msg
            path_from = in_progress_path + "/*-" + self.source_tablename + ".csv"
            path_to   = data_paths + self.source_schemaname
            move_files(path_from,path_to)
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            conn_target.rollback()
            conn_target.close()
            conn_metadata.close()
            return error, err_msg, self.source_schemaname + "." + self.source_tablename


        conn_target.commit()
        conn_target.close()
        conn_metadata.close()
        return error, err_msg, self.source_schemaname + "." + self.source_tablename


    def file2rds_hvr(self):
        print "Inside file2RDS_HVR"
        tablename           = self.source_schemaname + "." + self.source_tablename
        run_id_sql          = "select nextval('sync.datasync_seq')"
        plant_name          = 'DATASYNC'
        system_name         = 'RDS - Postgres'
        job_name            = 'SRC-->RDS'
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

        dbtgt_Pwd                  = base64.b64decode(self.config_list['tgt_db_dbPwd'])
        dbtgt_dbName               = self.config_list['tgt_db_dbName']

        data_paths                 = self.config_list['misc_dataPath']
        hdfs_data_path             = self.config_list['misc_hdfsPath']
        env                         = self.config_list['env']

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

# Check if HVR has dumped files. If we not we will exit from here. No need to continue the process

        path               = data_paths + self.source_schemaname + "_rds/" + "*-" + self.source_tablename + ".csv"
        files              = glob.glob(path)

        if len(files) == 0:
            error           = 0
            err_msg         = "No data dumped from source @:" + data_paths + self.source_schemaname + "_rds/"
            status          = 'Job Finished'
            output_msg      = "No data dumped from source @:" +  data_paths + self.source_schemaname + "_rds/"
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status,self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            print "Finished No Data"
            conn_metadata.close()
            return error, err_msg, self.source_schemaname + "." + self.source_tablename
        else:
            in_progress_path = data_paths + self.source_schemaname + "_rds/in_progress"
            move_files(path,in_progress_path)

# Get target database access

        try:
           conn_target, cur_target      = txn_dbConnect(dbtgt_dbName, dbtgt_User, dbtgt_Url, dbtgt_Pwd)
        except psycopg2.Error as e:
            error   = 2
            err_msg = "Error connecting to Target database"
            print err_msg
            status = 'Job Error'
            output_msg = traceback.format_exc()
            print output_msg
            path_from = in_progress_path + "/*-" + self.source_tablename + ".csv"
            path_to   = data_paths + self.source_schemaname + "_rds"
            move_files(path_from,path_to)
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            conn_metadata.close()
            return error, err_msg, self.source_schemaname + "." + self.source_tablename

# Get columns of Target Table for temp table
        try:
            column_list_sql = "SELECT attrelid::regclass, attnum, attname,format_type(a.atttypid, a.atttypmod) AS data_type FROM pg_attribute a WHERE  attrelid = " \
                                  + "'" + self.source_schemaname + "." + self.source_tablename + "'" + "::regclass"              \
                                  " AND attnum > 0 AND NOT attisdropped ORDER BY attnum"
            columns         = dbQuery(cur_target, column_list_sql)
        except psycopg2.Error as e:
            error   = 5
            err_msg = "Error while getting column list for Heap table"
            print err_msg
            status = 'Job Error'
            output_msg = traceback.format_exc()
            print output_msg
            path_from = in_progress_path + "/*-" + self.source_tablename + ".csv"
            path_to   = data_paths + self.source_schemaname + "_rds"
            move_files(path_from,path_to)
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            return error, err_msg, self.source_schemaname + "." + self.source_tablename

        if not columns:
            err_msg = "Column list not found in pg_attribute system table "
            status = 'Job Error'
            output_msg = "Column list not found in pg_attribute system table "
            print output_msg
            path_from = in_progress_path + "/*-" + self.source_tablename + ".csv"
            path_to   = data_paths + self.source_schemaname + "_rds"
            move_files(path_from,path_to)
            error   = 6
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            return error, err_msg, self.source_schemaname + "." + self.source_tablename

# Prepare column list for Temp Table

        try:
            len_cols            = len(columns) - 2
            columns             = columns[:len_cols]
            select_list         = '","'.join(d['attname'] for d in columns)
            select_list         = '"' + select_list + '"'
            target_create_list  = ',"'.join(d['attname']+'" '+d['data_type'] for d in columns)
            target_create_list  = '"' + target_create_list + ", op_code integer, time_key varchar(100)"
            insert_list         = ',a.'.join(d['attname'] for d in columns)
            insert_list         = 'a.' + insert_list
        except Exception as e:
            error   = 7
            print e
            err_msg = "Error while preparing column list for Heap table"
            print err_msg
            status = 'Job Error'
            output_msg = traceback.format_exc()
            print output_msg
            path_from = in_progress_path + "/*-" + self.source_tablename + ".csv"
            path_to   = data_paths + self.source_schemaname + "_rds"
            move_files(path_from,path_to)
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            return error, err_msg, self.source_schemaname + "." + self.source_tablename

# Create Temp Table for data load

        try:
            create_temp_table_sql     = "CREATE TEMP TABLE " + self.target_tablename  + "_ext_hvr ( " + target_create_list + " )"
            print create_temp_table_sql
            cur_target.execute(create_temp_table_sql)
        except psycopg2.Error as e:
            error       = 8
            err_msg = "Error while creating temp table in target"
            status = 'Job Error'
            output_msg = traceback.format_exc()
            path_from = in_progress_path + "/*-" + self.source_tablename + ".csv"
            path_to   = data_paths + self.source_schemaname + "_rds"
            move_files(path_from,path_to)
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,source_row_count,0,output_msg)
            conn_target.rollback()
            conn_target.close()
            conn_metadata.close()
            return error, err_msg, self.source_schemaname + "." + self.source_tablename

# Load Data into Temp Table before final table load

        try:
            file_path = in_progress_path  + "/*-"  + self.source_tablename + ".csv"
            files = glob.glob(file_path)
            for file in files:
                f    = open(file)
                #if env == 'dev':
                insert_temp_sql = "COPY " + self.target_tablename + "_ext_hvr" + " FROM STDIN WITH CSV DELIMITER '|' NULL '\\\\N' ESCAPE '\\\\'"
                #else:
                #    insert_temp_sql = "COPY " + self.target_tablename + "_ext_hvr" + " FROM STDIN WITH CSV DELIMITER '|' NULL '\N' ESCAPE '\\'"
                print insert_temp_sql
                cur_target.copy_expert(insert_temp_sql,f)
                f.close()
        except psycopg2.Error as e:
            error   = 9
            err_msg = "Error while inserting records into temp table for INCREMENTAL load"
            status = 'Job Error'
            sql_state = e.pgcode
            sql_error_msg = e.pgerror
            print sql_error_msg
            if not sql_state:
                output_msg = sql_state + ':' + sql_error_msg
            else:
                output_msg = traceback.format_exc()
            path_from = in_progress_path + "/*-" + self.source_tablename + ".csv"
            path_to   = data_paths + self.source_schemaname + "_rds"
            move_files(path_from,path_to)
            audit_logging(cur_metadata, self.load_id, run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            conn_target.rollback()
            conn_target.close()
            conn_metadata.close()
            return error, err_msg, self.source_schemaname + "." + self.source_tablename

# Delete Logic
        if self.load_type == 'FULL' :
            delete_sql   = "TRUNCATE TABLE " + self.target_schemaname + "." + self.target_tablename
        else:
            delete_sql   = "DELETE FROM "  + self.target_schemaname + "." + self.target_tablename + " a WHERE EXISTS (SELECT 1 FROM  " \
                           + self.target_tablename + "_ext_hvr b WHERE 1=1 "
            for i in self.join_columns.split(','):
                delete_sql = delete_sql + " and a."+ i + " = " + "b." + i
            delete_sql = delete_sql + ")"

        try:
            print delete_sql
            cur_target.execute(delete_sql)
            rows_deleted = cur_target.rowcount
            print "Rows deleted : ", rows_deleted
        except psycopg2.Error as e:
            print e
            err_msg = "Error while deleting records that match the new data at target"
            status = 'Job Error'
            sql_state = e.pgcode
            sql_error_msg = e.pgerror
            if not sql_state:
                output_msg = sql_state + ':' + sql_error_msg
            else:
                output_msg = traceback.format_exc()
            path_from = in_progress_path + "/*-" + self.source_tablename + ".csv"
            path_to   = data_paths + self.source_schemaname + "_rds"
            move_files(path_from,path_to)
            audit_logging(cur_metadata, self.load_id, run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            conn_target.rollback()
            conn_target.close()
            conn_metadata.close()
            error       = 10
            return error, err_msg, self.source_schemaname + "." + self.source_tablename

# Insert Logic

        insert_sql   = "INSERT INTO " + self.target_schemaname+ "." + self.target_tablename + " SELECT " + insert_list + ", CASE WHEN op_code = 0 THEN 1 ELSE 0 END AS hvr_is_deleted, now()::timestamp without time zone FROM ( SELECT *, row_number() OVER (PARTITION BY " + self.join_columns+ " ORDER BY time_key DESC) as rownum FROM " \
                        + self.target_tablename + "_ext_hvr WHERE op_code <> 5) a WHERE a.rownum = 1"
        print insert_sql

        try:
            cur_target.execute(insert_sql)
            rows_inserted = cur_target.rowcount
            print "Rows inserted : ", rows_inserted
        except psycopg2.Error as e:
            error       = 11
            err_msg = "Error while inserting new records in target"
            status = 'Job Error'
            output_msg = traceback.format_exc()
            path_from = in_progress_path + "/*-" + self.source_tablename + ".csv"
            path_to   = data_paths + self.source_schemaname + "_rds"
            move_files(path_from,path_to)
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,source_row_count,0,output_msg)
            conn_target.rollback()
            conn_target.close()
            conn_metadata.close()
            return error, err_msg, self.source_schemaname + "." + self.source_tablename

# Update Control table after finishing loading table
        try:
            update_control_info_sql = "UPDATE sync.control_table set last_run_time = '" + v_timestamp + "', hvr_last_processed_value = '" + v_timestamp + "' where source_schemaname = '" + self.source_schemaname + "' AND source_tablename = '" + self.source_tablename + "' AND data_path in ('SRC2RDS')"
            print update_control_info_sql
            cur_metadata.execute(update_control_info_sql)
        except psycopg2.Error as e:
            error       = 14
            err_msg = "Error while updating the control table"
            status = 'Job Error'
            output_msg = traceback.format_exc()
            path_from = in_progress_path + "/*-" + self.source_tablename + ".csv"
            path_to   = data_paths + self.source_schemaname
            move_files(path_from,path_to)
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            conn_target.rollback()
            conn_target.close()
            conn_metadata.close()
            return error, err_msg, self.source_schemaname + "." + self.source_tablename

# Move Files to HDFS Staging location

        try:
            from datetime import date
            today = date.today()
            todaystr = today.isoformat()
            path               = in_progress_path + "/" + "*-" + self.source_tablename + ".csv"
            hdfs_path          = hdfs_data_path + self.source_schemaname + "_rds/" + todaystr + "/" + self.source_tablename + "/"
            print path
            print hdfs_path
            move_files(path,hdfs_path)
        except Exception as e:
            error= 13
            err_msg = "Error while moving files to HDFS directory"
            print err_msg
            status  = "Job Error"
            output_msg = "Error while moving files to HDFS directory"
            print output_msg
            path_from = in_progress_path + "/*-" + self.source_tablename + ".csv"
            path_to   = data_paths + self.source_schemaname
            move_files(path_from,path_to)
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,source_row_count,0,output_msg)
            conn_target.rollback()
            conn_target.close()
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
            path_from = in_progress_path + "/*-" + self.source_tablename + ".csv"
            path_to   = data_paths + self.source_schemaname
            move_files(path_from,path_to)
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            conn_target.rollback()
            conn_target.close()
            conn_metadata.close()
            return error, err_msg, self.source_schemaname + "." + self.source_tablename

        conn_target.commit()
        conn_target.close()
        conn_metadata.close()
        return error, err_msg, self.source_schemaname + "." + self.source_tablename


    def file2rds(self):
        print "Inside file2RDS_i360"
        tablename           = self.source_schemaname + "." + self.source_tablename
        run_id_sql          = "select nextval('sync.datasync_seq')"
        plant_name          = 'DATASYNC'
        system_name         = 'RDS - Postgres'
        job_name            = 'File-->RDS'
        technology          = 'Python'
        num_errors          = 0
        source_row_count    = 0
        target_row_count    = 0

# proceed to point everything at the 'branched' resources
        metastore_dbName           = self.config_list['meta_db_dbName']
        dbmeta_Url                 = self.config_list['meta_db_dbUrl']
        dbmeta_User                = self.config_list['meta_db_dbUser']
        dbmeta_Pwd                 = base64.b64decode(self.config_list['meta_db_dbPwd'])

        dbtgt_Url                  = self.config_list['tgt_db_i360_dbUrl']
        dbtgt_User                 = self.config_list['tgt_db_i360_dbUser']
        dbtgt_dbName                = self.config_list['tgt_db_i360_dbName']
        dbtgt_Pwd                 = base64.b64decode(self.config_list['tgt_db_i360_dbPwd'])

        data_paths                 = self.config_list['misc_dataPath']
        hdfs_data_path             = self.config_list['misc_hdfsPath']
        data_paths_i360             = self.config_list['misc_dataPathi360']


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

        path               = data_paths_i360 + self.source_schemaname + "." + self.source_tablename + "*.dat"
        print path
        files              = glob.glob(path)

        if len(files) == 0:
            error           = 0
            err_msg         = "No data dumped from source @:" + data_paths_i360
            status          = 'Job Finished'
            output_msg      = "No data dumped from source @:" +  data_paths_i360
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status,self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            print "Finished No Data"
            conn_metadata.close()
            return error, err_msg, self.source_schemaname + "." + self.source_tablename
        else:
# Get target database access
            try:
               conn_target, cur_target      = txn_dbConnect(dbtgt_dbName, dbtgt_User, dbtgt_Url, dbtgt_Pwd)
            except psycopg2.Error as e:
                error   = 2
                err_msg = "Error connecting to Target database"
                print err_msg
                status = 'Job Error'
                output_msg = traceback.format_exc()
                print output_msg
                audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
                conn_metadata.close()
                return error, err_msg, self.source_schemaname + "." + self.source_tablename

            abs_file_name   = data_paths_i360 + self.source_schemaname + "." + self.source_tablename + ".dat"
            print abs_file_name
            file          = open(abs_file_name)

# Incremental Logic
            if self.load_type == 'INCREMENTAL' :
                create_temp_table_sql = "CREATE TEMPORARY TABLE " + self.target_tablename + "_temp AS SELECT * FROM " + self.target_schemaname + "." + self.target_tablename + \
                                     " WHERE 1=0"
                print create_temp_table_sql
                insert_temp_sql = "COPY " + self.target_tablename + "_temp" + " FROM STDIN WITH DELIMITER E'\x01' "
                print insert_temp_sql
                try:
                    cur_target.execute(create_temp_table_sql)
                    cur_target.copy_expert(insert_temp_sql,file)
                    file.close()
                except psycopg2.Error as e:
                    err_msg = "Error while inserting records into temp table for INCREMENTAL load"
                    status = 'Job Error'
                    sql_state = e.pgcode
                    sql_error_msg = e.pgerror
                    if not sql_state:
                        output_msg = sql_state + ':' + sql_error_msg
                    else:
                        output_msg = traceback.format_exc()
                    audit_logging(cur_metadata, run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
                    conn_target.rollback()
                    conn_target.close()
                    conn_metadata.close()
                    return

# Delete Logic
            if self.load_type == 'FULL' :
                delete_sql   = "TRUNCATE TABLE " + self.target_schemaname + "." + self.target_tablename
            else:
                delete_sql   = "DELETE FROM "  + self.target_schemaname + "." + self.target_tablename + " a WHERE EXISTS (SELECT 1 FROM  " \
                               + self.target_tablename + "_temp b WHERE 1=1 "
                for i in self.join_columns.split(','):
                    delete_sql = delete_sql + " and a."+ i + " = " + "b." + i
                delete_sql = delete_sql + ")"

            try:
                print delete_sql
                cur_target.execute(delete_sql)
            except psycopg2.Error as e:
                err_msg = "Error while deleting records that match the new data at target"
                status = 'Job Error'
                sql_state = e.pgcode
                sql_error_msg = e.pgerror
                if not sql_state:
                    output_msg = sql_state + ':' + sql_error_msg
                else:
                    output_msg = traceback.format_exc()
                audit_logging(cur_metadata, run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
                conn_target.rollback()
                conn_target.close()
                conn_metadata.close()
                return


# Final Insert Logic
            file          = open(abs_file_name)
            insert_sql    = "COPY " + self.target_schemaname + "." + self.target_tablename + " FROM STDIN WITH DELIMITER E'\x01' "
            print  insert_sql
            try:
                cur_target.copy_expert(insert_sql,file)
                err_msg    = ''
                status     = 'Job Finished'
                output_msg = ''
                file.close()
                print "Rows Inserted into Table : ", cur_target.rowcount
                # audit_logging(cur_metadata, run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            except psycopg2.Error as e:
                err_msg = "Error while inserting new records in target"
                status = 'Job Error'
                sql_state = e.pgcode
                sql_error_msg = e.pgerror
                if not sql_state:
                    output_msg = sql_state + ':' + sql_error_msg
                else:
                    output_msg = traceback.format_exc()
                audit_logging(cur_metadata, self.load_id, run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
                conn_target.rollback()
                conn_target.close()
                conn_metadata.close()
                return

# Update Control table after finishing loading table

            try:
                update_control_info_sql = "UPDATE sync.control_table set last_run_time = '" + v_timestamp + "' where id = " + str(self.id) + " AND source_schemaname = '" + self.source_schemaname + "' AND source_tablename = '" + self.source_tablename + "' AND data_path = '"+ self.data_path +"'"
                print update_control_info_sql
                cur_metadata.execute(update_control_info_sql)
            except psycopg2.Error as e:
                error   = 14
                err_msg = "Error while updating the control table"
                print err_msg
                status = 'Job Error'
                output_msg = traceback.format_exc()
                print output_msg
                audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
                conn_target.rollback()
                conn_target.close()
                conn_metadata.close()
                return error, err_msg, self.source_schemaname + "." + self.source_tablename

# Remove data files from gpfdist directory
            try:
               os.remove(abs_file_name)
            except OSError:
               pass

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

        conn_target.commit()
        conn_target.close()
        conn_metadata.close()
        return error, err_msg, self.source_schemaname + "." + self.source_tablename


if __name__ == "__main__":
    print "ERROR: Direct execution on this file is not allowed"

# if __name__ == "__main__":
#
#
#     APP_NAME='incremental_update_' + str(sys.argv[1])
#     table_name = sys.argv[1]
#     # Configure OPTIONS
#     conf = SparkConf().setAppName(APP_NAME)
#     #in cluster this will be like
#     #"spark://ec2-0-17-03-078.compute-#1.amazonaws.com:7077"
#     sc   = SparkContext(conf=conf)
#     load_id = int(sys.argv[2])
#     run_id  = int(sys.argv[3])
#     data_path = str(sys.argv[4])
#     v_timestamp = sys.argv[5]
#     print data_path
#     sc.setLogLevel('ERROR')
#     # Execute Main functionality
#     # error, err_msg = merge_data(sc,table_name,load_id,run_id,data_path,v_timestamp)
#     if data_path == "GP2HDFS":
#         file2db_sync = File2DBSync()
#         error, err_msg = file2db_sync.load_file2hive_spark(sc, table_name, load_id, run_id, data_path, v_timestamp)
#
#     print error
#     print err_msg
#
#     sys.exit(error)

