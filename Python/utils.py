##################################################
#               UTILITIES                        #
##################################################
import sys
import ConfigParser
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from smtplib import SMTPException
import smtplib
import textwrap
import glob
import shutil
import subprocess
import pyhs2
import inspect
import traceback
import time
import base64
import mysql.connector


def read_config(cfg_files):
    # sys.path.append("/apps/common")
    # sys.path.append("/data/analytics/common")
    if(cfg_files != None):
        config = ConfigParser.RawConfigParser()
        config.optionxform = str
        # merges all files into a single config
        for i, cfg_file in enumerate(cfg_files):
            if(os.path.exists(cfg_file)):
                print "Inside read config", cfg_file
                config.read(cfg_file)
        return config


def dbConnect (db_parm, username_parm, host_parm, pw_parm):
    # Parse in connection information
    credentials = {'host': host_parm, 'database': db_parm, 'user': username_parm, 'password': pw_parm}
    conn = psycopg2.connect(**credentials)
    conn.autocommit = True  # auto-commit each entry to the database
    conn.cursor_factory = RealDictCursor
    cur = conn.cursor()
    print ("[utils: dbConnect] - Connected Successfully to DB: " + str(db_parm) + "@" + str(host_parm))
    return conn, cur


def txn_dbConnect (db_parm, username_parm, host_parm, pw_parm):
    # Parse in connection information
    credentials = {'host': host_parm, 'database': db_parm, 'user': username_parm, 'password': pw_parm}
    conn = psycopg2.connect(**credentials)
    conn.autocommit = False  # auto-commit each entry to the database
    conn.cursor_factory = RealDictCursor
    cur = conn.cursor()
    print ("[utils: txn_dbConnect] - Connected Successfully to DB: " + str(db_parm) + "@" + str(host_parm))
    return conn, cur


def dbQuery (cur, query):
    cur.execute(query)
    rows = cur.fetchall()
    return rows


def sendMail(sender_parm, receivers_parm, message, error_table_list, load_id_parm, env="dev"):
    sender          = sender_parm
    receivers       = receivers_parm.split(',')
    env             = env.upper()
    message         = textwrap.dedent("""\
From: %s
To: %s
Subject: %s : HVR DataSync

%s : %s

Load ID : %s

Best Regards,
EDGE Node""" %(sender_parm, receivers_parm,env,message,error_table_list,load_id_parm))
    try:
       smtpObj = smtplib.SMTP('localhost')
       smtpObj.sendmail(sender, receivers, message)
       print ("[utils: sendMail] - Successfully sent email")
    except SMTPException:
       print ("[utils: sendMail] - Error: unable to send email")


def move_files(path_from, path_to):
    print "Source Path : ", path_from
    print "Destination Path : ", path_to
    files    = glob.glob(path_from)
    # for file in files:
    #     shutil.move(file,path_to)
    try:
        os.makedirs(path_to)
        for file in files:
            shutil.move(file,path_to)
    except Exception as e:
        print e
        if e.errno == 17:
            for file in files:
                shutil.move(file,path_to)
        else:
            print e
            err_msg = 'Error while moving files to HDFS path'
            print err_msg


def dbConnectHive (host_parm, port_parm, auth_parm):
    # Parse in connection information
    credentials = {'host': host_parm, 'port': port_parm, 'authMechanism': auth_parm}
    conn = pyhs2.connect(**credentials)                     # Change to Impyla
    conn.cursor_factory = RealDictCursor
    cur = conn.cursor()
    print ("[utils: dbConnectHive] - Connected Successfully to Hive: " + str(host_parm))
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


def remove_files(paths,source_schemaname,source_tablename):
    kinit_cmd  = "/usr/bin/kinit"
    kinit_args = "-kt"
    keytab     = "/home/gpadmin/gphdfs.service.keytab"
    env = os.environ.get('ENVIRONMENT_TAG', 'prod')
    if env == 'prod':
        realm = "gpadmin@TRANSPORTATION-HDPPROD.GE.COM"
    elif env == 'dev':
        realm = "gpadmin@TRANSPORTATION-HDPDEV.GE.COM"
    print ("[utils: remove_files] - realm: " + realm)
    try:
        java_home  = os.environ["JAVA_HOME"]
    except KeyError:
        java_home  = "/usr/lib/jvm/java"
        os.environ["JAVA_HOME"] = java_home
    # path       = paths + "/" + source_schemaname + "/" + source_tablename.replace('$','_') + "_ext/*"
    path = paths + source_schemaname + "/" + source_tablename.replace('$', '_') + "_ext/*"
    print ("[utils: remove_files] - path: " + path)
    try:
        #os.environ["JAVA_HOME"] = java_home
        subprocess.call([kinit_cmd, kinit_args, keytab, realm])
        subprocess.call(["hadoop", "fs", "-rm", "-r", path])
    except OSError as ose:
        print ("[utils: remove_files] - OSError: ", ose)
        pass


def run_cmd(args_list):
    print('[utils: run_cmd] - Running system command: {0}'.format(' '.join(args_list)))
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    s_output, s_err = proc.communicate()
    s_return =  proc.returncode
    return s_return, s_output, s_err


def load_config():
    print ("utils: load_config: Entered")
    # config = read_config(['/apps/gp2hdp_sync/environ.properties'])
    # config = read_config(['environ.properties'])
    # sys.path.append("/apps/common")
    # sys.path.append("/data/analytics/common")
    # print sys.path
    config_list = {}
    config = read_config(['/apps/common/environ.properties'])
    print config
    if (len(config.sections()) == 0):
        config = read_config(['/data/analytics/common/environ.properties'])
        print "Inside load_config"

    print config
    if (config == None):
        print ("utils: load_config: Config file not available")
    else:
        env = config.get('branch', 'env')
        config_list['env'] = env
        for section in config.sections():
            if str(section).find(env + ".") <> -1:
                section_part = str(section).split(".")[1]
                for option in config.options(section):
                    config_list[section_part + "_" + option] = config.get(section, option)

    return config_list


def call_method(path, module, method_name, *args):
    print ("utils: call_method: Entered")
    retry_cnt = 10
    wait_time = 20
    step_location = 0
    error = 0
    err_msg = ''
    output = {}
    while retry_cnt > 0:
        try:
            sys.path.append(path)
            module_handle = __import__(module, globals(), locals(), [], -1)
            func_handle = getattr(module_handle, method_name)

            if len(args) == 0:
                output = func_handle(step_location)
            elif len(args) == 1:
                output = func_handle(args[0], step_location)
            elif len(args) == 2:
                output = func_handle(args[0], args[1], step_location)
            elif len(args) == 3:
                output = func_handle(args[0], args[1], args[2], step_location)
            elif len(args) == 4:
                output = func_handle(args[0], args[1], args[2], args[3], step_location)
            elif len(args) == 5:
                output = func_handle(args[0], args[1], args[2], args[3], args[4], step_location)
            elif len(args) == 6:
                output = func_handle(args[0], args[1], args[2], args[3], args[4], args[5], step_location)
            elif len(args) == 7:
                output = func_handle(args[0], args[1], args[2], args[3], args[4], args[5], args[6], step_location)
            elif len(args) == 8:
                output = func_handle(args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7], step_location)
            elif len(args) == 9:
                output = func_handle(args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7], args[8], step_location)
            elif len(args) == 10:
                output = func_handle(args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7], args[8], args[9], step_location)

            # print ("utils: call_method: output: ", output)
            if (output is not None) and (len(output) > 0):
                output_msg = output.get('output_msg', "")

                step_location = output.get('step_location', 0)

                if output_msg.find("java.io.FileNotFoundException: File does not exist") <> -1:
                    retry_cnt -= 1
                    print ("utils: call_method: retries left: " + str(retry_cnt + 1) + ": waiting for " + str(wait_time) + " secs...")
                    time.sleep(wait_time)
                else:
                    retry_cnt = 0
            else:
                retry_cnt = 0
        except Exception as e:
            print ("utils: call_method: ERROR: ", e)
            rows_inserted = 0
            rows_updated = 0
            rows_deleted = 0
            num_errors = 1
            error = 1
            err_msg = "ERROR: utils: call_method:  while calling method " + method_name + " inside module " + module + " at " + path
            status = 'Job Error'
            output_msg = traceback.format_exc()

            if (output is None) or (len(output) == 0):
                output = {"error": error, "err_msg": err_msg, "status": status, "output_msg": output_msg, "rows_inserted": rows_inserted, \
                          "rows_updated": rows_updated, "rows_deleted": rows_deleted, "num_errors": num_errors, "step_location": step_location}
            else:
                output["error"] = error
                output["err_msg"] = err_msg
                output["output_msg"] = output_msg
            retry_cnt = 0

        if retry_cnt == 0:
            print ("utils: call_method: No Retry")

    if (output is None) or (len(output) == 0):
        output = {"error": error, "err_msg": err_msg}
    return output


def call_datasync_method(path, i_instance, method_name, sc, table_name, load_id, run_id, data_path, v_timestamp):
    print ("utils: call_datasync_method: Entered")
    retry_cnt = 10
    wait_time = 20
    error = 0
    err_msg = ''
    output = {}
    while retry_cnt > 0:
        try:
            sys.path.append(path)
            func_handle = getattr(i_instance, method_name)

            hive_txn = HiveTxn()
            read_output = hive_txn.get_datasync_objects(data_path, table_name)
            print ("utils: call_datasync_method: read_output: ", read_output)

            if read_output['error'] > 0:
                output = read_output
            else:
                output = func_handle(sc, table_name, load_id, run_id, data_path, v_timestamp)
                print ("utils: call_datasync_method: output: ", output)

            if output['error'] > 0:
                output_msg = output["output_msg"]
                if output_msg.find("java.io.FileNotFoundException: File does not exist") <> -1:
                    retry_cnt -= 1
                    print ("utils: call_datasync_method: retries left: " + str(retry_cnt + 1) + ": waiting for " + str(wait_time) + " secs...")
                    time.sleep(wait_time)
                else:
                    retry_cnt = 0
            else:
                write_output = hive_txn.write_hive(sc)
                print ("utils: call_datasync_method: write_output: ", write_output)

                if write_output['error'] > 0:
                    output['error'] = write_output['error']
                    output['err_msg'] = write_output['err_msg']
                    output['status'] = write_output['status']
                    output['output_msg'] = write_output['output_msg']
                retry_cnt = 0

        except Exception as e:
            rows_inserted = output.get('rows_inserted', 0)
            rows_updated = output.get('rows_updated', 0)
            rows_deleted = output.get('rows_deleted', 0)
            num_errors = output.get('num_errors', 0)
            error = 1
            err_msg = "ERROR: utils: call_datasync_method:  while calling method " + method_name + " of class " + i_instance.__class__.__name__ +" at " + path
            status = 'Job Error'
            output_msg = traceback.format_exc()
            if len(output) == 0:
                output = {"error": error, "err_msg": err_msg, "status": status, "output_msg": output_msg, "rows_inserted": rows_inserted, \
                          "rows_updated": rows_updated, "rows_deleted": rows_deleted, "num_errors": num_errors}
            else:
                output["error"] = error
                output["err_msg"] = err_msg
                output["output_msg"] = output_msg
            retry_cnt = 0
        finally:
            if retry_cnt == 0:
                print ("utils: call_datasync_method: No Retry")
                hive_txn.cleanup_hive()

    if len(output) == 0:
        output = {"error": error, "err_msg": err_msg}
    return output



def call_method_alt(method_name, *args):
    print ("utils: call_method_alt: Entered")
    retry_cnt = 10
    wait_time = 20
    step_location = 0
    output = {}
    while retry_cnt > 0:
        error_flag = 0
        try:
            frame = inspect.stack()[1]
            i_module = inspect.getmodule(frame[0])
            file = i_module.__file__
            file = str(file).replace(".py","")
            li = str(file).split("/")
            py_module = li[len(li)-1]
            li.remove(py_module)
            path = "/".join(li)
            sys.path.append(path)
            caller_handle = __import__(py_module, globals(), locals(), [], -1)
            func_handle = getattr(caller_handle, method_name)

            if len(args) == 1:
                output = func_handle(args[0], step_location)
            elif len(args) == 2:
                output = func_handle(args[0], args[1], step_location)
            elif len(args) == 3:
                output = func_handle(args[0], args[1], args[2], step_location)
            elif len(args) == 4:
                output = func_handle(args[0], args[1], args[2], args[3], step_location)
            elif len(args) == 5:
                output = func_handle(args[0], args[1], args[2], args[3], args[4], step_location)
            elif len(args) == 6:
                output = func_handle(args[0], args[1], args[2], args[3], args[4], args[5], step_location)
            elif len(args) == 7:
                output = func_handle(args[0], args[1], args[2], args[3], args[4], args[5], args[6], step_location)
            elif len(args) == 8:
                output = func_handle(args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7],
                                     step_location)
            elif len(args) == 9:
                output = func_handle(args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7], args[8],
                                     step_location)
            elif len(args) == 10:
                output = func_handle(args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7], args[8],
                                     args[9], step_location)

            output_msg = output["output_msg"]
            step_location = output["step_location"]
            if output_msg.find("java.io.FileNotFoundException: File does not exist") <> -1:
                retry_cnt -= 1
                error_flag = 1
                print ("utils: call_method_alt: retries left: " + str(retry_cnt + 1) + ": waiting for " + str(
                    wait_time) + " secs...")
                time.sleep(wait_time)
            else:
                retry_cnt = 0
        except Exception as e:
            print ("utils: call_method: ERROR: ", e)
            rows_inserted = 0
            rows_updated = 0
            rows_deleted = 0
            num_errors = 1
            error = 1
            err_msg = "ERROR: utils: call_method:  while calling method " + method_name
            status = 'Job Error'
            output_msg = traceback.format_exc()

            if len(output) == 0:
                output = {"error": error, "err_msg": err_msg, "status": status, "output_msg": output_msg, "rows_inserted": rows_inserted, \
                          "rows_updated": rows_updated, "rows_deleted": rows_deleted, "num_errors": num_errors, "step_location": step_location}
            else:
                output["output_msg"] = output_msg
            retry_cnt = 0

        if retry_cnt == 0:
            print ("utils: call_method_alt: No Retry")

        return output


class HiveTxn():

    LOAD_TYPE_FULL = 'FULL'
    LOAD_TYPE_APPEND_ONLY = 'APPEND_ONLY'
    LOAD_TYPE_INCREMENTAL = 'INCREMENTAL'

    def __init__(self):
        print ("[HiveTxn: __init__]: Entered")
        self.class_name = self.__class__.__name__
        self.config_list = load_config()
        self.data_path = None
        self.source_table_dlist = []
        self.target_table_dlist = []


    def read_hive(self, source_table_dlist, target_table_dlist, is_source_ddl=True):
        method_name = self.class_name + ": " + "read_hive"
        print_hdr = "[" + method_name + "] - "
        print (print_hdr + "Entered")

        table_list = []
        for table_d in target_table_dlist:
            table_list.append(table_d['table_name'])
        if source_table_dlist is not None and len(source_table_dlist) > 0:
            for table_d in source_table_dlist:
                table_list.append(table_d['table_name'])

        print (print_hdr + "table_list: ", table_list)
        conn_hivemeta = None
        conn_hive = None
        tmp_source_table_dlist = []
        tmp_target_table_dlist = []
        error = 0
        err_msg = ''
        output = {}

        try:
            table_filter_list = ",".join("'" + l + "'" for l in table_list)

            hivemeta_sql = "SELECT  concat(concat(d.NAME, '.'), t.TBL_NAME) AS FULL_TABLE_NAME, d.NAME AS SCHEMA_NAME, t.TBL_NAME AS TABLE_NAME, \
                                    CASE WHEN t.TBL_TYPE LIKE '%VIEW'THEN 'VIEW' WHEN t.TBL_TYPE LIKE '%TABLE' THEN 'TABLE' ELSE t.TBL_TYPE END AS TABLE_TYPE, \
                                    coalesce(p.PARTITION_COL_CNT,0) AS PARTITION_COL_CNT, p.PARTITION_COLS, t.VIEW_EXPANDED_TEXT AS VIEW_SQL \
                                    FROM TBLS t inner join DBS d \
                                    on d.DB_ID = t.DB_ID \
                                    left outer join (select TBL_ID, count(*) as PARTITION_COL_CNT, group_concat(PKEY_NAME) as PARTITION_COLS from hive.PARTITION_KEYS group by TBL_ID) p \
                                    on p.TBL_ID = t.TBL_ID \
                                WHERE 1 = 1 \
                                    AND concat(concat(d.NAME, '.'), t.TBL_NAME) IN (" + table_filter_list + ")"

            print (print_hdr + "hivemeta_sql: " + hivemeta_sql)
            conn_hivemeta = mysql.connector.connect(user=self.config_list['mysql_dbUser'], password=base64.b64decode(self.config_list['mysql_dbPwd']),
                                                    host=self.config_list['mysql_dbUrl'], database=self.config_list['mysql_dbMetastore_dbName'])

            cur_hivemeta = conn_hivemeta.cursor()
            cur_hivemeta.execute(hivemeta_sql)
            hivemeta_results = cur_hivemeta.fetchall()
            print (print_hdr + "hivemeta_result: ", hivemeta_results)

            staging_path = self.config_list['misc_hdfsStagingPath']
            warehouse_path = self.config_list['misc_hiveWarehousePath']
            cur_hive = None
            target_cnt = 0
            if len(hivemeta_results) > 0:
                try:
                    conn_hive, cur_hive = dbConnectHive(self.config_list['tgt_db_hive_dbUrl'], self.config_list['tgt_db_hive_dbPort'], self.config_list['tgt_db_hive_authMech'])
                except Exception as e:
                    try:
                        conn_hive, cur_hive = dbConnectHive(self.config_list['tgt_db_hive_dbUrl2'], self.config_list['tgt_db_hive_dbPort'], self.config_list['tgt_db_hive_authMech'])
                    except Exception as e:
                        raise

                cur_hive.execute("SET hive.exec.dynamic.partition = true")
                cur_hive.execute("SET hive.exec.dynamic.partition.mode = nonstrict")
                # Not in list of params that are allowed to be modified at runtime
                # cur_hive.execute("SET mapred.input.dir.recursive = true")
                # cur_hive.execute("SET hive.mapred.supports.subdirectories = true")

                for result in hivemeta_results:
                    full_tablename, schema_name, table_name, table_type, partition_col_cnt, partition_cols, view_sql = result

                    is_target = False
                    is_source = False
                    for target_dict in target_table_dlist:
                        if full_tablename == target_dict.get('table_name'):
                            target_dict['partition_col_cnt'] = partition_col_cnt
                            target_dict['partition_cols'] = partition_cols
                            tmp_target_table_dlist.append(target_dict)
                            is_target = True
                            target_cnt += 1

                    for source_dict in source_table_dlist:
                        if full_tablename == source_dict.get('table_name'):
                            tmp_source_table_dlist.append(source_dict)
                            is_source = True

                    tmp_table = table_name + '_tmp'
                    # tmp_path = staging_path + schema_name + '.db/' + tmp_table + '/'
                    tmp_path = staging_path + schema_name + '/' + tmp_table + '/'
                    whs_tmp_path = warehouse_path + schema_name + '.db/' + tmp_table + '/'

                    if is_target or (is_source and is_source_ddl):

                        (ret_rm, out_rm, err_rm) = run_cmd(['hadoop', 'fs', '-rm', '-r', '-f', (tmp_path + '*')])
                        print (print_hdr + "removing staging tmp dir - STATUS: " + str(ret_rm))

                        drop_hive_temp_ext_table = "DROP TABLE IF EXISTS `" + schema_name + "." + tmp_table + "`"
                        print (print_hdr + "drop_hive_temp_ext_table: " + drop_hive_temp_ext_table)
                        cur_hive.execute(drop_hive_temp_ext_table)

                        create_hive_temp_ext_table = "CREATE EXTERNAL TABLE `" + schema_name + "." + tmp_table + "` LIKE `" + schema_name + "." + table_name + "` \
                                                STORED AS ORC LOCATION '" + tmp_path + "' \
                                                TBLPROPERTIES ('orc.compress'='ZLIB')"

                        print (print_hdr + "create_hive_temp_ext_table: " + create_hive_temp_ext_table)
                        cur_hive.execute(create_hive_temp_ext_table)

                    if is_source and is_source_ddl:
                        if table_type == 'VIEW':
                            # Change location of tmp table from warehouse path to staging path
                            alter_location_sql = "ALTER TABLE `" + schema_name + "." + tmp_table + "` SET LOCATION 'hdfs://getnamenode" + tmp_path + "'"
                            print (print_hdr + "alter_location_sql: " + alter_location_sql)
                            cur_hive.execute(alter_location_sql)

                            # insert_tmp_table_sql = "INSERT OVERWRITE TABLE `" + schema_name + "." + tmp_table + "`" + view_sql
                            insert_tmp_table_sql = "INSERT OVERWRITE TABLE `" + schema_name + "." + tmp_table + "` SELECT * FROM `" + full_tablename + "`"
                            print (print_hdr + "insert_tmp_table_sql: " + insert_tmp_table_sql)
                            cur_hive.execute(insert_tmp_table_sql)

                            # Remove warehouse path created for tmp table from view
                            (ret_rm, out_rm, err_rm) = run_cmd(['hadoop', 'fs', '-rm', '-r', '-f', whs_tmp_path])
                            print (print_hdr + "removing warehouse tmp dir - STATUS: " + str(ret_rm))

                        elif table_type == 'TABLE':
                            insert_tmp_table_sql = "INSERT OVERWRITE TABLE `" + schema_name + "." + tmp_table + \
                                                   "` SELECT * FROM `" + schema_name + "." + table_name + "`"

                            print (print_hdr + "insert_tmp_table_sql: " + insert_tmp_table_sql)
                            cur_hive.execute(insert_tmp_table_sql)

            if (len(hivemeta_results) == 0) or (target_cnt == 0):
                rows_inserted = 0
                rows_updated = 0
                rows_deleted = 0
                num_errors = 1
                error = 1
                err_msg = "ERROR: HiveTxn: read_hive: Table details not existing in Hive MetaStore"
                status = 'Job Error'
                output_msg = "Table details not existing in Hive MetaStore"
                output = {"error": error, "err_msg": err_msg, "status": status, "output_msg": output_msg, "rows_inserted": rows_inserted, \
                          "rows_updated": rows_updated, "rows_deleted": rows_deleted, "num_errors": num_errors}

            self.source_table_dlist = tmp_source_table_dlist
            self.target_table_dlist = tmp_target_table_dlist

        except Exception as e:
            rows_inserted = 0
            rows_updated = 0
            rows_deleted = 0
            num_errors = 1
            error = 1
            err_msg = "ERROR: HiveTxn: read_hive: while reading hive transactions"
            status = 'Job Error'
            output_msg = traceback.format_exc()
            output = {"error": error, "err_msg": err_msg, "status": status, "output_msg": output_msg, "rows_inserted": rows_inserted, \
                      "rows_updated": rows_updated, "rows_deleted": rows_deleted, "num_errors": num_errors}
        finally:
            try:
                if conn_hivemeta is not None:
                    conn_hivemeta.close()
            except: pass

            try:
                if conn_hive is not None:
                    conn_hive.close()
            except: pass

        if len(output) == 0:
            output = {"error": error, "err_msg": err_msg}
        return output


    def write_hive(self, sc):
        method_name = self.class_name + ": " + "write_hive"
        print_hdr = "[" + method_name + "] - "
        print (print_hdr + "Entered")

        conn_hive = None
        error = 0
        err_msg = ''
        output = {}

        try:
            staging_path = self.config_list['misc_hdfsStagingPath']
            warehouse_path = self.config_list['misc_hiveWarehousePath']
            conn_hive = None
            try:
                conn_hive, cur_hive = dbConnectHive(self.config_list['tgt_db_hive_dbUrl'], self.config_list['tgt_db_hive_dbPort'], self.config_list['tgt_db_hive_authMech'])
            except Exception as e:
                try:
                    conn_hive, cur_hive = dbConnectHive(self.config_list['tgt_db_hive_dbUrl2'], self.config_list['tgt_db_hive_dbPort'], self.config_list['tgt_db_hive_authMech'])
                except Exception as e:
                    raise

                cur_hive.execute("SET hive.exec.dynamic.partition = true")
                cur_hive.execute("SET hive.exec.dynamic.partition.mode = nonstrict")
                # Not in list of params that are allowed to be modified at runtime
                # cur_hive.execute("SET mapred.input.dir.recursive = true")
                # cur_hive.execute("SET hive.mapred.supports.subdirectories = true")

            print (print_hdr + "target_table_dlist: ", self.target_table_dlist)
            for table_dict in self.target_table_dlist:
                table = table_dict['table_name']
                load_type = table_dict.get('load_type', self.LOAD_TYPE_FULL)
                s3_backed = table_dict.get('s3_backed', False)

                partition_col_cnt = table_dict['partition_col_cnt']
                partition_cols = table_dict['partition_cols']

                schema_name, table_name = table.split('.')
                tmp_table = table_name + '_tmp'
                # tmp_path = staging_path + schema_name + '.db/' + tmp_table + '/'
                tmp_path = staging_path + schema_name + '/' + tmp_table + '/'
                hdfs_tmp_path = 'hdfs://getnamenode' + tmp_path

                if s3_backed:
                    target_path = 's3a://' + self.config_list['s3_bucket_name'] + '/' + schema_name + '/' + table_name + '/'
                else:
                    target_path = warehouse_path + schema_name + '.db/' + table_name + '/'

                print (print_hdr + "temp_path: " + tmp_path)
                print (print_hdr + "hdfs_tmp_path: " + hdfs_tmp_path)
                print (print_hdr + "target_path: " + target_path)

                ret_rm = 0
                err_rm = ''
                ret_mv = 0
                err_mv = ''

                table_lock_sql = "LOCK TABLE `" + schema_name + "." + table_name + "` EXCLUSIVE"
                table_unlock_sql = "UNLOCK TABLE `" + schema_name + "." + table_name + "`"

                try:
                    print (print_hdr + "table_lock_sql: " + table_lock_sql)
                    cur_hive.execute(table_lock_sql)

                    if (load_type == self.LOAD_TYPE_FULL) or (load_type == self.LOAD_TYPE_INCREMENTAL and partition_col_cnt == 0):
                        (ret_rm, out_rm, err_rm) = run_cmd(['hadoop', 'fs', '-rm', '-r', '-f', (target_path + "*")])
                        print (print_hdr + load_type + ": removing files from target - STATUS: " + str(ret_rm))

                        if s3_backed:
                            (ret_mv, out_mv, err_mv) = run_cmd(['hadoop', 'distcp', '-D', 'fs.s3a.fast.upload=true', (hdfs_tmp_path + "*"), target_path])
                            print (print_hdr + load_type + ": distcp files from temp to s3 target - STATUS: " + str(ret_mv))
                        else:
                            (ret_mv, out_mv, err_mv) = run_cmd(['hadoop', 'fs', '-mv', (tmp_path + "*"), target_path])
                            print (print_hdr + load_type + ": moving files from temp to target - STATUS: " + str(ret_mv))

                    elif load_type == self.LOAD_TYPE_APPEND_ONLY and partition_col_cnt == 0:
                        if s3_backed:
                            (ret_mv, out_mv, err_mv) = run_cmd(['hadoop', 'distcp', (hdfs_tmp_path + "*"), target_path])
                            print (print_hdr + load_type + ": distcp files from temp to s3 target - STATUS: " + str(ret_mv))
                        else:
                            (ret_mv, out_mv, err_mv) = run_cmd(['hadoop', 'fs', '-mv', (tmp_path + "*"), target_path])
                            print (print_hdr + load_type + ": moving files from temp to target -  STATUS: " + str(ret_mv))

                    elif (load_type == self.LOAD_TYPE_INCREMENTAL or load_type == self.LOAD_TYPE_APPEND_ONLY) and partition_col_cnt > 0:
                        from pyspark.sql import HiveContext

                        sqlContext = HiveContext(sc)
                        sqlContext.setConf("hive.exec.dynamic.partition", "true")
                        sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
                        sqlContext.setConf("spark.sql.orc.filterPushdown", "true")
                        sqlContext.setConf("mapred.input.dir.recursive", "true")
                        sqlContext.setConf("hive.mapred.supports.subdirectories", "true")

                        # Disabled till assertion issue resolved
                        # sqlContext.setConf("spark.sql.orc.enabled", "true")
                        # sqlContext.setConf("spark.sql.hive.convertMetastoreOrc", "true")
                        # sqlContext.setConf("spark.sql.orc.char.enabled", "true")

                        sc._jsc.hadoopConfiguration().set('fs.s3a.attempts.maximum', '30')

                        sqlContext.setConf("hive.hadoop.supports.splittable.combineinputformat", "true")
                        sqlContext.setConf("tez.grouping.min-size", "1073741824")
                        sqlContext.setConf("tez.grouping.max-size", "2147483648")
                        sqlContext.setConf("mapreduce.input.fileinputformat.split.minsize", "2147483648")
                        sqlContext.setConf("mapreduce.input.fileinputformat.split.maxsize", "2147483648")
                        sqlContext.setConf("hive.merge.smallfiles.avgsize", "2147483648")
                        sqlContext.setConf("hive.exec.reducers.bytes.per.reducer", "2147483648")

                        select_tmp_sql = "SELECT * FROM " + schema_name + "." + tmp_table
                        print (print_hdr + "select_tmp_sql: " + select_tmp_sql)
                        final_df = sqlContext.sql(select_tmp_sql)

                        if load_type == self.LOAD_TYPE_APPEND_ONLY:
                            partition_cols_list =  partition_cols.split(",")
                            final_df.write.option("compression", "zlib").mode("append").format("orc").partitionBy(partition_cols_list).save(target_path)
                        else:
                            spark_tmp_table = table_name + '_stg'
                            final_df.createOrReplaceTempView(spark_tmp_table)

                            insert_ext_target_sql = 'INSERT OVERWRITE TABLE ' + schema_name + '.' + table_name + \
                                        ' PARTITION ( ' + partition_cols + ' ) \
                                        SELECT * FROM ' + spark_tmp_table

                            print (print_hdr + "insert_ext_target_sql: " + insert_ext_target_sql)
                            sqlContext.sql(insert_ext_target_sql)

                    else:
                        error = 1
                        err_msg = "ERROR: HiveTxn: write_hive: Not entered control flow for loading target table"
                        status = 'Job Error'
                        output_msg = "Not entered control flow for loading target table"
                        output = {"error": error, "err_msg": err_msg, "status": status, "output_msg": output_msg}

                    if ret_rm <> 0 or ret_mv <> 0:
                        error = ret_rm if ret_mv == 0 else ret_mv
                        err_msg = "ERROR: HiveTxn: write_hive: Error with unix commnds"
                        status = 'Job Error'
                        output_msg = err_rm[0:3997] if ret_mv == 0 else err_mv[0:3997]
                        print (print_hdr + "ERROR output_msg: ", output_msg)
                        if output_msg.find("_SUCCESS") == -1:
                            output = {"error": error, "err_msg": err_msg, "status": status, "output_msg": output_msg}
                            break
                        else:
                            error = 0
                            err_msg = ''
                            print (print_hdr + "Ignored ERROR for _SUCCESS file exists in the target")

                except Exception as e:
                    raise
                finally:
                    try:
                        print (print_hdr + "table_unlock_sql: " + table_unlock_sql)
                        cur_hive.execute(table_unlock_sql)
                    except:
                        pass

                # if (load_type == self.LOAD_TYPE_INCREMENTAL or load_type == self.LOAD_TYPE_APPEND_ONLY) and partition_col_cnt > 0:
                #
                #     # Not in list of params that are allowed to be modified at runtime
                #     # cur_hive.execute("SET hive.hadoop.supports.splittable.combineinputformat = true")
                #     # cur_hive.execute("SET tez.grouping.min-size = 1073741824")
                #     # cur_hive.execute("SET tez.grouping.max-size = 2147483648")
                #     cur_hive.execute("SET mapreduce.input.fileinputformat.split.minsize = 2147483648")
                #     # cur_hive.execute("SET mapreduce.input.fileinputformat.split.maxsize = 2147483648")
                #     cur_hive.execute("SET hive.merge.smallfiles.avgsize = 2147483648")
                #     cur_hive.execute("SET hive.exec.reducers.bytes.per.reducer = 2147483648")
                #
                #     insert_ext_target_sql = "INSERT"
                #     if load_type == self.LOAD_TYPE_APPEND_ONLY:
                #         insert_ext_target_sql = insert_ext_target_sql + " INTO"
                #     else:
                #         insert_ext_target_sql = insert_ext_target_sql + " OVERWRITE"
                #     insert_ext_target_sql = insert_ext_target_sql + " TABLE `" + schema_name + "." + table_name + "` \
                #                     PARTITION ( " + partition_cols + " ) \
                #                     SELECT * FROM `" + schema_name + "." + tmp_table + "`"
                #
                #     # if load_type == self.LOAD_TYPE_APPEND_ONLY:
                #     #     insert_ext_target_sql = "INSERT INTO TABLE `" + schema_name + "." + table_name + "` \
                #     #                             PARTITION ( " + partition_cols + " ) \
                #     #                             SELECT * FROM `" + schema_name + "." + tmp_table + "`"
                #     #
                #     # else:
                #     #     insert_ext_target_sql = "INSERT OVERWRITE TABLE `" + schema_name + "." + table_name + "` \
                #     #                             PARTITION ( " + partition_cols + " ) \
                #     #                             SELECT * FROM `" + schema_name + "." + tmp_table + "`"
                #
                #     print (print_hdr + "insert_ext_target_sql: " + insert_ext_target_sql)
                #     cur_hive.execute(insert_ext_target_sql)

                if partition_col_cnt > 0:
                    analyze_table_sql = 'ANALYZE TABLE ' + schema_name + "." + table_name + ' partition (' + partition_cols + ') COMPUTE STATISTICS'
                    print (print_hdr + "analyze_table_sql: " + analyze_table_sql)
                    cur_hive.execute(analyze_table_sql)

                    repair_table_sql = 'MSCK REPAIR TABLE ' + schema_name + "." + table_name
                    print (print_hdr + "repair_table_sql: " + repair_table_sql)
                    cur_hive.execute(repair_table_sql)
                else:
                    analyze_table_sql = 'ANALYZE TABLE ' + schema_name + "." + table_name + ' COMPUTE STATISTICS'
                    print (print_hdr + "analyze_table_sql: " + analyze_table_sql)
                    cur_hive.execute(analyze_table_sql)

                # refresh_metadata_sql = 'REFRESH TABLE ' + schema_name + "." + table_name
                # print (print_hdr + "refresh_metadata_sql: " + refresh_metadata_sql)
                # cur_hive.execute(refresh_metadata_sql)

        except Exception as e:
            error = 2
            err_msg = "ERROR: HiveTxn: write_hive: while writing hive transactions"
            status = 'Job Error'
            output_msg = traceback.format_exc()
            output = {"error": error, "err_msg": err_msg, "status": status, "output_msg": output_msg}
        finally:
            try:
                if conn_hive is not None:
                    conn_hive.close()
            except: pass

        if len(output) == 0:
            output = {"error": error, "err_msg": err_msg}
        return output


    def cleanup_hive(self):
        method_name = self.class_name + ": " + "cleanup_hive"
        print_hdr = "[" + method_name + "] - "
        print (print_hdr + "Entered")

        conn_hive = None
        try:
            staging_path = self.config_list['misc_hdfsStagingPath']

            table_dlist = []
            if len(self.source_table_dlist) > 0:
                table_dlist.extend(self.source_table_dlist)
            if len(self.target_table_dlist) > 0:
                table_dlist.extend(self.target_table_dlist)

            print (print_hdr + "table_dlist: ", table_dlist)
            if len(table_dlist) > 0:
                try:
                    conn_hive, cur_hive = dbConnectHive(self.config_list['tgt_db_hive_dbUrl'], self.config_list['tgt_db_hive_dbPort'], self.config_list['tgt_db_hive_authMech'])
                except Exception as e:
                    try:
                        conn_hive, cur_hive = dbConnectHive(self.config_list['tgt_db_hive_dbUrl2'], self.config_list['tgt_db_hive_dbPort'], self.config_list['tgt_db_hive_authMech'])
                    except Exception as e:
                        raise

                for table_dict in table_dlist:
                    table = table_dict['table_name']
                    schema_name, table_name = table.split('.')
                    tmp_table = table_name + '_tmp'
                    # tmp_path = staging_path + schema_name + '.db/' + tmp_table + '/'
                    tmp_path = staging_path + schema_name + '/' + tmp_table + '/'

                    drop_hive_temp_ext_table = "DROP TABLE IF EXISTS `" + schema_name + "." + tmp_table + "`"
                    print (print_hdr + "drop_hive_temp_ext_table: " + drop_hive_temp_ext_table)
                    cur_hive.execute(drop_hive_temp_ext_table)

                    (ret_rm, out_rm, err_rm) = run_cmd(['hadoop', 'fs', '-rm', '-r', '-f', (tmp_path + "*")])
                    print (print_hdr + "removing files from tmp - STATUS: " + str(ret_rm))

        except Exception as e:
            print (print_hdr + "Ignored ERROR: ", traceback.format_exc())
        finally:
            try:
                if conn_hive is not None:
                    conn_hive.close()
            except: pass


    def get_datasync_objects(self, data_path, table_name):
        method_name = self.class_name + ": " + "get_datasync_objects"
        self.data_path = data_path
        print_hdr = "[" + method_name + ": " + data_path + "] - "
        print (print_hdr + "Entered")

        conn_metadata = None
        error = 0
        err_msg = ''
        output = {}

        try:
            control_sql = "SELECT  distinct concat(concat(source_schemaname,'.'),source_tablename) as source_full_tablename, \
                            concat(concat(target_schemaname,'.'),target_tablename) as target_full_tablename, \
                            source_schemaname, source_tablename, target_schemaname, target_tablename, incremental_column, \
                            second_last_run_time, s3_backed, load_type \
                            FROM sync.control_table \
                            where 1 = 1 \
                                and concat(concat(target_schemaname,'.'),target_tablename) = '" + table_name + "' \
                                and status_flag = 'Y' \
                                and data_path = '" + data_path + "'"

            print (print_hdr + "control_sql: " + control_sql)

            conn_metadata, cur_metadata = dbConnect(self.config_list['meta_db_dbName'], self.config_list['meta_db_dbUser'],
                                                    self.config_list['meta_db_dbUrl'], base64.b64decode(self.config_list['meta_db_dbPwd']))
            control_results = dbQuery(cur_metadata, control_sql)
            print (print_hdr + "control_results: ", control_results)

            source_table_dlist = []
            target_table_dlist = []
            if len(control_results) > 0:
                if data_path.find("HDFS2MIR") <> -1:
                    target_table_dlist.append({'table_name': str(control_results[0]['target_full_tablename']), 'load_type': str(control_results[0]['load_type']), \
                                               's3_backed': bool(control_results[0]['s3_backed'])})
                else:
                    source_table_dlist.append({'table_name': str(control_results[0]['source_full_tablename']), 'load_type': str(control_results[0]['load_type']), \
                                             'incremental_column': str(control_results[0]['incremental_column']), 'last_run_time': str(control_results[0]['second_last_run_time'])})
                    target_table_dlist.append({'table_name': str(control_results[0]['target_full_tablename']), 'load_type': str(control_results[0]['load_type']), \
                                               's3_backed': bool(control_results[0]['s3_backed'])})

                output = self.read_hive(source_table_dlist, target_table_dlist)
            else:
                rows_inserted = 0
                rows_updated = 0
                rows_deleted = 0
                num_errors = 1
                error = 1
                err_msg = "ERROR: HiveTxn: get_datasync_objects: No control data available"
                status = 'Job Error'
                output_msg = 'No control data available'
                output = {"error": error, "err_msg": err_msg, "status": status, "output_msg": output_msg,"rows_inserted": rows_inserted, \
                          "rows_updated": rows_updated, "rows_deleted": rows_deleted, "num_errors": num_errors}

        except Exception as e:
            rows_inserted = 0
            rows_updated = 0
            rows_deleted = 0
            num_errors = 1
            error = 1
            err_msg = "ERROR: HiveTxn: get_datasync_objects: while getting datasync objects"
            status = 'Job Error'
            output_msg = traceback.format_exc()
            output = {"error": error, "err_msg": err_msg, "status": status, "output_msg": output_msg, "rows_inserted": rows_inserted, \
                      "rows_updated": rows_updated, "rows_deleted": rows_deleted, "num_errors": num_errors}
        finally:
            if conn_metadata is not None:
                conn_metadata.close()

        if len(output) == 0:
            output = {'error': error, 'err_msg': err_msg}
        return output
