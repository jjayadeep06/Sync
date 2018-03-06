

from pyspark import SparkConf, SparkContext
import sys
from file2db import File2DBSync

if __name__ == "__main__":

    table_name      = str(sys.argv[1])
    load_id         = int(sys.argv[2])
    run_id          = int(sys.argv[3])
    data_path       = str(sys.argv[4])
    v_timestamp     = sys.argv[5]

    APP_NAME        = 'datasync_spark_driver_' + table_name
    conf            = SparkConf().setAppName(APP_NAME)
    conf.set("spark.driver.maxResultSize", "5G")

    sc              = SparkContext(conf=conf)
    sc.setLogLevel('ERROR')
    print ("[datasync_spark_driver: main] - " + data_path + ": " + str(load_id) + ": " + table_name + ": " + str(run_id))

    error = 0
    err_msg = ""

    file2db_sync = File2DBSync()
    # if data_path.find("HDFS2MIR") <> -1:
    #     output = file2db_sync.sync_hive_spark(sc, table_name, load_id, run_id, data_path, v_timestamp)
    # else:
    output = file2db_sync.sync_hive_wrapper(sc, table_name, load_id, run_id, data_path, v_timestamp)

    error = output["error"]
    err_msg = output["err_msg"]
    print ("[datasync_spark_driver: main] - ERROR: " + str(error) + " :: err_msg: " + err_msg)

    sys.exit(error)
