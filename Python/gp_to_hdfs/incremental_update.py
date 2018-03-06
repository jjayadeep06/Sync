import  mysql.connector
import ConfigParser
import os
from itertools import chain
from readCfg import read_config
import base64
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext
import sys
import subprocess
#****************************************************************************************************************************************#
# Name       | Date              | Version                      | Comments                                                               #
#****************************************************************************************************************************************#
# Jayadeep   |  31-May-2017      | 0.1                          | Initial Implementation                                                 #
#****************************************************************************************************************************************#
def run_cmd(args_list):
    print('Running system command: {0}'.format(' '.join(args_list)))
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    s_output, s_err = proc.communicate()
    s_return =  proc.returncode
    return s_return, s_output, s_err

#merge all into one config dictionary
def merge_data (sc,table_name):
    print ("Entered Merged data Function Testing")

    sqlContext = HiveContext(sc)
    config = read_config(['/apps/incremental/hdp/environ.properties'])


    input_schema_name, input_table_name = table_name.split('.')

    if(config == None):
        print "Configuration Entry Missing"
        sys.exit(1)

    # get the current branch (from local.properties)
    env            = config.get('branch','env')

    # proceed to point everything at the 'branched' resources
    dbUrl                      = config.get(env+'.mysql','dbUrl')
    dbUser                     = config.get(env+'.mysql','dbUser')
    dbPwd                      = base64.b64decode(config.get(env+'.mysql','dbPwd'))
    dbMetastore_dbName         = config.get(env+'.mysql','dbMetastore_dbName')
    dbApp_dbName               = config.get(env+'.mysql','dbApp_dbName')
    bucket_name                = config.get(env+'.s3','bucket_name')

    print (dbUrl,",",dbUser,",",dbPwd,",",dbMetastore_dbName,",",dbApp_dbName)
    # Connection to the Hive Metastore to get column and partition list
    connection     =  mysql.connector.connect(user=str(dbUser),password=str(dbPwd),host=str(dbUrl),database=str(dbApp_dbName))

    # Get control table access
    try:
        cursor     = connection.cursor()
        merge_sql = "SELECT * FROM application.control_table WHERE target_schemaname = '" + input_schema_name + "' and target_tablename = '" + input_table_name + "'"
        print merge_sql
        cursor.execute(merge_sql)

        control     = cursor.fetchall()
    except Exception as e:
        print 'Issue connectining to metadata database:', e
    finally:
        connection.close()

    control_list = list(chain.from_iterable(control))

    if  not control_list:
        print "Control Entry missing in table"
        sys.exit(1)

    source_schema                     = str(control_list[1])
    source_tablename                  = str(control_list[2])
    target_schema                     = str(control_list[3])
    target_tablename                  = str(control_list[4])
    partitioned                       = control_list[5]
    load_type                         = str(control_list[6])
    s3_backed                         = control_list[7]
    first_partitioned_column          = str(control_list[8])
    second_partitioned_column         = str(control_list[9])
    partitioned_column_transformation = str(control_list[10])
    custom_sql                        = str(control_list[11])
    join_columns                      = str(control_list[12])
    archived_enabled                  = control_list[13]
    distribution_columns              = str(control_list[18])
    dist_col_transformation           = str(control_list[19])

    print distribution_columns, dist_col_transformation

    # Connection to the Hive Metastore to get column and partition list
    connection     =  mysql.connector.connect(user=dbUser, password=dbPwd,host=dbUrl,database=dbMetastore_dbName)

    # Establish connection to the hive metastore to get the list of columns
    try:
        cursor     = connection.cursor()
        #cursor.execute("""SELECT COLUMN_NAME, TBL_NAME FROM COLUMNS_V2 c JOIN TBLS a ON c.CD_ID=a.TBL_ID where a.TBL_ID = 52""")
        #cursor.execute("""SELECT COLUMN_NAME FROM COLUMNS_V2 c JOIN TBLS a ON c.CD_ID=a.TBL_ID where a.TBL_ID = 52""")
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
                        TBL_NAME = " + "'" + target_tablename + "' " + "                      \
                        AND d.NAME=" + " '" + target_schema + "' " +  "                       \
                        ORDER by c.INTEGER_IDX"

        cursor.execute(sql_query)
        target_result     = cursor.fetchall()


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
                        TBL_NAME = " + "'" + source_tablename + "' " + "                      \
                        AND d.NAME=" + " '" + source_schema + "' " +  "                       \
                        ORDER by c.INTEGER_IDX"

        cursor.execute(sql_query)
        source_result     = cursor.fetchall()


    except Exception as e:
        print 'Issue running SQL in hive metadata database:', e
        raise
    finally:
        connection.close()

    # Get the column on which the table is partitioned
    source_select_list           = ', '.join(map(''.join,source_result))
    target_select_list           = ', '.join(map(''.join,target_result))

    if not source_select_list:
        print "Hive Table Not Found in metadata database"
        sys.exit(1)
    # Create the SELECT query string for fetching data from the external table
    if len(dist_col_transformation) > 0 :
        target_select_list           = target_select_list
        source_select_list           = source_select_list + ' , ' + dist_col_transformation

    if (partitioned):
        incremental_sql_query = 'select ' + source_select_list + ', ' + partitioned_column_transformation + ' from ' + source_schema + '.' + source_tablename
        if second_partitioned_column <> 'None':
            target_sql_query      = 'select ' + target_select_list + ', ' + first_partitioned_column + ', ' + second_partitioned_column + ' from ' + target_schema + '.' + target_tablename
        else:
            target_sql_query      = 'select ' + target_select_list + ', ' + first_partitioned_column + ' from ' + target_schema + '.' + target_tablename
    else:
        incremental_sql_query = 'select ' + source_select_list + ' from ' + source_schema + '.' + source_tablename
        target_sql_query      = 'select ' + target_select_list + ' from ' + target_schema + '.' + target_tablename

    connection     =  mysql.connector.connect(user=dbUser, password=dbPwd,host=dbUrl,database=dbMetastore_dbName)
    try:
        cursor     = connection.cursor()
        #cursor.execute("""SELECT COLUMN_NAME, TBL_NAME FROM COLUMNS_V2 c JOIN TBLS a ON c.CD_ID=a.TBL_ID where a.TBL_ID = 52""")
        #cursor.execute("""SELECT COLUMN_NAME FROM COLUMNS_V2 c JOIN TBLS a ON c.CD_ID=a.TBL_ID where a.TBL_ID = 52""")
        bloom_sql_query = "SELECT e.PARAM_VALUE                                                 \
                           FROM                                                                 \
                                TBLS t                                                          \
                           JOIN DBS d                                                           \
                              ON t.DB_ID = d.DB_ID                                              \
                           LEFT OUTER JOIN TABLE_PARAMS e                                       \
                                ON t.TBL_ID = e.TBL_ID                                          \
                                AND PARAM_KEY = 'orc.bloom.filter.columns'                      \
                           WHERE                                                                \
                                TBL_NAME = " + "'" + target_tablename + "' " + "                \
                                AND d.NAME=" + " '" + target_schema + "' "                      \

        cursor.execute(bloom_sql_query)
        bloom_filter      = cursor.fetchall()
    except Exception as e:
        print 'Issue running SQL in hive metadata database:', e
        raise
    finally:
        connection.close()

    bloom_filters_columns = ''
    if len(bloom_filter) > 1:
        bloom_filters_columns = ','.join(map(''.join,bloom_filter))
        bloom_filter_list = bloom_filters_columns.split ","
    # Execute the query to get the data into Spark Memory

    # Figure out if it is incremental or full load process
    # If Full Load then truncate the target table and insert the entire incoming data
    # If Incremental Load then determine if the table is partitioned as the logic needs to be handled differently for partitioned and non-partitioned tables
    #      For Non Partitioned Tables merge the incoming data with the table date and save it to the database
    #      For Partitioned Tables identify the partitions for which there is incremental data and intelligently merge the data and save it to the database table
    table_name = target_schema + '.' + target_tablename
    sqlContext.setConf("hive.exec.dynamic.partition", "true")
    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    sqlContext.setConf("spark.sql.orc.filterPushdown", "true")
    sqlContext.setConf("mapred.input.dir.recursive", "true")
    sqlContext.setConf("hive.mapred.supports.subdirectories", "true")

    sc._jsc.hadoopConfiguration().set('fs.s3a.attempts.maximum','30')

    if s3_backed:
         path = 's3a://' + bucket_name + '/' + target_schema + '/' + target_tablename + '/'
    else:
         path = '/apps/hive/warehouse/'  + target_schema + '.db/' + target_tablename + '/'

    if second_partitioned_column <> 'None':
        partitioned_columns = first_partitioned_column + second_partitioned_columns
    else:
        partitioned_columns = first_partitioned_column

    if len (distribution_columns) > 0 :
       bucket_columns = partitioned_columns + distribution_columns
       bucket_column_list = bucket_columns.split(",")
    else:
       bucket_columns = partitioned_columns
       bucket_column_list = bucket_columns.split(",")

    if len (partitioned_columns) > 0:
       partition_column_list = partitioned_columns.split(",")

    from pyspark.sql.functions import col
    try:
####################################################################################################################################################################
# Below logic is to sort the data based on the bloom filter columns across multiple tasks. This is the most optimal way for storing data for efficient             #
# reads but it takes a lot of time to load the data as the data is stored one partition at a time. We can speed up the process by persisting data but then if the  #
# partitions are not equally sized there are chances of shuffle reads crossing 2GB limit causing MAX_INT error.                                                    #
#                                                                                                                                                                  #
# Solution for Spark 2.1 : To sort the data by task so that the performance would be better than not sorting but less efficient than the below process             #
# Solution for Spark 2.2 : Use the sortBy API being introduced                                                                                                     #
#                                                                                                                                                                  #
# The reason for commenting out the code instead of removing is to prove that the logic can be implemented technically in prior versions of Spark but it is very   #
# inefficient                                                                                                                                                      #
####################################################################################################################################################################
                    # first_partitioned_list = final_df.select(first_partitioned_column) \
                    #                         .rdd.flatMap(lambda x: x).distinct().collect()
                    # if second_partitioned_column <> 'None':
                    #     second_partitioned_list = final_df.select(second_partitioned_column)\
                    #                         .rdd.flatMap(lambda x: x).distinct().collect()
                    # #final_df.persist()
                    # if second_partitioned_column <> 'None':
                    #     for first_partition in first_partitioned_list:
                    #         for second_partition in  second_partitioned_list:
                    #             final_path = path + first_partitioned_column + '=' + format(first_partition) + '/' +  \
                    #                     second_partitioned_column + '=' + format(second_partition)
                    #             write_df = final_df.where(col(first_partitioned_column).isin(format(first_partition)) &
                    #                                       col(second_partitioned_column).isin(format(second_partition)))
                    #             save_df = write_df.drop(first_partitioned_column).drop(second_partitioned_column)
                    #             save_df.write.option("compression","zlib").mode("overwrite").format("orc").save(path)
                    # else:
                    #     for first_partition in first_partitioned_list:
                    #         final_path = path + first_partitioned_column + '=' + format(first_partition)
                    #         print path
                    #         write_df = final_df.where(col(first_partitioned_column).isin(format(first_partition)))
                    #         save_df = write_df.drop(first_partitioned_column)
                    #         save_df.write.option("compression","zlib").mode("overwrite").format("orc").save(final_path)
        if load_type == 'FULL':
            merge_df = sqlContext.sql(incremental_sql_query)
            if partitioned:
                if bloom_filters_columns:
                    final_df = merge_df.repartition(len(merge_df.select(bucket_column_list).distinct().collect()),bucket_column_list) \
                               .sortWithinPartitions(bloom_filter_list)
                else:
                    final_df = merge_df.repartition(len(merge_df.select(bucket_column_list).distinct().collect()),bucket_column_list)
                final_df.write.option("compression","zlib").mode("overwrite").format("orc").partitionBy(partition_column_list).save(path)
            else:
                if merge_df.rdd.getNumPartitions() > 300:
                    merge_coalesce_df = merge_df.coalesce(300)
                else:
                    merge_coalesce_df = merge_df
                if bloom_filters_columns:
                    save_df = merge_coalesce_df.sortWithinPartitions(bloom_filters_columns)
                    save_df.write.option("compression","zlib").mode("overwrite").format("orc").save(path)
                else:
                    merge_coalesce_df.write.option("compression","zlib").mode("overwrite").format("orc").save(path)
        # Incremental Logic for Append Only table especially for S3
        elif load_type == 'APPEND_ONLY':
            merge_df = sqlContext.sql(incremental_sql_query)
            if s3_backed:
                temp_table = target_tablename + '_tmp'
                temp_path  = '/apps/hive/warehouse/'  + target_schema + '.db/' + temp_table + '/'
            else:
                temp_path  = path
            print temp_path
            if  partitioned:
                if bloom_filters_columns:
                    final_df = merge_df.repartition(len(merge_df.select(bucket_column_list).distinct().collect()),bucket_column_list) \
                               .sortWithinPartitions(bloom_filter_list)
                else:
                    final_df = merge_df.repartition(len(merge_df.select(bucket_column_list).distinct().collect()),bucket_column_list)
                final_df.write.option("compression","zlib").mode("append").format("orc").partitionBy(partition_column_list).save(temp_path)
            else:
                if merge_df.rdd.getNumPartitions() > 300:
                    merge_coalesce_df = merge_df.coalesce(300)
                else:
                    merge_coalesce_df = merge_df
                if bloom_filters_columns:
                    save_df = merge_coalesce_df.sortWithinPartitions(bloom_filters_columns)
                    save_df.write.option("compression","zlib").mode("append").format("orc").save(temp_path)
                else:
                    merge_coalesce_df.write.option("compression","zlib").mode("append").format("orc").save(temp_path)
            if s3_backed:
                target_path = 's3a://' + bucket_name + '/' + target_schema + '/' + target_tablename
                source_path = 'hdfs://getnamenode/apps/hive/warehouse/'  + target_schema + '.db/' + target_tablename + '_tmp' + '/*'
                print source_path
                print target_path
                (ret, out, err) = run_cmd(['hadoop', 'distcp', source_path, target_path])
                (ret, out, err) = run_cmd(['hadoop','fs', '-rm','-r',source_path])
        else:
            if (partitioned):
                from pyspark.sql.functions import col
                incremental_df = sqlContext.sql(incremental_sql_query)

                first_partitioned_list = incremental_df.select(first_partitioned_column) \
                                        .rdd.flatMap(lambda x: x).distinct().collect()
                if second_partitioned_column <> 'None':
                    second_partitioned_list = incremental_df.select(second_partitioned_column)\
                                        .rdd.flatMap(lambda x: x).distinct().collect()
                    merge_df           = sqlContext.sql(target_sql_query)\
                                        .where(col(first_partitioned_column).isin(first_partitioned_list) & \
                                               col(second_partitioned_column).isin(second_partitioned_list))
                else:
                    merge_df           = sqlContext.sql(target_sql_query) \
                                        .where(col(first_partitioned_column).isin(first_partitioned_list))
                join_column_list       = join_columns.split(",")
                output_df              = merge_df.join(incremental_df,join_column_list,"leftanti")
                final_df               = output_df.union(incremental_df)
                if bloom_filters_columns:
                    save_df = final_df.repartition(len(final_df.select(bucket_column_list).distinct().collect()),bucket_column_list) \
                               .sortWithinPartitions(bloom_filter_list)
                else:
                    save_df = final_df.repartition(len(final_df.select(bucket_column_list).distinct().collect()),bucket_column_list)

                save_df.persist()
                save_df.count()

                #final_df.persist()
                if second_partitioned_column <> 'None':
                    for first_partition in first_partitioned_list:
                        for second_partition in  second_partitioned_list:
                            final_path = path + first_partitioned_column + '=' + format(first_partition) + '/' +  \
                                    second_partitioned_column + '=' + format(second_partition)
                            write_df = save_df.where(col(first_partitioned_column).isin(format(first_partition)) &
                                                      col(second_partitioned_column).isin(format(second_partition)))
                            out_df.write.option("compression","zlib").mode("overwrite").format("orc").partitionBy(partition_column_list).save(path)
                else:
                    for first_partition in first_partitioned_list:
                        final_path = path + first_partitioned_column + '=' + format(first_partition)
                        print path
                        write_df = save_df.where(col(first_partitioned_column).isin(format(first_partition)))
                        out_df.write.option("compression","zlib").mode("overwrite").format("orc").partitionBy(partition_column_list).save(final_path)
        # Incremental Update of non-partitioned table
            else:
                incremental_df = sqlContext.sql(incremental_sql_query)
                current_df = sqlContext.sql(target_sql_query)

                join_column_list       = join_columns.split(",")
                output_df              = current_df.join(incremental_df,join_column_list,"leftanti")

                merge_df = output_df.union(incremental_df)
                merge_df.persist()
                merge_df.count()

                if merge_df.rdd.getNumPartitions() > 300:
                    merge_coalesce_df = merge_df.coalesce(300)
                else:
                    merge_coalesce_df = merge_df
                if bloom_filters_columns:
                    save_df = merge_coalesce_df.sortWithinPartitions(bloom_filters_columns)
                    save_df.write.option("compression","zlib").mode("overwrite").format("orc").save(path)
                else:
                    merge_coalesce_df.write.option("compression","zlib").mode("overwrite").format("orc").save(path)

        if (partitioned):
            repair_table_sql = 'MSCK REPAIR TABLE ' + table_name
            sqlContext.sql(repair_table_sql)

        refresh_metadata_sql = 'REFRESH TABLE ' + table_name
        sqlContext.sql(refresh_metadata_sql)
        sqlContext.sql(refresh_metadata_sql)

    except Exception as e:
        print 'Exception while loading data:', e # coding=utf-8
        sys.exit(1)

    if archived_enabled:
        target_path = 's3a://' + bucket_name + '/' + target_schema + '/' + target_tablename + '_bkp/'
        if  s3_backed:
            source_path = 's3a://' + bucket_name + '/' + source_schema + '/' + source_tablename + '/'
        else:
            source_path = 'hdfs://apps/hive/warehouse/'  + source_schema + '.db/' + source_tablename + '/*'
        print source_path
        print target_path
        (ret, out, err) = run_cmd(['hadoop', 'distcp', source_path, target_path])
        print "Errors:",err

#
    # if archive_enabled :
    #     if s3_backed:
    #          hadoop_path = 's3a://' + bucket_name + '/' + target_schema + '/' + source_tablename + '/'
    #     else:
    #          hadoop_path = '/apps/hive/warehouse/'  + target_schema + '.db/' + source_tablename + '/'
    #
    #     s3_path = 's3a://' + bucket_name + '/' + target_schema + '/' + source_tablename + '_bkp' + '/'
    #     subprocess.call(["hadoop", "distcp", hadoop_path, s3_path])

#
   # Remove Incoming Staging Files at the end of the process
#    if  s3_backed:
#        hdfs_file_path = 's3a://' + bucket_name + '/' + source_schema + '/' + source_tablename + '/*'
#        print hdfs_file_path
#    else:
#        hdfs_file_path = '/apps/staging/'  + source_schema + '/' + source_tablename + '/*'
#        print hdfs_file_path
#    (ret, out, err) = run_cmd(['hdfs', 'dfs', '-rm', '-r', hdfs_file_path])

#    if err:
#       print 'Errror while removing files:', ret,out, err

if __name__ == "__main__":


    APP_NAME='incremental_update_' + str(sys.argv[1])
    table_name = sys.argv[1]
    # Configure OPTIONS
    conf = SparkConf().setAppName(APP_NAME)
    #in cluster this will be like
    #"spark://ec2-0-17-03-078.compute-#1.amazonaws.com:7077"
    sc   = SparkContext(conf=conf)
    sc.setLogLevel('ERROR')
    # Execute Main functionality
    merge_data(sc,table_name)
