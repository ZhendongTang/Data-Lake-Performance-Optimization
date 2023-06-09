package org.ccf.bdci2022.datalake_contest

import com.dmetasoul.lakesoul.tables.LakeSoulTable
import mergeTools.MergeOpLong
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import tools.{MergePartitionInfoTable, dataLakeTable}

object Read {

  /**
   * @note 读取任务 无shuffle 无upload s3
   */
  def main(args: Array[String]): Unit = {
    val builder = SparkSession.builder()
      .appName("CCF BDCI 2022 DataLake Contest")
      .master("local[4]")
      //----------- s3 -----------------
      .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3.buffer.dir", "/opt/spark/work-dir/s3")
      .config("spark.hadoop.fs.s3a.buffer.dir", "/opt/spark/work-dir/s3a")
      .config("spark.hadoop.fs.s3a.fast.upload.buffer", "array")
      .config("spark.hadoop.fs.s3a.fast.upload", value = true)
      .config("spark.hadoop.fs.s3a.multipart.size", 55*1024*1024)
      .config("spark.hadoop.fs.s3a.connection.maximum", 150)
      .config("fs.s3a.connection.timeout", 20000000)
      //----------- parquet split -----------------
      .config("spark.sql.parquet.mergeSchema", value = false)
      .config("spark.sql.parquet.filterPushdown", value = false)
      .config("spark.sql.files.maxPartitionBytes", 3 * 1024 * 1024)
      .config("mapreduce.input.fileinputformat.split.minsize", 3 * 1024 * 1024)
      .config("mapreduce.input.fileinputformat.split.maxsize", 3 * 1024 * 1024)
      .config("spark.sql.files.openCostInBytes", 3 * 1024 * 1024)
      .config("spark.default.parallelism", 1)
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "45m")
      .config("spark.sql.adaptive.coalescePartitions.minPartitionNum", "4")

      //----------- commiter -----------------
      //version=2
      //commitTask 执行的时候，会调用 mergePaths 方法直接将 Task 生成的数据从 Task 临时目录移动到程序最后生成目录
      //而在执行 commitJob 的时候 不用再移动数据
      .config("spark.hadoop.mapred.output.committer.class", "org.apache.hadoop.mapred.FileOutputCommitter")
      .config("mapreduce.fileoutputcommitter.algorithm.version", 2)

      //----------- base -----------------
      .config("spark.sql.shuffle.partitions", 16)
      .config("spark.driver.maxResultSize", "2g")
      .config("spark.sql.warehouse.dir", "s3://ccf-datalake-contest/datalake_table/")
      .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog")
      .config("spark.sql.parquet.enableVectorizedReader",value = true)
      .config("spark.shuffle.file.buffer","190m")
      .config("spark.driver.maxResultSize","4G")
      //禁止Parquet元数据摘要 parquet.enable.summary-metadata已过期 新的为parquet.summary.metadata.level ALL/NONE
      .config("parquet.summary.metadata.level","NONE")
      //减少s3权限检查次数
      .config("hive.warehouse.subdir.inherit.perms", false)
      //内存划分
      .config("spark.shuffle.memoryFraction", 0.1)
      .config("spark.memory.fraction", 0.7)
      .config("spark.storage.memoryFraction", 0.1)
      .config("spark.dmetasoul.lakesoul.bucket.scan.multi.partition.enable",true)
      //rdd序列化
      //.config("spark.rdd.compress",true)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("registerKryoClasses", classOf[String].getName)
      .config("registerKryoClasses", Short.getClass.getName)
      .config("registerKryoClasses", "tools.dataLakeTable")

    if (args.length >= 1 && args(0) == "--localtest")
      builder.config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")

    val spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    //表地址以及输出地址信息
    val tablePath= "s3://ccf-datalake-contest/datalake_table"
    val finalSavePath = "/opt/spark/work-dir/result/ccf/"

    //注册读时合并所用Operator，无需注册默认的Operator
    LakeSoulTable.registerMergeOperator(spark,
      classOf[MergeOpLong].getName,
      "SumMergeOpLong")
    LakeSoulTable.registerMergeOperator(spark,
       "mergeTools.MergeNonNullOp",
      "MergeNonNullOp")

    try{
      //合并元数据表partition_info中的多次upsert，
      //按version大小升序合并snapshot快照列表
      MergePartitionInfoTable.mergePartitionInfo()

      //读时合并，未指定Operator的采用默认DefaultMergeOp，合并规则为取最后一个
      LakeSoulTable.forPath(tablePath)
        .toDF
        .selectExpr("uuid", "ip", "hostname",
          "SumMergeOpLong(requests) as requests",
          "MergeNonNullOp(name) as name",
          "city", "job", "phonenum")
        .write
        .format("parquet")
        .save(finalSavePath)
    } finally {
      spark.close()
    }
  }
}
