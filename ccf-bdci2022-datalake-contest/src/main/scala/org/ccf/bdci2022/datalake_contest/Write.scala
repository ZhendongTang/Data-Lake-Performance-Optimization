package org.ccf.bdci2022.datalake_contest

import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import tools.dataLakeTable

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object Write {
  def main(args: Array[String]): Unit = {
    val builder = SparkSession.builder()
      .appName("CCF BDCI 2022 DataLake Contest")
      .master("local[4]")
      //不合并schema
      .config("spark.sql.parquet.mergeSchema", value = false)
      .config("spark.sql.parquet.filterPushdown", value = true)
      .config("spark.hadoop.mapred.output.committer.class", "org.apache.hadoop.mapred.FileOutputCommitter")
      .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
      .config("spark.sql.warehouse.dir", "s3://ccf-datalake-contest/datalake_table/")
      .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog")
      .config("spark.kryoserializer.buffer.max", "900m")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("registerKryoClasses", "tools.dataLakeTable")
      .config("parquet.block.size",3*1024*1024)
      .config("dfs.blocksize", 3*1024*1024)
      .config("spark.sql.files.maxPartitionBytes", 900*1024*1024)
      .config("mapreduce.input.fileinputformat.split.minsize", 900*1024*1024)
      .config("mapreduce.input.fileinputformat.split.maxsize", 900*1024*1024)
      .config("spark.sql.files.openCostInBytes", 900*1024*1024)
      .config("set spark.sql.parquet.adaptiveFileSplit",value = true)
      //避免读文件的时候生成无用task
      .config("spark.default.parallelism", 1)
      .config("parquet.split.files", false)
      .config("parquet.task.side.metadata", "false")
      .config("parquet.enable.dictionary", "false")
      //s3
      .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      //partitioned  directory magic
      .config("hadoop.fs.s3a.committer.name", "directory")
      .config("spark.hadoop.fs.s3a.committer.magic.enabled", "false")
      .config("spark.hadoop.fs.s3a.committer.staging.conflict-mode", "append")
      .config("spark.hadoop.fs.s3a.committer.staging.tmp.path", "/opt/spark/work-dir/s3a_staging")
      .config("spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a", "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory")
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.buffer.dir", "/opt/spark/work-dir/s3a")
      .config("spark.hadoop.fs.s3a.fast.upload.buffer", "array")
      .config("spark.hadoop.fs.s3a.fast.upload", "true")
      .config("spark.hadoop.fs.s3a.committer.staging.unique-filenames", "true")
      .config("spark.hadoop.fs.s3a.committer.staging.abort.pending.uploads", "true")
//      .config("spark.sql.sources.commitProtocolClass", "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol")
//      .config("spark.sql.parquet.output.committer.class", "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter")
      .config("spark.hadoop.fs.s3a.multipart.size", 50*1024*1024)
      //shuffle参数
      .config("spark.sql.shuffle.partitions", 8)
//      .config("spark.shuffle.manager","tungsten-sort")
//      .config("spark.shuffle.manager", "sort")
      .config("spark.shuffle.sort.bypassMergeThreshold", 300)
      .config("spark.shuffle.consolidateFiles",true)
      //缓冲内存
      .config("spark.shuffle.file.buffer","20m")
      .config("spark.reducer.maxSizeInFlight","200m")
      //shuffle聚合内存的比例
      .config("spark.shuffle.memoryFraction", 0.8)
      .config("spark.memory.fraction", 0.7)
      .config("spark.storage.memoryFraction", 0.1)
      //启动堆外内存进行shuffle
      .config("spark.memory.offHeap.enabled",true)
      .config("spark.memory.offHeap.size","2g")
      //减少s3权限检查次数
      .config("hive.warehouse.subdir.inherit.perms", false)
      //禁止Parquet元数据摘要 parquet.enable.summary-metadata已过期 新的为parquet.summary.metadata.level ALL/NONE
      .config("parquet.summary.metadata.level", "NONE")


    if (args.length >= 1 && args(0) == "--localtest")
      builder.config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")

    val ss = builder.getOrCreate()
    ss.sparkContext.setLogLevel("ERROR")

    val tablePath = "s3://ccf-datalake-contest/datalake_table"
    val dataPath0 = "/opt/spark/work-dir/data/base-0.parquet"
    val dataPath1 = "/opt/spark/work-dir/data/base-1.parquet"
    val dataPath2 = "/opt/spark/work-dir/data/base-2.parquet"
    val dataPath3 = "/opt/spark/work-dir/data/base-3.parquet"
    val dataPath4 = "/opt/spark/work-dir/data/base-4.parquet"
    val dataPath5 = "/opt/spark/work-dir/data/base-5.parquet"
    val dataPath6 = "/opt/spark/work-dir/data/base-6.parquet"
    val dataPath7 = "/opt/spark/work-dir/data/base-7.parquet"
    val dataPath8 = "/opt/spark/work-dir/data/base-8.parquet"
    val dataPath9 = "/opt/spark/work-dir/data/base-9.parquet"
    val dataPath10 = "/opt/spark/work-dir/data/base-10.parquet"

    try {
      //step1: 创建lakeSoul表
      createLakeSoulTable(ss, tablePath)

      val pathList = List(dataPath0, dataPath1, dataPath2, dataPath3, dataPath4, dataPath5, dataPath6, dataPath7,
        dataPath8, dataPath9, dataPath10)

      implicit val singleLogEncoder: Encoder[dataLakeTable] = {
        Encoders.bean(classOf[dataLakeTable])
      }

      //step2: 通过Future并行执行所有路径文件的Upsert，线程池的提交顺序按照文件名数字顺序,Upsert时传入自定义的版本号，值为当次文件序号
      //step3: 遍历FutureList，Await所有任务执行成功
      pathList
        .map(path => Future {
            LakeSoulTable
              .forPath(tablePath)
              .upsert(ss.read.parquet(path).as[dataLakeTable].toDF(), pathList.indexOf(path))
            })
        .foreach(Await.ready(_, Duration.Inf))
    } finally {
      ss.close()
    }
  }

  /**
   * 通过spark-sql创建lakeSoul表
   * @param ss SparkSession
   * @param tablePath 表数据的存储地址，这里base s3地址
   * @note 以uuid作为hashPartition，由于excuter数量为4，分区数量设为4的倍数较优，
   *       <br>理论上分区数量为4，由于文件数较少，不需考虑小文件合并问题，可以得到最优的读任务性能，
   *       <br>但实际由于只有4个task时，写任务总时长取决于执行时间最长的那一个task，由于每个task执行时间存在浮动性，此时写任务时间时间方差较大。
   *       <br>采取8/12/16分区数从某种程度上可以减少结果时间的方差得到一个较稳定的结果，
   *       <br>但为得到最好成绩，考虑采用分区数为4多次执行。
   */
  def createLakeSoulTable(ss: SparkSession, tablePath: String): Unit = {
    ss.sql(s"CREATE TABLE t0(" +
      s" uuid string," +
      s" ip string, " +
      s" hostname string, " +
      s" requests Long, " +
      s" name string, " +
      s" city string, " +
      s" job string, " +
      s" phonenum String) " +
      s" USING lakesoul" +
      s" LOCATION '$tablePath'" +
      s" TBLPROPERTIES(" +
      s" 'hashPartitions'='uuid'," +
      s" 'hashBucketNum'='4')")
  }
}
