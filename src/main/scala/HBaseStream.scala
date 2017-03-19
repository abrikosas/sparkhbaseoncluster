

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}



object HBaseAttackStream extends Serializable {
  final val tableName = "test:attacksv3"
  final val cfAttacker = Bytes.toBytes("af")
  //final val patternList = List("Failed","failed")
  final val patternList = List("Invalid")

  def main(args: Array[String]): Unit = {
    // set up HBase Table configuration
    val conf = HBaseConfiguration.create()
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    val jobConfig: JobConf = new JobConf(conf, this.getClass)
jobConfig.set("mapreduce.output.fileoutputformat.outputdir", "/tmp/out")
    jobConfig.setOutputFormat(classOf[TableOutputFormat])
    jobConfig.set(TableOutputFormat.OUTPUT_TABLE, tableName)


    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext()
    //    val spark = SparkSession
    //      .builder
    //      .appName(getClass.getSimpleName)
    //      .getOrCreate()
    //val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(1))
    println("Stream processing logic start")
    val attackDStream = ssc.socketTextStream("localhost", args(0).toInt, StorageLevel.MEMORY_AND_DISK_SER).map(Attack.parseEvent)
    attackDStream.print()

    attackDStream.foreachRDD { rdd =>

      // convert Attack data to put object and write to HBase table column family data
      rdd.map(Attack.convertToPut).saveAsHadoopDataset(jobConfig)
    }

    // Start the computation
    ssc.start()
    // Wait for the computation to terminate
    ssc.awaitTermination()

  }

  case class Attack(monthe: String, daye: String, timee: String, unknown: String, ssh: String,
                    failed: String, passw: String, forw: String, inv: String, user: String,
                    realuser: String, from: String, ip: String, port: String, unknown1: String,
                    protocol: String)

  case class ShortAttack(timestamp: String, realuser: String, ip: String)

  object Attack extends Serializable {
    def parseEvent(str: String): ShortAttack = {
      val a: Array[String] = str.split("\\s+").filter(_.length == 16).filter(l => patternList.exists(_.contains()))
      a.foreach(println)

      ShortAttack(a(0) + " " + a(1) + " " + a(2), a(9), a(12))

    }

    //  Convert a row of Attack object data to an HBase put object
    def convertToPut(attack: ShortAttack): (ImmutableBytesWritable, Put) = {
      val dateTime = attack.timestamp
      // create a composite row key: Attackid_date time
      val rowkey = attack.ip
      val put = new Put(Bytes.toBytes(rowkey))
      // add to column family data, column  data values to put object
      put.add(cfAttacker, Bytes.toBytes(dateTime), Bytes.toBytes(attack.realuser))
      return (new ImmutableBytesWritable(Bytes.toBytes(rowkey)), put)
    }

  }

}
