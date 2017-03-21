

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

/**
  * * @param args(0)        - port
  * * @param args(1)        - host
  * * @param args(2)        - hbase table name
  * * @param args(3)        - column family name
  * * @param args(4)        - window size
  * * @param args(5)        - mapreduce output
  */

object HBaseAttackStream extends Serializable {
  //final val cfAttacker = Bytes.toBytes("af")
  //final val patternList = List("Failed","failed")
  final val patternList = List("Invalid")
  //  final val tableName = "test:attacksv3"

  def main(args: Array[String]): Unit = {
    // set up HBase Table configuration
    val tableName = args(2)
    val cfAttacker = args(3)
    val host = args(1)
    val port = args(0).toInt
    val windowSize = args(4)
    val mrOutput = args(5)
    val conf = HBaseConfiguration.create()
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    val jobConfig: JobConf = new JobConf(conf, this.getClass)
    jobConfig.set("mapreduce.output.fileoutputformat.outputdir", mrOutput)
    jobConfig.setOutputFormat(classOf[TableOutputFormat])
    jobConfig.set(TableOutputFormat.OUTPUT_TABLE, tableName)


    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext()
    val ssc = new StreamingContext(sc, Seconds(windowSize.toInt))
    println("Stream processing logic start")
    val attackDStream = ssc.socketTextStream(host, port, StorageLevel.MEMORY_AND_DISK_SER).map(_.split("\\+"))
      .filter(_.length == 16).map(Attack.parseEvent)


    attackDStream.foreachRDD { rdd =>

      // convert Attack data to put object and write to HBase table column family data
      rdd.map(Attack.convertToPut(_, cfAttacker)).saveAsHadoopDataset(jobConfig)
    }

    ssc.start()
    ssc.awaitTermination()

  }

  /*case class Attack(monthe: String, daye: String, timee: String, unknown: String, ssh: String,
                    failed: String, passw: String, forw: String, inv: String, user: String,
                    realuser: String, from: String, ip: String, port: String, unknown1: String,
                    protocol: String)*/

  case class Attack(monthe: String, daye: String, timee: String, localHostName: String, ssh: String,
                    failed: String, userU: String, realuser: String, from: String, ip: String)

  case class ShortAttack(timestamp: String, realuser: String, ip: String)

  object Attack extends Serializable {
    def parseEvent(str: Array[String]): ShortAttack = {


      ShortAttack(str(0) + " " + str(1) + " " + str(2), str(9), str(12))

    }

    //  Convert a row of Attack object data to an HBase put object
    def convertToPut(attack: ShortAttack, cfAttacker: String): (ImmutableBytesWritable, Put) = {
      val dateTime = attack.timestamp
      // create a composite row key: Attackid_date time
      val rowkey = attack.ip
      val put = new Put(Bytes.toBytes(rowkey))
      // add to column family data, column  data values to put object
      put.add(Bytes.toBytes(cfAttacker), Bytes.toBytes(dateTime), Bytes.toBytes(attack.realuser))
      return (new ImmutableBytesWritable(Bytes.toBytes(rowkey)), put)
    }

  }

}