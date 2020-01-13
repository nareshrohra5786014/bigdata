package com.cloudxlab.logparsing

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

case class LogParser( hostName: String, timeStamp: String, url: String, HTTPcodes: Int)

class Utils extends Serializable {
    val PATTERN = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)(.*)" (\d{3}) (\S+)""".r
    
    def parseLogLine(log: String): LogParser =
    {
        val line = PATTERN.findFirstMatchIn(log) 
        if (line.isEmpty)
        {   println("Empty Log Line: " + log)
            LogParser("Empty", "", "",  -1 )
        }
        else{
            val attributes = line.get
            LogParser(attributes.group(1), attributes.group(4), attributes.group(6), attributes.group(8).toInt)
        }

    }

    def getLogFile(accessLogs:RDD[String], spark: SparkSession): Unit =
    {
        import spark.implicits._
        val accessLogDF = accessLogs.map(parseLogLine).toDF()
        accessLogDF.printSchema
	accessLogDF.createOrReplaceTempView("nasalog")
	val output = spark.sql("select * from nasalog")
	output.createOrReplaceTempView("nasa_log")
	//spark.sql("cache TABLE nasa_log")

	spark.sql("select url,count(*) as req_cnt from nasa_log where upper(url) like '%HTML%' group by url order by req_cnt desc LIMIT 10").show

	spark.sql("select hostName,count(*) as req_cnt from nasa_log group by hostName order by req_cnt desc LIMIT 5").show

	spark.sql("select substr(timeStamp,1,14) as timeFrame,count(*) as req_cnt from nasa_log group by substr(timeStamp,1,14) order by req_cnt desc LIMIT 5").show

	spark.sql("select substr(timeStamp,1,14) as timeFrame,count(*) as req_cnt from nasa_log group by substr(timeStamp,1,14) order by req_cnt  LIMIT 5").show

	spark.sql("select HTTPcodes,count(*) as req_cnt from nasa_log group by HTTPcodes").show


    }

 
 }

object EntryPoint {
    val usage = """
        Usage: EntryPoint <file_or_directory_in_hdfs>
        Eample: EntryPoint  /data/spark/project/NASA_access_log_Aug95.gz
    """
    
    def main(args: Array[String]) {
        
        if (args.length != 2) {
            println("Expected:2 , Provided: " + args.length)
            println(usage)
            return;
        }

        var utils = new Utils
	    val spark = SparkSession
                    .builder()
                    .appName("NASA Kennedy Space Center Log Parsing")
                    .getOrCreate()    

        spark.sparkContext.setLogLevel("WARN")
	

        // var accessLogs = sc.textFile("/data/spark/project/access/access.log.45.gz")
        var accessLogs = spark.sparkContext.textFile(args(1))
        utils.getLogFile(accessLogs, spark)
        
    }
}
