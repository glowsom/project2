package music


import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import org.apache.hadoop.fs.{ FileSystem, Path }
import java.net.URI
import java.util.Properties

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes

object DataFilterEnrich{

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Music Data Filter & Enrichment").master("yarn").getOrCreate()
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._
    import spark.sql

    /*=================================================LOOKUP TABLE SETUP=================================================*/


    //Creating Table properties
    val conf = new Configuration();
    val prop1 = new Properties();
    val uri = args(0);
    prop1.load(FileSystem.get(URI.create(uri), conf).open(new Path(uri)))
   
    val station_geo_cd_Hbase_conf = HBaseConfiguration.create();
    station_geo_cd_Hbase_conf.set("hbase.zookeeper.quorum", "sandbox-hdp.hortonworks.com");
    station_geo_cd_Hbase_conf.set(TableInputFormat.INPUT_TABLE,prop1.getProperty("stn_geocd_map"))
    station_geo_cd_Hbase_conf.set("zookeeper.znode.parent", "/hbase-unsecure")

    val station_geo_cd_Hbase = spark.sparkContext.newAPIHadoopRDD(station_geo_cd_Hbase_conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

    // Access LookUp tables in map format
    val station_geo_cd = station_geo_cd_Hbase.mapPartitions(f => f.map(row1 => (
      Bytes.toString(row1._2.getRow),
      Bytes.toString(row1._2.getValue(Bytes.toBytes("geo"), Bytes.toBytes("cd"))))))

    val station_geo_cd_map = spark.sparkContext.broadcast(station_geo_cd.map(x => (x._1 -> x._2)).collectAsMap)

    //Repeat same for song_artist map.
    val song_artist_Hbase_conf = HBaseConfiguration.create();
    song_artist_Hbase_conf.set("hbase.zookeeper.quorum", "sandbox-hdp.hortonworks.com");
    song_artist_Hbase_conf.set(TableInputFormat.INPUT_TABLE,prop1.getProperty("song_artist_map"))
    song_artist_Hbase_conf.set("zookeeper.znode.parent", "/hbase-unsecure")

    val song_artist_Hbase = spark.sparkContext.newAPIHadoopRDD(song_artist_Hbase_conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    
    val song_artist = song_artist_Hbase.mapPartitions(f => f.map(row1 => (
      Bytes.toString(row1._2.getRow),
      Bytes.toString(row1._2.getValue(Bytes.toBytes("artist"), Bytes.toBytes("id"))))))
   
    val song_artist_map = spark.sparkContext.broadcast(song_artist.map(x => (x._1 -> x._2)).collectAsMap)


    /*======================================================LOAD DATA=====================================================*/


    //Load Web data first, then UNION with Mobile data into one Dataframe

    val tempDF: DataFrame = spark.read.format("xml").option("rowTag", "record").load(args(1))

    val tempDF2 = tempDF.select(col("user_id"),
      col("song_id"),
      col("artist_id"),
      unix_timestamp(col("timestamp"), "yyyy/MM/dd") as "timestamp",
      unix_timestamp(col("start_ts"), "yyyy/MM/dd") as "start_ts",
      unix_timestamp(col("end_ts"), "yyyy/MM/dd") as "end_ts",
      col("geo_cd"),
      col("station_id"),
      col("song_end_type"),
      col("like"),
      col("dislike"))

    val tempDF3 = tempDF2.union(spark.read.format("csv").load(args(2)))



    /*===============================================FILTERING OUT BAD RECORDS============================================*/

    /**
      * Filter out bad records which have null in these columns: user_id, song_id, start_ts and end_ts
      */
    val filteredDF = tempDF3.filter( col("user_id").isNotNull
      && col("song_id").isNotNull
      && col("start_ts").isNotNull
      && col("end_ts").isNotNull )

    /*=======================================================UDFs=========================================================*/

    /**
      * Methods to be used as UDFs during Data Enrichment Stage
      */

    def geo(geo_cd:String, station_id:String) :String = {
      Option[String](geo_cd).getOrElse(station_geo_cd_map.value.getOrElse(station_id, "invalid"))
    }

    def artist(artist_id:String, song_id:String):String = {
      Option[String](artist_id).getOrElse(song_artist_map.value.getOrElse(song_id, "invalid"))
    }

    def pref(p:String) :Int = Option[Int](p.toInt).getOrElse(0)



    //Creating UDFs from methods in scala
    val geoUDF = udf (geo _)
    val artistUDF = udf (artist _)
    val prefUDF = udf (pref _)


    /*===================================================DATA ENRICHMENT==================================================*/

    /**
      * Enrich Data with udfs to fill in missing content.
      */

    val enrichedDF = filteredDF.select( col("user_id"),
      col("song_id"),
      artistUDF(col("artist_id"),col("song_id")) as "artist_id",
      col("timestamp").cast(LongType) as "timestamp",
      col("start_ts").cast(LongType) as "start_ts",
      col("end_ts").cast(LongType) as "end_ts",
      geoUDF(col("geo_cd"), col("station_id")) as "geo_cd",
      col("station_id"),
      col("song_end_type").cast(IntegerType) as "song_end_type",
      prefUDF(col("like")) as "like",             //Assign 0 if null
      prefUDF(col("dislike")) as "dislike" )         //Assign 0 if null

      //If artist_id or geo_cd is "invalid" meaning, unavailable in LookUp table,
      .filter( col("artist_id") =!= "invalid" || col("geo_cd") =!= "invalid" )


    enrichedDF.write.format("parquet").save(args(3))    //Saving in parquet for quick access in Data analysis stage.



  }

}
