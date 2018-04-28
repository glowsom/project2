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

object DataAnalysis {
  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder().master("yarn").appName("Music Data Analysis").getOrCreate()

    /*=================================================LOOKUP TABLE SETUP=================================================*/


    //Creating Table properties
    val conf = new Configuration();
    val prop1 = new Properties();
    val uri = args(0);

    prop1.load(FileSystem.get(URI.create(uri), conf).open(new Path(uri)))

    /** 
      * Creating an RDD 'user_artist_subscn' from HBase Table 'user_artist_subscn_map' and out of that RDD,
      * make 3 broadcast variables to find the right values for missing information in the dataframe (subscription timestamps and artist ids)
      */

    val user_artist_Hbase_conf = HBaseConfiguration.create();
    user_artist_Hbase_conf.set("hbase.zookeeper.quorum", "sandbox-hdp.hortonworks.com");
    user_artist_Hbase_conf.set(TableInputFormat.INPUT_TABLE,prop1.getProperty("user_artist_subscn_map"))
    user_artist_Hbase_conf.set("zookeeper.znode.parent", "/hbase-unsecure")

    val user_artist_Hbase = spark.sparkContext.newAPIHadoopRDD(user_artist_Hbase_conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    
    val user_artist_subscn = user_artist_Hbase.mapPartitions(f => f.map(row1 => (
      Bytes.toString(row1._2.getRow),								// user id
      Bytes.toString(row1._2.getValue(Bytes.toBytes("artist"), Bytes.toBytes("id"))),		// artist id
      Bytes.toLong(row1._2.getValue(Bytes.toBytes("subscn"), Bytes.toBytes("start"))),		// subscription start timestamp
      Bytes.toLong(row1._2.getValue(Bytes.toBytes("subscn"), Bytes.toBytes("end"))))))		// subscription end timestamp


    val user_artist_map = spark.sparkContext.broadcast(user_artist_subscn.map(x => (x._1 -> x._2)).collectAsMap)	// Broadcast user id, artist id

    val user_sub_start_map = spark.sparkContext.broadcast(user_artist_subscn.map(x => (x._1 -> x._3)).collectAsMap)	// Broadcast user id, subscription start timestamp

    val user_sub_end_map = spark.sparkContext.broadcast(user_artist_subscn.map(x => (x._1 -> x._4)).collectAsMap)	// Broadcast user id, subscription end timestamp



    /*=====================================================LOAD DATA====================================================*/


    val enrichedDF = spark.read.format("parquet").load(args(6))		//	Creating RDD from DataFilterEnrich.scala output


    /*======================================================PROBLEM 1=====================================================*/

    //1. Determine top 10 station_id(s) where maximum number of songs were played, which were liked by unique users.

    enrichedDF.select(col("station_id"), col("song_id"))		//	First select station and song
      .filter(col("like") === "1")					//	Take the records that have liked songs
      .distinct()							//	Eliminating duplicates will retain unique songs played by each station
      .groupBy(col("station_id"))					//	Group by station, and 
      .count().orderBy(col("count").desc).limit(10)			//	count the occurence of songs for each station, and arrange accordingly to get the top 10

      .write.csv(args(1))


    /*======================================================PROBLEM 2=====================================================*/
    /**
      * 2. Determine total duration of songs played by each type of user, where type of user can be 'subscribed' or
      * 'unsubscribed'. An unsubscribed user is the one whose record is either not present in Subscribed_users lookup
      * table or has subscription_end_date earlier than the timestamp of the song played by him.
      */

    //UDFs to be used

    def type_of_user(user_id:String, timestamp:Long):String = {
      val subscr_end :Long = user_sub_end_map.value.getOrElse(user_id, 0)       //subscr_end_date = 0 if absent in lookUp
      if( timestamp > subscr_end )                                 //If subscr_end = 0 or it's comes before timestamp
        "unsubscribed" else "subscribed"                    //Then user is unsubscribed, Else user is subscribed
    }

    val type_of_user_UDF = udf(type_of_user _)      //Register UDF

    enrichedDF.select( type_of_user_UDF(col("user_id"), col("timestamp")) as "type_of_user",
      ( col("end_ts")-col("start_ts")) as "duration" )				//Select Duration as the start time minus end time of each song per record
      .groupBy("type_of_user").agg(sum("duration") as "total_duration")		//sum up the duration for each type of user
      .write.csv(args(2))
     /*======================================================PROBLEM 3=====================================================*/
    /**
      * 3. Determine top 10 connected artists. Connected artists are those whose songs are most listened by the unique
      * users who follow them.
      */
    def isConnected_artist(user_id:String, artist_id:String):Boolean = {      //Option assumes user_id is present
      user_artist_map.value.getOrElse(user_id, "").contains(artist_id)
      //If user_id is not present in file, that song doesn't count for the artist
    }
    val isConnected_artistUDF = udf(isConnected_artist _)      //Register UDF

    enrichedDF.select("artist_id")
      .filter(isConnected_artistUDF(col("user_id"), col("artist_id")))		//Select only rows with connected artists
      .groupBy("artist_id")							//simple groupby will allow each occurence of uniques artists to be counted
      .count()
      .orderBy(col("count").desc).limit(10)					//take only top 10

      .write.csv(args(3))


    /*======================================================PROBLEM 4=====================================================*/
    /**
      * 4. Determine top 10 songs who have generated the maximum revenue. Royalty applies to a song only if it was liked
      * or was completed successfully or both. Song was completed successfully if song_end_type = 1.
      */
    def royalty(like:Int, song_end_type:Int) :Boolean = {
      if(like == 1  ||  song_end_type == 1)             //If song was liked or song was completed successfully  then it yeilds a royalty so return true
        true else false}
    val royaltyUDF = udf (royalty _)        //Register UDF

    enrichedDF.filter(royaltyUDF(enrichedDF("like"), enrichedDF("song_end_type")))	//Retain only records with royalties
      .select(enrichedDF("song_id"))							//Select only song_id 
      .groupBy("song_id")								//then group them and count # of occurences per unique song
      .count()
      .orderBy(col("count").desc).limit(10)

      .write.csv(args(4))



    /*======================================================PROBLEM 5=====================================================*/
    // 5. Determine top 10 unsubscribed users who listened to the songs for the longest duration.
    //UDF creation
    def isUnsub(user_id:String, timestamp:Long):Boolean = {
      val subscr_end :Long = user_sub_end_map.value.getOrElse(user_id, 0)    //subscr_end_date = 0 if absent in lookUp
      if( timestamp > subscr_end )                     //If subscr_end = 0 or it's comes before timestamp
        true else false                    //Then user is unsubscribed, Else user is subscribed
    }
    val isUnsubUDF = udf(isUnsub _)      //Register UDF

    enrichedDF.filter( isUnsubUDF(col("user_id"), col("timestamp")))		//Retain only records with unsubscribed users
      .select(col("user_id"), (col("end_ts")-col("start_ts")) as "duration" )	//get duration of songs by subtracting start time from end time.
      .groupBy("user_id")							//Group according to user and sum up all durations per user
      .sum("duration")
      .orderBy(col("sum(duration)").desc).limit(10)				//take top 10

      .write.csv(args(5))


  }
}
