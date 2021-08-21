package FlightAnalytics

import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import java.sql.Date


object Main {
  
  
   	def main(args:Array[String]):Unit={ 

	        val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")
					
          val spark=SparkSession.builder().getOrCreate()					
					import spark.implicits._
	
					val rawDF = spark.read.format("csv").option("Header","true").load(".//data//flightData.csv")
					
				// Question 1 Find the total number of flights for each month.
					val monthDF = rawDF.withColumn("Month", date_format(to_date($"date", "yyyy-MM-dd"), "M"))
					                    .withColumn("Month",col("Month").cast("integer"))
					monthDF.groupBy("Month").count().withColumnRenamed("count", "Number of Flights").sort("Month").show()
					
					
					// Question 2 Find the names of the 100 most frequent flyers.
					val pDF =  rawDF.groupBy("passengerId").count()
					                .sort($"count".desc)
					                .limit(100)
					                .withColumnRenamed("count", "Number of Flights")
					                .withColumnRenamed("passengerId", "Passenger ID")
					               					
					val passengerDF = spark.read.format("csv").option("Header","true").load(".//data//passengers.csv")
					
					passengerDF.join(pDF,passengerDF("passengerId") ===  pDF("Passenger ID"),"inner")
					           .select("Passenger ID","Number of Flights","firstName","lastName")
					           .sort($"Number of Flights".desc)
					           .show(100)					           
					
					//Quesstion 3 Find the greatest number of countries a passenger has been in without being in the UK. 
					val rawFromDF = rawDF.filter(col("from")=!="uk")
					                     .select("passengerId","from")
					                     .withColumnRenamed("passengerId", "Passenger ID")
					                     .withColumnRenamed("from", "Longest Run")
					                      
					
					val rawToDF = rawDF.filter(col("to")=!="uk")
					                     .select("passengerId","to")
					                     .withColumnRenamed("passengerId", "Passenger ID")
					                     .withColumnRenamed("to", "Longest Run")
					                      
					                     
					val longestRunDF = rawFromDF.union(rawToDF).dropDuplicates()
					longestRunDF.groupBy("Passenger ID")
					            .count()					
					            .withColumnRenamed("count", "Longest Run")
					            .show()
					
					            
					            
					// Question 4 Find the passengers who have been on more than 3 flights together.
					
					rawDF.as("rawDF1").join(rawDF.as("rawDF2"),
                      $"rawDF1.passengerId" < $"rawDF2.passengerId" &&
                      $"rawDF1.flightId" === $"rawDF2.flightId" &&
                      $"rawDF1.date" === $"rawDF2.date",
                      "inner"
                    ).
                    groupBy($"rawDF1.passengerId", $"rawDF2.passengerId").
                    agg(count("*").as("flightsTogether"), min($"rawDF1.date").as("from"), max($"rawDF1.date").as("to")).
                    where($"flightsTogether" >= 3).show()

					            
					//Question 5 with parameters
            flownTogether(rawDF,4, "2017-10-04","2017-11-29").show()
            
                    
   	}
   	
   	
   	def flownTogether(rawDF: DataFrame,atLeastNTimes: Int, from: String, to: String) : DataFrame = {
          
          return 	rawDF.as("rawDF1").join(rawDF.as("rawDF2"),
                      col("rawDF1.passengerId") < col("rawDF2.passengerId") &&
                      col("rawDF1.flightId") === col("rawDF2.flightId") &&
                      col("rawDF1.date") === col("rawDF2.date"),
                      "inner"
                    ).
                    groupBy(col("rawDF1.passengerId"), col("rawDF2.passengerId")).
                    agg(count("*").as("flightsTogether"), min(col("rawDF1.date")).as("from"), max(col("rawDF1.date")).as("to")).
                    where(col("flightsTogether") >= atLeastNTimes  
                          && col("from")>= from
                          && col("to")<= to
                    ) 
           
          
    }

   	
   	
   	
}