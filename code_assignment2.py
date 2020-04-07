import traceback
import socket
import math
from pyspark import SparkContext
from pyspark import SparkContext
from pyspark.sql.types import StructType
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql import functions
from pyspark.sql import types as T
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import datetime
from datetime import datetime

# Lazily instantiated global instance of SparkSession
def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]

def create_row_df(line):
    """
        Function that creates a Structured Row object representing a Row in a DataFrame
        Params:
            line - A line from the input file
        Returns:
            A Row object representing a row in a Dataframe
    """
    #Field - Array_position

    #pickup_dt - 0      fare_amount - 8
    #dropoff_dt - 1     tip_amount - 9
    #trip_time - 2      total_amount - 10
    #trip_distance - 3  pickup_cell - 11
    #pickup_long - 4    dropoff_cell - 12
    #pickup_lat - 5     medallion = 13
    #dropoff_long - 6
    #dropoff_lat - 7
    splitted_line = line.split(',')
    return Row(
        pickup_dt = splitted_line[2], dropoff_dt = splitted_line[3], trip_time = int(splitted_line[4]), \
        trip_distance = float(splitted_line[5]), pickup_long = float(splitted_line[6]), pickup_lat = float(splitted_line[7]), \
        dropoff_long = float(splitted_line[8]), dropoff_lat = float(splitted_line[9]), fare_amount = float(splitted_line[11]), \
        tip_amount = float(splitted_line[14]), total_amount = float(splitted_line[16]), pickup_cell = estimate_cellid(float(splitted_line[7]), float(splitted_line[6])), \
        dropoff_cell = estimate_cellid(float(splitted_line[9]), float(splitted_line[8])), medallion = splitted_line[0]
        )


def filter_lines(line):
    """
        Function that filters out empty lines as well as lines that have coordinates as 0.0000 (non relevant points)
        Params:
            line - A line from the input file
        Returns:
            True if the line passed this condition, False otherwise
    """
    splitted_line = line.split(',')

    #Limits of the grid. Every point that is not between these coordinates will be considered as an outlier
    lon_min = -74.916578
    lon_max = -73.120784
    lat_min = 40.129716
    lat_max = 41.477183

    return (
        len(line) > 0) and \
        (float(splitted_line[6]) != 0) and \
        (float(splitted_line[8]) != 0 and \
        (float(splitted_line[6]) >= lon_min) and \
        (float(splitted_line[6]) <= lon_max) and \
        (float(splitted_line[7]) >= lat_min) and \
        (float(splitted_line[7]) <= lat_max) and \
        (float(splitted_line[8]) >= lon_min) and \
        (float(splitted_line[8]) <= lon_max) and \
        (float(splitted_line[9]) >= lat_min) and \
        (float(splitted_line[9]) <= lat_max)
        )


def create_row(line):
    """
        Function that creates a structured tuple representing a row in a RDD
        Params:
            line - A line from the input file
        Rerturns:
            A Structured tuple with 14 positions
    """
    #Field - Array_position

    #pickup_dt - 0      fare_amount - 8
    #dropoff_dt - 1     tip_amount - 9
    #trip_time - 2      total_amount - 10
    #trip_distance - 3  pickup_cell - 11
    #pickup_long - 4    dropoff_cell - 12
    #pickup_lat - 5     medallion = 13
    #dropoff_long - 6
    #dropoff_lat - 7
    
    splitted_line = line.split(',')
    return (
        splitted_line[2], splitted_line[3], int(splitted_line[4]), float(splitted_line[5]), float(splitted_line[6]), \
        float(splitted_line[7]), float(splitted_line[8]), float(splitted_line[9]), float(splitted_line[11]), \
        float(splitted_line[14]), float(splitted_line[16]), estimate_cellid(float(splitted_line[7]), float(splitted_line[6])),\
        estimate_cellid(float(splitted_line[9]), float(splitted_line[8])), splitted_line[0]
    )

def estimate_cellid(lat, lon):
    """
        Function that estimates a cell ID given a latitude and longitude based on the coordinates of cell 1.1
        Params:
            lat - Input latitude for which to find the cellID
            lon - Input longitude for which to fin the cellID
        Returns:
            A String such as 'xxx.xxx' representing the ID of the cell
    """
    x0 = -74.913585 #longitude of cell 1.1
    y0 = 41.474937  #latitude of cell 1.1
    s = 500 #500 meters

    delta_x = 0.005986 / 500.0  #Change in longitude coordinates per meter
    delta_y = 0.004491556 /500.0    #Change in latitude coordinates per meter

    cell_x = 1 + math.floor((1/2) + (lon - x0)/(s * delta_x))
    cell_y = 1 + math.floor((1/2) + (y0 - lat)/(s * delta_y))
    
    return f"{cell_x}.{cell_y}"

def process(time, rdd, query_name):
    print("========= %s =========" % str(time))
    try:
        # Get the singleton instance of SparkSession
        spark = getSparkSessionInstance(rdd.context.getConf())
        
        # Convert RDD[String] to RDD[Row] to DataFrame
        rowRdd = rdd.map(lambda line: create_row_df(line))
        #rowRdd = rdd.map(lambda line: Row(word=line))
        df = spark.createDataFrame(rowRdd)

        # This if block switches between the different queries
        if query_name == "Query1":

            # Obtain the unsorted list of route frequencies by
            # grouping by a route (a pickup_cell and a dropoff_cell) and a window of 30 minutes
            # then aggregate the values into a count (in this case counting fare amount for no particular reason)
            route_freqs = df\
                        .groupBy(df.pickup_cell,\
                                 df.dropoff_cell,\
                                 window(df.pickup_dt, '30 minutes'))\
                        .agg(count(df.fare_amount).alias("NumTrips"))

            # Then select only the desired columns and sort descendingly the NumTrips variable and limit it to 10 values
            # to get the 10 most frequent routes
            most_freq_routes = route_freqs\
                        .select(route_freqs.pickup_cell, route_freqs.dropoff_cell, route_freqs.NumTrips)\
                        .sort(desc("NumTrips"))\
                        .limit(10)

            most_freq_routes.show()
            
        elif query_name == "Query2":

            # Get the number of empty taxis in an area by obtaining the number of taxis that
            # had a dropoff in a given cell in the last 30 minutes
            empty_taxis = df\
                        .groupBy(df.dropoff_cell,\
                                 window(df.dropoff_dt, "30 minutes"))\
                        .count().alias("NumEmptyTaxis")

            # Get the total profit of an area which is the average
            # of fare amount plus tip for a given area in the last 15 minutes
            profit = df\
                    .groupBy(df.pickup_cell,\
                             window(df.pickup_dt, "15 minutes"))\
                    .agg(avg(df.fare_amount + df.tip_amount).alias("AreaProfit"))
            
            # Join the two streams together on the commun column (the cell of pickup or dropoff)
            joined_dfs = empty_taxis.join(profit, empty_taxis.dropoff_cell == profit.pickup_cell)
            
            # Creating a temporary view of this stream naming it 'joined_dfs'
            joined_dfs.createOrReplaceTempView('joined_dfs')

            # This SQL expression selects the a given cell and for that cell
            # divides the total area profit by the number of empty taxis to obtain the profitability
            sql_query = """
            SELECT pickup_cell, AreaProfit/count AS Profitability
            FROM joined_dfs
            WHERE count > 0
            ORDER BY Profitability DESC
            """
            profitability = spark.sql(sql_query)
            
            profitability.show()
            
        elif query_name == "Query3":
            # Unfinished, completed with regular spark RDDs
            dropoffs_aux = df\
                    .groupBy(df.medallion,\
                             df.dropoff_cell,\
                             window(df.dropoff_dt, "1 hours"))\
                    .agg(first(df.dropoff_dt).alias("ddt"))

            dropoffs = dropoffs_aux.select(dropoffs_aux.medallion,\
                                           dropoffs_aux.dropoff_cell,\
                                           dropoffs_aux.ddt)
            
            
            pickups_aux = df\
                    .groupBy(df.medallion,\
                             df.pickup_cell,\
                             window(df.pickup_dt, "1 hours"))\
                    .agg(first(df.pickup_dt).alias("pdt"))
            
            pickups = pickups_aux.select(pickups_aux.medallion,\
                                           pickups_aux.pickup_cell,\
                                           pickups_aux.pdt)
            
            conditions = [dropoffs.medallion == pickups.medallion]
            joined_dfs = dropoffs.join(pickups, conditions)
            
            joined_dfs_correct = joined_dfs.where(joined_dfs.dropoff_cell == joined_dfs.pickup_cell)
            joined_dfs_correct.show()
            
        elif query_name == "Query4":

            def find_peak(durations, cells):
                """
                    Aggregation function that receives a list of durations and cells
                    Searches in the list of durations for a pattern where a given duration is higher
                    than the one before and after it, and the duration after it is followed by 3 rides of increasing duration
                """
                if(len(durations) == 0):
                    return "No Congested Areas, and no durations"
                
                for idx, val in enumerate(durations):
                    try:
                        if idx != 0:
                            if val > durations[idx - 1] and val > durations[idx + 1] and  durations[idx + 1] <  durations[idx + 2] and durations[idx + 2] <  durations[idx + 3]:
                                return f"Alert, area {cells[idx]} is congested"
                            else:
                                return "No Congested Areas"
                        else:
                            return "No congested Areas"
                    except:
                        return "No Congested Areas"
             
            
            # Registering the user defined function
            find_peak_udf = functions.udf(find_peak, T.StringType())
            
            # Obtain all rides for a taxi in a day and aggregate the values with the custom aggregation function
            # described above
            grouped = df.groupBy(df.medallion, window(df.pickup_dt, "1 days")).agg(find_peak_udf(collect_list(df.trip_time), collect_list(df.pickup_cell)).alias("alert_message"))
            
            grouped.select(grouped.alert_message).show()
            
        elif query_name == "Query5":

            # Obtain an unsorted list of pleasant taxis by obtaining a list
            # of taxis followed by the sum of the tips they obtained in a day
            most_pleasant_unsorted = df\
                                .groupBy(df.medallion,\
                                         window(df.pickup_dt, "1 days"))\
                                .agg(functions.sum(df.tip_amount).alias("TotalTipAmount"))
            
            # Sort this previous value descendingly and limit the stream to just 10 outputs
            # to obtain the 10 most pleasant taxi drivers
            most_pleasant = most_pleasant_unsorted\
                        .select(most_pleasant_unsorted.medallion, most_pleasant_unsorted.TotalTipAmount)\
                        .sort(desc("TotalTipAmount"))\
                        .limit(10)
            
            most_pleasant.show()
            
    except:
        traceback.print_exc()

def minutes_between(d1, d2):
    d1 = datetime.strptime(d1, '%Y-%m-%d %H:%M:%S')
    d2 = datetime.strptime(d2, '%Y-%m-%d %H:%M:%S')
    return int((d2 - d1).seconds // 60 % 60)

################## FIRST QUERY

try:
    # Creating the spark context and streaming context objects
    sc = SparkContext("local[2]", "KafkaExample")
    ssc = StreamingContext(sc, 5)
    
    # Establsihing the Kafka conectoin subscribing to topic "debs" and listening on port 9092
    lines = KafkaUtils.createDirectStream(ssc, ["debs"], \
                {"metadata.broker.list": "kafka:9092"})

    # Kafka sends a timestamp followed by the actual value we're interested in
    # so we just keep the second value in the tuple
    lines = lines.map(lambda tup: tup[1])

    # Filtering the lines according to our custom filter function
    filtered_lines = lines.filter(lambda line: filter_lines(line))

    ############### Query 1 ###############
    # For each mini-batch RDD aplly the process function specifying the Query1
    filtered_lines.foreachRDD(lambda time, rdd: process(time, rdd, "Query1"))

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
    sc.stop()
except:
    traceback.print_exc()
    ssc.stop()
    sc.stop()


################## SECOND QUERY

try:
    # Creating the spark context and streaming context objects
    sc = SparkContext("local[2]", "KafkaExample")
    ssc = StreamingContext(sc, 5)
    
    # Establsihing the Kafka conectoin subscribing to topic "debs" and listening on port 9092
    lines = KafkaUtils.createDirectStream(ssc, ["debs"], \
                {"metadata.broker.list": "kafka:9092"})

    # Kafka sends a timestamp followed by the actual value we're interested in
    # so we just keep the second value in the tuple
    lines = lines.map(lambda tup: tup[1])

    # Filtering the lines according to our custom filter function
    filtered_lines = lines.filter(lambda line: filter_lines(line))

    ################ Query 2 ################
    # For each mini-batch RDD aplly the process function specifying the Query2
    filtered_lines.foreachRDD(lambda time, rdd: process(time, rdd, "Query2"))

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
    sc.stop()
except:
    traceback.print_exc()
    ssc.stop()
    sc.stop()


################## THIRD QUERY


try:
    # Creating the spark context and streaming context objects
    sc = SparkContext("local[2]", "KafkaExample")
    ssc = StreamingContext(sc, 5)
    
    # Establsihing the Kafka conectoin subscribing to topic "debs" and listening on port 9092
    lines = KafkaUtils.createDirectStream(ssc, ["debs"], \
                {"metadata.broker.list": "kafka:9092"})

    # Kafka sends a timestamp followed by the actual value we're interested in
    # so we just keep the second value in the tuple
    lines = lines.map(lambda tup: tup[1])

    # Filtering the lines according to our custom filter function
    filtered_lines = lines.filter(lambda line: filter_lines(line))
    
    ############### Query 3 ################
    #filtered_lines.foreachRDD(lambda time, rdd: process(time, rdd, "Query3"))
    
    structured_lines = filtered_lines.map(lambda line: create_row(line))

    # Creating a Window of 1 hour (60 seconds * 60)
    # Then creating a (medallion, (pickup_dt, dropoff_dt, 1)) key-value pair
    avg_idle_times = structured_lines \
                        .window(60 * 60) \
                        .map(lambda tup: (tup[13], (tup[0], tup[1], 1)))

    # Calculating the minutes between the pickup_dt and dropoff_dt
    # Then filtering the negative values (occurs when pick_dt is before dropoff_dt)
    avg_idle_times = avg_idle_times \
                        .mapValues(lambda tup: (minutes_between(tup[1], tup[0]), 1)) \
                        .filter(lambda tup: int(tup[1][0] > 0))

    # Summing the idle times to then make the average for each medallion
    # Then calculating the average idle time for each medallion
    avg_idle_times = avg_idle_times \
                        .reduceByKey(lambda acc, elem: (acc[0] + elem[0], acc[1] + elem[1])) \
                        .mapValues(lambda tup: tup[0] / tup[1])

    # Retaining only the values that have an average idle team of over 10 minutes
    # Then emmiting an alert message with the idle time of that medallion
    avg_idle_times = avg_idle_times \
                        .filter(lambda tup: tup[1] > 10) \
                        .mapValues(lambda idle_time: f"Idle time alert: {idle_time} minutes idle")

    avg_idle_times.pprint()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
    sc.stop()
except:
    traceback.print_exc()
    ssc.stop()
    sc.stop()



################## FOURTH QUERY


try:
    # Creating the spark context and streaming context objects
    sc = SparkContext("local[2]", "KafkaExample")
    ssc = StreamingContext(sc, 5)
    
    # Establsihing the Kafka conectoin subscribing to topic "debs" and listening on port 9092
    lines = KafkaUtils.createDirectStream(ssc, ["debs"], \
                {"metadata.broker.list": "kafka:9092"})

    # Kafka sends a timestamp followed by the actual value we're interested in
    # so we just keep the second value in the tuple
    lines = lines.map(lambda tup: tup[1])

    # Filtering the lines according to our custom filter function
    filtered_lines = lines.filter(lambda line: filter_lines(line))

    ################ Query 4 ################
    # For each mini-batch RDD aplly the process function specifying the Query4
    filtered_lines.foreachRDD(lambda time, rdd: process(time, rdd, "Query4"))

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
    sc.stop()
except:
    traceback.print_exc()
    ssc.stop()
    sc.stop()



################## FIFTH QUERY


try:
    # Creating the spark context and streaming context objects
    sc = SparkContext("local[2]", "KafkaExample")
    ssc = StreamingContext(sc, 5)
    
    # Establsihing the Kafka conectoin subscribing to topic "debs" and listening on port 9092
    lines = KafkaUtils.createDirectStream(ssc, ["debs"], \
                {"metadata.broker.list": "kafka:9092"})

    # Kafka sends a timestamp followed by the actual value we're interested in
    # so we just keep the second value in the tuple
    lines = lines.map(lambda tup: tup[1])

    # Filtering the lines according to our custom filter function
    filtered_lines = lines.filter(lambda line: filter_lines(line))

    ################ Query 5 ################
    # For each mini-batch RDD aplly the process function specifying the Query5
    filtered_lines.foreachRDD(lambda time, rdd: process(time, rdd, "Query5"))

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
    sc.stop()
except:
    traceback.print_exc()
    ssc.stop()
    sc.stop()