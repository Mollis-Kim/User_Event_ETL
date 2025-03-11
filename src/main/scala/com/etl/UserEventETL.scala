package com.etl

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import java.util.UUID

object UserEventETL {
  def main(args : Array[String]): Unit = {

    /*
    * Spark 세션 생성.
    * spark.task.maxFailures : 1개의 Task 최대 실패 횟수
    * spark.stage.maxConsecutiveAttempts : 1개의 스테이지내에서 최대 재시도 횟수
    * spark.yarn.maxAppAttempts : 애플리케이션 최대 실패 횟수
    * */
    val spark: SparkSession = SparkSession
      .builder()
      .appName("User Event ETL Spark job")
      .master("yarn")
      .config("spark.task.maxFailures", "3")
      .config("spark.stage.maxConsecutiveAttempts", "3")
      .config("spark.yarn.maxAppAttempts", "3")
      .config("spark.sql.warehouse.dir", "hdfs://master:9000/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    // 5. 배치 장애시 복구를 위한 장치 구현 - 체크포인트
    spark.sparkContext.setCheckpointDir("hdfs://master:9000/tmp/checkpoint")

    // 스키마 설정
    val customSchema = StructType(Array(
      StructField("event_time", TimestampType, true),
      StructField("event_type", StringType, true),
      StructField("product_id", StringType, true),
      StructField("category_id", StringType, true),
      StructField("category_code", StringType, true),
      StructField("brand", StringType, true),
      StructField("price", DoubleType, true),
      StructField("user_id", StringType, true),
      StructField("user_session", StringType, true)
    ))


    // hdfs상에 존재하는 csv파일 읽기
    val df = spark.read
      .option("header", "true")
      .schema(customSchema)
      .csv("hdfs://master:9000/data/*.csv")


    // 1. KST기준 daily partition 처리
    // - from_utc_timestamp와 년-월-일 파티션 구분을 위한 컬럼 생성
    val dfWithKST = df.withColumn("event_time", from_utc_timestamp(col("event_time"),"Asia/Seoul"))
    val dfWithPartition = dfWithKST.withColumn("year", date_format(col("event_time"), "yyyy"))
                            .withColumn("month", date_format(col("event_time"),"MM"))
                            .withColumn("day", date_format(col("event_time"),"dd"))

    // 2. 동일 user_id 내에서 event_time 간격이 5분 이상인 경우 새로운 세션ID 생성
    // - window함수(lag)을 이용하여 이전 시간과 비교
    // - generateSessionUDF UDF함수를 활용해 이전 시간과 5분 이상인 경우 새로운 세션ID생성
    val dfWithTimeLag = dfWithPartition.withColumn("prev_event_time", lag("event_time", 1).over(Window.partitionBy("user_id").orderBy("event_time")))
    val dfWithSessionLag = dfWithTimeLag.withColumn("prev_session_id", lag("user_session", 1).over(Window.partitionBy("user_id").orderBy("event_time")))

    val generateSessionUDF = udf((event_time: java.sql.Timestamp,
                                  prev_event_time: java.sql.Timestamp,
                                  user_session: String,
                                  prev_user_session: String) => {
      if (prev_event_time != null && prev_user_session != null &&
          event_time.getTime - prev_event_time.getTime > 5 * 60 * 1000 && user_session == prev_user_session) {
        UUID.randomUUID().toString // 새로운 세션아이디 생성
      } else {
        user_session  // 기존 세션아이디 유지
      }
    })

    val dfWithNewSession = dfWithSessionLag.withColumn("new_user_session",
                                                        generateSessionUDF(col("event_time"),col("prev_event_time"),col("user_session"),col("prev_session_id")))
    // 5. 배치 장애시 복구를 위한 체크포인트 설정
    val dfWithCheckPoint = dfWithNewSession.drop("prev_event_time", "prev_session_id").checkpoint()

    // 3. parquet, snappy처리 및 파티션 저장 Action
    dfWithCheckPoint.write
      .partitionBy("year","month", "day")
      .format("parquet")
      .option("compression", "SNAPPY")
      .mode(SaveMode.Overwrite)
      .save("hdfs://master:9000/user/hive/warehouse/user_event");

    // Hive에 External 테이블 생성
    spark.sql("""
      CREATE EXTERNAL TABLE IF NOT EXISTS user_event (
        event_time TIMESTAMP,
        event_type STRING,
        product_id STRING,
        category_id STRING,
        category_code STRING,
        brand STRING,
        price DOUBLE,
        user_id STRING,
        user_session STRING,
        new_user_session STRING
      )
      PARTITIONED BY (year STRING, month STRING, day STRING)
      STORED AS PARQUET
      LOCATION 'hdfs://master:9000/user/hive/warehouse/user_event'
    """)

    // MSCK로 메타데이터 반영
    spark.sql("MSCK REPAIR TABLE user_event")

  }
}


