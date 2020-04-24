spark-shell --packages org.mongodb.spark:mongo-spark-connector_2.11:2.2.1


import com.mongodb.spark._
import com.mongodb.spark.config._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import com.mongodb.spark.rdd.MongoRDD
import com.mongodb.spark.sql._
import org.bson.Document
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import java.sql.Timestamp
import java.time.Duration
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.Column

import org.apache.log4j.Logger
import org.apache.log4j.Level

val base ="mongodb://127.0.0.1/cpmongo."

val replyUri = "CPS_BOARD_REPLY"  //댓글
val codeUri = "CPS_CODE_MNG"  //통합 코드관리 테이블
val gradCorpUri = "CPS_GRADUATE_CORP_INFO"  //졸업 기업
val ncrInfoUri = "CPS_NCR_PROGRAM_INFO"  //비교과 정보
val ncrStdInfoUri = "CPS_NCR_PROGRAM_STD"  //비교과 신청학생
val outActUri = "CPS_OUT_ACTIVITY_MNG"  //교외활동
val jobInfoUri = "CPS_SCHOOL_EMPLOY_INFO"  //채용정보-관리자 등록
val sjobInfoUri = "CPS_SCHOOL_EMPLOY_STD_INFO"  //채용정보-신청학생 정보

val deptInfoUri = "V_STD_CDP_DEPT"  //학과 정보 (department info)
val clPassUri = "V_STD_CDP_PASSCURI" //교과목 수료(class pass)
val stInfoUri = "V_STD_CDP_SREG"  //학생 정보 (student info)
val pfInfoUri = "V_STD_CDP_STAF"  //교수 정보 (professor info)
val clInfoUri = "V_STD_CDP_SUBJECT"  //교과 정보 (class info)

//가져오기 getMongo(spark, 앞서 정의한 Uri)로 사용
def getMongoDF(
     spark : SparkSession,
     coll : String ) : DataFrame = {
     spark.read.mongo(ReadConfig(Map(＂uri＂->(base+coll))))
}

//저장하기 setMongo(spark, Uri, dataframe)으로 사용
def setMongoDF(
     spark : SparkSession,
     coll: String,
     df : DataFrame ) = {
     df.saveToMongoDB(WriteConfig(Map("uri"->(base+coll))))
}

val test2 = getMongoDF(spark, pfInfoUri)
test2.show
