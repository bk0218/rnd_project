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
import scala.collection.mutable

val base2 ="mongodb://127.0.0.1/cpmongo."
val base ="mongodb://127.0.0.1/cpmongo_distinct."
val output_base = "mongodb://127.0.0.1/cpmongo_distinct.USER_SIMILARITY"
val USER_LIST_FOR_SIM_output_base = "mongodb://127.0.0.1/cpmongo_distinct.USER_LIST_FOR_SIMILARITY"
val USER_SIM_output_base = "mongodb://127.0.0.1/cpmongo_distinct.USER_SIMILARITY"
val SREG_output_base = "mongodb://127.0.0.1/cpmongo_distinct.SREG_SIM"
val NCR_output_base = "mongodb://127.0.0.1/cpmongo_distinct.NCR_SIM"
val ACT_output_base = "mongodb://127.0.0.1/cpmongo_distinct.ACTIVITY_SIM"
//교과: SREG_SIM, 비교과: NCR_SIM, 자율활동: ACTIVITY_SIM
val Result_output_base = "mongodb://127.0.0.1/cpmongo_distinct.Recommend_Result"

val comRatingUri = "CPS_RATING"
val sjobSrhUri = "CPS_EMPLOY_SEARCH_HIS"  
val replyUri = "CPS_BOARD_REPLY"  //댓글
val codeUri = "CPS_CODE_MNG"  //통합 코드관리 테이블
val gradCorpUri = "CPS_GRADUATE_CORP_INFO"  //졸업 기업 // <<관심기업팀>>
val ncrInfoUri = "CPS_NCR_PROGRAM_INFO"  //비교과 정보
val ncrStdInfoUri = "CPS_NCR_PROGRAM_STD"  //비교과 신청학생
val outActUri = "CPS_OUT_ACTIVITY_MNG"  //교외활동
val jobInfoUri = "CPS_SCHOOL_EMPLOY_INFO"  //채용정보-관리자 등록 // <<관심기업팀>>
val sjobInfoUri = "CPS_SCHOOL_EMPLOY_STD_INFO"  //채용정보 신청 학생 정보(student job info) // <<관심기업팀>>
val deptInfoUri = "V_STD_CDP_DEPT"  //학과 정보 (department info)
val clPassUri = "V_STD_CDP_PASSCURI" //교과목 수료(class pass)
val stInfoUri = "V_STD_CDP_SREG"  //학생 정보 (student info) // <<관심기업팀>>
val pfInfoUri = "V_STD_CDP_STAF"  //교수 정보 (professor info)
val clInfoUri = "V_STD_CDP_SUBJECT"  //교과 정보 (class info)

val cpsStarUri = "CPS_STAR_POINT"  //교과/비교과용 별점 테이블
val userforSimilarityUri = "USER_LIST_FOR_SIMILARITY" //유사도 분석 팀이 생성한 테이블

Logger.getLogger("org").setLevel(Level.OFF)
Logger.getLogger("akka").setLevel(Level.OFF)
Logger.getLogger("MongoRelation").setLevel(Level.OFF)
Logger.getLogger("MongoClientCache").setLevel(Level.OFF)
Logger.getRootLogger().setLevel(Level.ERROR)


def getMongoDF(
                spark : SparkSession,
                coll : String ) : DataFrame = {
  spark.read.mongo(ReadConfig(Map("uri"->(base+coll))))
}

def getMongoDF2(
                 spark : SparkSession,
                 coll : String ) : DataFrame = {
  spark.read.mongo(ReadConfig(Map("uri"->(base2+coll))))
}

//저장하기 setMongo(spark, Uri, dataframe)으로 사용
def setMongoDF(
                spark : SparkSession,
                coll: String,
                df : DataFrame ) = {
  df.saveToMongoDB(WriteConfig(Map("uri"->(base+coll))))
}



//추천결과팀 데이터 저장
def setMongoDF_result(
                       spark : SparkSession,
                       df : DataFrame ) = {
  df.saveToMongoDB(WriteConfig(Map("uri"->(Result_output_base))))
}


def setMongoDF_USER_LIST(
spark : SparkSession,
df : DataFrame ) = {
df.saveToMongoDB(WriteConfig(Map("uri"->(USER_LIST_FOR_SIM_output_base))))
}

def setMongoDF_USER_SIM(
spark : SparkSession,
df : DataFrame ) = {
df.saveToMongoDB(WriteConfig(Map("uri"->(USER_SIM_output_base))))
}





val replyUri_table = getMongoDF(spark, replyUri)  //댓글
val codeUri_table =  getMongoDF(spark, codeUri)  //통합 코드관리 테이블
val gradCorpUri_table =  getMongoDF(spark, gradCorpUri)  //졸업 기업
val ncrInfoUri_table =  getMongoDF(spark, ncrInfoUri)  //비교과 정보
val ncrStdInfoUri_table =  getMongoDF(spark, ncrStdInfoUri)  //비교과 신청학생
val outActUri_table =  getMongoDF(spark, outActUri)  //교외활동
val jobInfoUri_table =  getMongoDF(spark, jobInfoUri)  //채용정보-관리자 등록
val sjobInfoUri_table =  getMongoDF(spark, sjobInfoUri)  //채용정보 신청 학생 정보(student job info)


val deptInfoUri_table =  getMongoDF(spark, deptInfoUri)  //학과 정보 (department info)
val clPassUri_table =  getMongoDF(spark, clPassUri) //교과목 수료(class pass)
val stInfoUri_table =  getMongoDF(spark, stInfoUri)  //학생 정보 (student info)
val pfInfoUri_table =  getMongoDF(spark, pfInfoUri)  //교수 정보 (professor info)
val clInfoUri_table =  getMongoDF(spark, clInfoUri)  //교과 정보 (class info)

val cpsStarUri_table = getMongoDF(spark, cpsStarUri)  //교과/비교과용 별점 테이블
val userforSimilarity_table = getMongoDF(spark, userforSimilarityUri) //유사도 분석 팀이 생성한 테이블



//============================관심기업(수빈,영호) start =======================================

//applyComlist 나오는 학번
// var std_no = 201926086

//searchComlist 나오는 학번
var std_no = 201226070

//gStdComList는 원래 안나옴



//유사도팀에서 사용하는 학번 : 20142820, 20142932, 20152611
var std_no = 20142820




/*학번을 이용하여 학생이 신청한 채용정보의 회사 리스트 생성(최신신청순)*/
def mkjobComList (spark:SparkSession, stdNo: Int) : DataFrame = {
	//학생-채용 신청 정보 테이블
	val getsjobInfo = getMongoDF(spark, sjobInfoUri)
	val sjob = getsjobInfo.select('SCE_INFO_KEY_ID, 'REG_DATE, 'SCE_STD_NO, 'SCE_STD_APPLY_YN).distinct()
	val sjobFilter = sjob.filter(sjob("SCE_STD_APPLY_YN") === "Y")
	val std_sjobList= sjobFilter.filter(sjobFilter("SCE_STD_NO") === stdNo)
	//채용정보 테이블
	val getjobInfo = getMongoDF(spark, jobInfoUri)
	val jobInfo = getjobInfo.select('SCE_KEY_ID.as("SCE_INFO_KEY_ID"), 'SCE_COMP_TITLE, 'SCE_SECTORS, 'SCE_RELATED_DEPTS).distinct().orderBy("SCE_KEY_ID")
	//학생-채용신청-회사 정보 리스트 생성	//REG_DATE기준으로 최신순으로 정렬
	val jobComList = std_sjobList.join(jobInfo, Seq("SCE_INFO_KEY_ID"), "inner").orderBy("REG_DATE")
	val comList = jobComList.select('SCE_COMP_TITLE).withColumnRenamed("SCE_COMP_TITLE", "ComList")
	comList
}


/*학생의 채용정보 조회 이력을 통한 회사 리스트(최신검색순)*/
//201226070
def mkjobSearch (spark:SparkSession, stdNo: Int) : DataFrame = {
	//채용정보 조회 이력 테이블
	val getsjobSrh = getMongoDF(spark, sjobSrhUri)
	val std_srhList = getsjobSrh.select('SEARCH_PARAM, 'REG_ID, 'REG_DATE, 'CORP_NM).distinct().orderBy(desc("REG_DATE")).filter('REG_ID === stdNo)
	val srhList = std_srhList.select('CORP_NM).withColumnRenamed("CORP_NM", "ComList")
	srhList
}



/*학생의 학과, 성별과 일치하는 기업 리스트 생성*/
//201028411 세무회계학과 여
def mkDepGenComList (spark:SparkSession, stdNo : Int) : DataFrame = {
	val getGstdInfo = getMongoDF(spark, gradCorpUri)
	val getStdInfo = getMongoDF(spark, stInfoUri)
	//std_no를 이용하여 getStdInfo 에서 일치하는 학생 정보(학번, 이름, 학과명, 성별, 졸업여부)만 추출
  val stdInfo = getStdInfo.select('STD_NO, 'KOR_NM, 'SUST_CD_NM, 'GEN_FG_NM, 'GRADT_YN).filter('STD_NO === stdNo)
	//질의 해당 학생의 성별, 학과
	var stdGen = stdInfo.select('GEN_FG_NM).first.getString(0)
	if (stdGen == "남성")
		stdGen = "남"
	else
		stdGen = "여"
	val stdDep = stdInfo.select('SUST_CD_NM).first.getString(0)
	//select를 통해 졸업자 취업리스트(학번, 학과, 성별, 기업명 리스트) 생성
	val gStdInfo = getGstdInfo.select('GCI_STD_NO, 'GCI_SUST_CD_NM, 'GCI_GEN_FG_NM, 'GCI_CORP_KEY_ID, 'GCI_CORP_NM).distinct()
	//성별, 학과 모두 일치하는 기업리스트 생성
	val gStdInfoList = gStdInfo.filter('GCI_GEN_FG_NM === stdGen).filter('GCI_SUST_CD_NM === stdDep)
	val gStdComList = gStdInfoList.select('GCI_CORP_NM).withColumnRenamed("GCI_CORP_NM", "ComList")
	gStdComList
}


val applyComlist = mkjobComList(spark, std_no)
val searchComlist = mkjobSearch(spark, std_no)
val gStdComList = mkDepGenComList(spark, std_no)

//============================관심기업(수빈,영호)end=======================================




//============================콘텐츠신뢰도(재용)start=======================================

val corps_applyComlist = applyComlist.collect.map(_.toSeq).flatten
val corps_searchComlist = searchComlist.collect.map(_.toSeq).flatten
val corps_gStdComList = gStdComList.collect.map(_.toSeq).flatten

val getGstdInfo = getMongoDF(spark, gradCorpUri) //test
val getGstdInfo_ = getGstdInfo.select("*") //rdd2

def categ(corps_applyComlist: Array[Any]):scala.collection.mutable.Map[Any, Array[org.apache.spark.sql.Row]]={
     var a = scala.collection.mutable.Map[Any, Array[org.apache.spark.sql.Row]]()
     for(i<-0 until corps_applyComlist.size){
     a = a+(corps_applyComlist(i)->getGstdInfo_.filter(getGstdInfo_("GCI_CORP_NM")===corps_applyComlist(i)).collect)
     }
     a}

def categ1(corps_searchComlist: Array[Any]):scala.collection.mutable.Map[Any, Array[org.apache.spark.sql.Row]]={
     var a = scala.collection.mutable.Map[Any, Array[org.apache.spark.sql.Row]]()
     for(i<-0 until corps_searchComlist.size){
     a = a+(corps_searchComlist(i)->getGstdInfo_.filter(getGstdInfo_("GCI_CORP_NM")===corps_searchComlist(i)).collect)
     }
     a}

def categ2(corps_gStdComList: Array[Any]):scala.collection.mutable.Map[Any, Array[org.apache.spark.sql.Row]]={
     var a = scala.collection.mutable.Map[Any, Array[org.apache.spark.sql.Row]]()
     for(i<-0 until corps_gStdComList.size){
     a = a+(corps_gStdComList(i)->getGstdInfo_.filter(getGstdInfo_("GCI_CORP_NM")===corps_gStdComList(i)).collect)
     }
     a}

val corps1 = categ(corps_applyComlist)
val corps2 = categ1(corps_searchComlist)
val corps3 = categ2(corps_gStdComList)

corps1.size
corps2.size
corps3.size

var final_corps = scala.collection.mutable.Map[Any, Array[org.apache.spark.sql.Row]]()

if(corps1.size >= 10){
	final_corps = corps1
} else if(corps1.size + corps2.size >= 10){
	final_corps = corps1 ++ corps2
	} else{
	final_corps = corps1 ++ corps2 ++ corps3
	}

val test2 = getMongoDF2(spark, "CPS_RATING")
var tuples = Seq[(Int, Double)]()
val t5 = getGstdInfo.select(col("GCI_STD_NO"), col("GCI_CORP_NM")).distinct.toDF

var final_corps_keys = final_corps.keys.toList

//---------------------사용자 신뢰도(재용)---------------------------


for(i<-0 until final_corps.size){
  val a = 0.2
  val b = 0.125
  val w1 = 0.5
  val w2 = 0.5
  val columns = Seq("GCI_STD_NO", "RATING")
  var df = test2.filter(test2("기업명")===final_corps_keys(i)).toDF
  var filter = t5.filter(t5("GCI_CORP_NM").equalTo(final_corps_keys(i))).collect
  if(df.collect.size>0){
    var add = df.select("기업가중치").as[String].collect()(0).toInt*w1*a+df.select("직원수 가중치").as[String].collect()(0).toInt*w2*b
    for(j<-0 until filter.size){
      tuples = tuples :+ (filter(j)(0).toString.toInt, add.toString.toDouble)
      }
    }else{
      for(j<-0 until filter.size){
        tuples = tuples :+ (filter(j)(0).toString.toInt, 0.0)
    }
  }
}

val df = tuples.toDF("GCI_STD_NO", "TRUST")

val test0 = getMongoDF(spark, "CPS_STAR_POINT")
var con = test0.select(col("STAR_KEY_ID"), col("STAR_POINT"))
var con1 = con.groupBy("STAR_KEY_ID").agg(avg("STAR_POINT").alias("STAR_POINT"))

//코드 다 실행하고 결과 출력해보는 거
df.show()
con1.show() //중분류에 대한 신뢰도

//활동 점수 계산
//교과 + 비교과 + 자율활동

val stInfoUri_ = getMongoDF2(spark, stInfoUri)
var st_list = stInfoUri_.limit(5).select("STD_NO").rdd.map(r=>r(0)).collect()

val gradCorpUri_ = getMongoDF2(spark, gradCorpUri)
val clPassUri_ = getMongoDF2(spark, clPassUri)
val ncrStdInfoUri_ = getMongoDF2(spark, ncrStdInfoUri)
val outActUri_ = getMongoDF2(spark, outActUri)
val starpoint_ = getMongoDF2(spark, "CPS_STAR_POINT")
var out_act_score = Seq[(Int, Double)]()

//아래 for문 시간 좀 걸림.
for(i<-0 until st_list.size){ //학생들의 활동 점수 계산을 위해 영역 별로 count를 수행하여 DF로 만들 것
  var sbjt = clPassUri_.filter(clPassUri_("STD_NO")===st_list(i)).toDF
  var ncr = ncrStdInfoUri_.filter(ncrStdInfoUri_("NPS_STD_NO")===st_list(i)).toDF
  var out = outActUri_.filter(outActUri_("OAM_STD_NO")===st_list(i)).toDF

  var stars = starpoint_.filter(starpoint_("STD_NO")===st_list(i)).count //st_list(i) 학생이 교과/비교과에 준 총 별점의 개수

  var total = sbjt.count+ncr.count+out.count //학생이 총 수강/활동한 (교과/비교과)/대외활동의 수
  var act_score = 0

  if(total==0){
    act_score = stars.toInt / (total+1).toInt //활동 점수 계산
  }else{
    act_score = stars.toInt / total.toInt //활동 점수 계산
  }

  out_act_score = out_act_score :+ (st_list(i).toString.toInt, act_score.toString.toDouble)

}

var out_act_score_df = out_act_score.toDF("STD_NO", "ACT_SCORE")

//st_list.size
//st_list(i)

//기업 점수 df와 활동 점수 df를 내부 조인함
val df_out_join = df.join(out_act_score_df, df("GCI_STD_NO")===out_act_score_df("STD_NO"), "inner")


val scores = df_out_join.select(col("STD_NO"), col("TRUST"), col("ACT_SCORE")).collect //GCI_STD_NO를 제외하고 컬럼들 선택

var user_trust = Seq[(Int, Double)]() //최종 신뢰도가 들어갈 시퀀스

for(i<-0 until scores.size){ //코드가 너무 조잡함. 보기 추함. 정리 필요.
  //scores의 1:학번, 2:기업점수, 3:활동점수가 순서대로 들어감.
  var std_no = scores(i)(0)
  var trust = scores(i)(1)
  var act_score = scores(i)(2)
  val w1 = 0.5
  val w2 = 0.5

  user_trust = user_trust :+ (std_no.asInstanceOf[Int], w1*scores(i)(1).asInstanceOf[Double]+w2*scores(i)(2).asInstanceOf[Double]) //여기서 인자를 추가해서 계산해줘야
}

var user_trust_df = user_trust.toDF("STD_NO", "TRUST") //최종 결과를 데이터프레임으로 만듬


//============================콘텐츠,사용자 신뢰도(재용) end=======================================


//============================유사도(연희,소민)start=======================================

//-------------------- # # # 교과목 리스트 # # # --------------------------------
//--------------------from. 교과목수료 테이블 : 학과명, 학번, 학점----------------------
//<학과 DataFrame> : departDF / 전체 학과의 모든 학생
//###학과 별 학생 번호 보기 ###

 // 201028406
 // 201028409
 // 201028407
 // 201028410
 // 201028411
//-------------------------------------------------------
//studentDF_test 중 한명의 학번
//컴퓨터공학과 학생으로 테스트함
// 1. 학생이 질의 -> 학번으로 질의함
// 2-1. 학번으로 그 학생이 무슨 과인지 질의 => 질의 한 후 String으로 학과를 저장함
// 2-2. 학번의로 그 과의 소속 학생들을 리스트로 저장
// 3-1 학과 이름으로 과에 있는 학생들이 수강한 교과목을 리스트로 저장
// 3-2 학과 이름으로 과에서 개설된 교과목을 리스트로 저장
//
// var std_NO = 20142820
var std_NO = 20152611
// 컴공과 데이터 목록 |20142820||20142932| |20152611|
//--------------------from. 교과목수료 테이블 V_STD_CDP_SUBJECT : 학과이름, 학번, 수업명, 교과목코드-------------------------
// var studentNO = students_in_departNM.filter(students_in_departNM("STD_NO").equalTo(s"${std_NO}"))

// 2-1. 학과
var clPassUri_DF = clPassUri_table.select(col("STD_NO"), col("SUST_CD_NM"), col("SBJT_KOR_NM"), col("SBJT_KEY_CD")).distinct.toDF
var departNM_by_stdNO = clPassUri_DF.filter(clPassUri_DF("STD_NO").equalTo(s"${std_NO}")).select(col("SUST_CD_NM")).distinct
var departNM = departNM_by_stdNO.collect().map(_.getString(0)).mkString("")

// 2-2. 학과 학생 리스트
var stdNO_in_departNM = clPassUri_DF.filter(clPassUri_DF("SUST_CD_NM").equalTo(s"${departNM}")).select(col("STD_NO"))
var stdNO_in_departNM_List = stdNO_in_departNM.rdd.map(r=>r(0)).collect.toList.distinct

// 3-1. 학생의 수업 리스트
// var sbjtNM_by_stdNO = clPassUri_DF.filter(clPassUri_DF("STD_NO").equalTo(s"${std_NO}")).select(col("SBJT_KOR_NM"))
var sbjtNM_by_stdNO = clPassUri_DF.filter(clPassUri_DF("STD_NO").equalTo(s"${std_NO}")).select(col("SBJT_KEY_CD"))
var sbjtNM_by_stdNO_List = sbjtNM_by_stdNO.rdd.map(r=>r(0)).collect.toList.distinct


// 3-2. 학과의 수업 리스트
// 컴퓨터공학과에서 개설된 수업명을 리스트로 생성 : 과목명이 1452개 -> distinct 지정distinct하게 자름 => 108개
var sbjtCD_in_departNM = clPassUri_DF.filter(clPassUri_DF("SUST_CD_NM").equalTo(s"${departNM}")).select("SBJT_KEY_CD", "STD_NO")
//학과별 교과목코드와 학생의 교과목 코드를 인덱스 비교 하기 위해 정렬함(sorted)
var sbjtCD_in_departNM_List = sbjtCD_in_departNM.rdd.map(r=>r(0).toString).collect.toList.distinct.sorted

//학과의 모든 학번(key)이 들은 교과목코드-별점 Map

var cpsStarUri_DF = cpsStarUri_table.select(col("STD_NO"), col("STAR_KEY_ID"), col("STAR_POINT"), col("TYPE"))
// 교과 별점 => 별점 테이블에서 "TYPE"이 "C" :: ex) BAQ00028

//STAR_KEY_ID(교과목코드, sbjtCD), STAR_POINT이 있는 dataframe
var cpsStarUri_sbjt_DF = cpsStarUri_DF.filter(cpsStarUri_DF("TYPE").equalTo("C")).drop("TYPE")

/*
// --------------------------------------------별점 추가하기---------------------------------------------
// 컴퓨터 공학과 학생 4명에 대해 별점 테이블 데이터 추가함 : |20142820||20142932| |20152611| |20152615|
// 일단 지금은 이 학생들에 대핸 비교과 활동에 대한 별점을 추가했음
// 이 학생들의 교과 리스트를 가져와서 (Key) 그 Key에 해당하는 별점을 mongodb에 추가해줌

// |20142820||20142932| |20152611| /////////////// |20152615|
// 과목마다 같은 과목이여도 코드가 다를 수 있으므로 한번에 select를 하여 code를 가져와야함
var std_NO1 = 20142820 //(6개)
var sbjtNM_by_stdNO1 = clPassUri_DF.filter(clPassUri_DF("STD_NO").equalTo(s"${std_NO1}")).select(col("SBJT_KOR_NM"), col("SBJT_KEY_CD")).show
// 네트워크프로그래밍, 시스템분석설계, 인공지능, 네트워크보안, 정보기술세미나, 서버구축및관리
// AAM00351, AAM00341, AAM00361, AAM00331, AAM00371, AAM00121

var std_NO2 = 20142932 //(7개)
var sbjtNM_by_stdNO2 = clPassUri_DF.filter(clPassUri_DF("STD_NO").equalTo(s"${std_NO2}")).select(col("SBJT_KOR_NM"), col("SBJT_KEY_CD")).show
// 시스템분석설계, 네트워크보안, 일본어회화Ⅰ, 네트워크프로그래밍, "MOSⅡ ", 인공지능, 정보기술세미나
// AAM00341, AAM00331, TAA02231, AAM00351, TAA04151, AAM00361, AAM00371

var std_NO3 = 20152611 //(7개)
var sbjtNM_by_stdNO3 = clPassUri_DF.filter(clPassUri_DF("STD_NO").equalTo(s"${std_NO3}")).select(col("SBJT_KOR_NM"), col("SBJT_KEY_CD")).show
// 컴퓨터구조, 취업연계현장실습Ⅰ, 인간중심감성공학의이해, 운영체제, 자바프로그래밍, 데이터베이스응용, 정보통신
// AAM00231, COM00051, BCD00017, AAM00191, AAM00241, AAM00201, AAM00211

// --------------------------------------------별점 추가하기 끝---------------------------------------------
*/

//------------------------------------------------------------------------

var arr01 = Array(20142820, 20142932, 20152611)

case class starPoint(sbjtCD:String, starpoint:Any)
var sbjtCD_star_byStd_Map = collection.mutable.Map[String, Array[starPoint]]()


val cpsStarUri_DF = cpsStarUri_table.select(col("STD_NO"), col("STAR_KEY_ID"), col("STAR_POINT"), col("TYPE"))
// 교과 별점 => 별점 테이블에서 "TYPE"이 "C" :: ex) BAQ00028

//STAR_KEY_ID(교과목코드, sbjtCD), STAR_POINT이 있는 dataframe
val cpsStarUri_sbjt_DF = cpsStarUri_DF.filter(cpsStarUri_DF("TYPE").equalTo("C")).drop("TYPE")

var sbjtCD_star_byStd_Map = arr01.flatMap{ stdNO =>
  // 학번 별 학번-교과코드-별점
  val star_temp_bystdNO_DF = cpsStarUri_sbjt_DF.filter(cpsStarUri_sbjt_DF("STD_NO").equalTo(s"${stdNO}"))
  // star_temp_bystdNO_DF.show
  // var sbjtcd_star_temp =

  val res =
    star_temp_bystdNO_DF
    .collect // dataframe 를 collect 해서 array[row] 를 받음
    .map(record => (record(0).toString, starPoint(record(1).toString, record(2).toString)))
    // 각 row에 대해서 (학번, starPoint 객체) 로 바꿈
    .groupBy(x => x._1) // 이걸 하면 데이터 타입이 맵(학번, )
    .map( x => (x._1, x._2.map(x => x._2)))
    // 그룹바이를 학번으로 해서 (학번, Array)
  res
}.toMap

var sbjt_tuples = Seq[(String, List[Double])]()

for(s<-0 until sbjtCD_star_byStd_Map.size){
  var star_point_List = List[Any]() // 학번당 별점을 저장
  var orderedIdx_byStd = List[Int]() //학번 당 교과 리스트
  var not_orderedIdx_byStd = List[Int]()

  for(i<-0 until sbjtCD_in_departNM_List.size){
      star_point_List = 0.0::star_point_List
  }
  // var clPassUri_DF = clPassUri_table.select(col("STD_NO"), col("SUST_CD_NM"), col("SBJT_KOR_NM"), col("SBJT_KEY_CD")).distinct.toDF

  var sbjtNM_by_stdNO = clPassUri_DF.filter(clPassUri_DF("STD_NO").equalTo(s"${arr01(s)}")).select(col("SBJT_KEY_CD"))
  var sbjtNM_by_stdNO_List = sbjtNM_by_stdNO.rdd.map(r=>r(0)).collect.toList.distinct

  var getStar_by_stdNO = cpsStarUri_sbjt_DF.filter(cpsStarUri_sbjt_DF("STD_NO").equalTo(s"${arr01(s)}")).select("STAR_KEY_ID").collect.map({row=>
     val str = row.toString
     val size = str.length
     val res = str.substring(1, size-1).split(",")
     val list_ = res(0)
     list_})

  var not_rated = sbjtNM_by_stdNO_List filterNot getStar_by_stdNO.contains

  var valueBystdNo_from_Map = sbjtCD_star_byStd_Map(arr01(s).toString) //!

  for(i<-0 until valueBystdNo_from_Map.size){
    //학생 한명의 중분류-별점 맵에서 중분류 키에 접근 : valueBystdNo_from_Map(i).subcd)
    //학생 한명이 들은 중분류 리스트를 가져옴
    orderedIdx_byStd = sbjtCD_in_departNM_List.indexOf(valueBystdNo_from_Map(i).sbjtCD)::orderedIdx_byStd
    // println("orderedIdx_byStd ===> " + orderedIdx_byStd)
  }

  for(i<-0 until not_rated.size){
    not_orderedIdx_byStd = sbjtCD_in_departNM_List.indexOf(not_rated(i).toString)::not_orderedIdx_byStd
    // println("not_orderedIdx_byStd ===> " + not_orderedIdx_byStd)
  }

  orderedIdx_byStd = orderedIdx_byStd.sorted
  // println("orderedIdx_byStdsorted ===>" + orderedIdx_byStd)

  not_orderedIdx_byStd = not_orderedIdx_byStd.sorted
  println("not_orderedIdx_byStd ===>" + not_orderedIdx_byStd)

  for(i<-0 until not_orderedIdx_byStd.size){
    star_point_List = star_point_List.updated(not_orderedIdx_byStd(i), -1)
  }

  for(i<-0 until orderedIdx_byStd.size){ // orderedIdx_byStd 크기 (학번당 들은 중분류를 for문 돌림)
    var k=0;
    //print(k)
    // 학과 전체의 중분류 리스트와 학생의 중분류 리스트의 값이 같을때까지 k를 증가
    while(sbjtCD_in_departNM_List(orderedIdx_byStd(i))!= valueBystdNo_from_Map(k).sbjtCD){
    k+=1;
    }
    // 같은 값이 나오면 0으로 설정돼있던 값을 (그 자리의 값을) 학생의 별점으로 바꿔줌
    star_point_List = star_point_List.updated(orderedIdx_byStd(i), valueBystdNo_from_Map(k).starpoint)
    // println(s"$star_point_List")
  }

  val star_list = star_point_List.map(x => x.toString.toDouble)
  println(">>"+star_list)
  sbjt_tuples = sbjt_tuples :+ (arr01(s).toString, star_list)
}

var sbjt_df = sbjt_tuples.toDF("STD_NO", "SUBJECT_STAR")

//======================================================================================================
//======================================================================================================


//-------------------- # # # 비교과 리스트 # # # --------------------------------
// from. 교과/비교과용 별점 테이블(CPS_STAR_POINT) : 학번(STD_NO), 비교과id(STAR_KEY_ID), 별점(STAR_POINT)
// from. 비교과 관련 테이블(CPS_NCR_PROGRAM_INFO) : 비교과id(NPI_KEY_ID), 중분류(NPI_AREA_SUB_CD)

//비교과 아이디(학번, 비교과id 사용 from.교과/비교과별점테이블)로 중분류 가져오기(비교과id, 중분류 from.비교과 관련 테이블)

//학과 별 학생들이 수강한 비교과의 중분류 list 로 포맷 잡고 : 학과 - 학번 돌면서 list 만들고 , 중분류로 바꿔주기
//학생 한명이 수강한 비교과 list -> 별점 가져오기(from. 교과/비교과 별점 테이블) -> 중분류 가져오기 -> 중분류 별 별점 avg 계산


// from. 교과/비교과용 별점 테이블(CPS_STAR_POINT) : 학번(STD_NO), 비교과id(STAR_KEY_ID), 별점(STAR_POINT), 타입(TYPE)
var cpsStarUri_DF = cpsStarUri_table.select(col("STD_NO"), col("STAR_KEY_ID").as("NPI_KEY_ID"), col("STAR_POINT"), col("TYPE"))
// from. 비교과 관련 테이블(CPS_NCR_PROGRAM_INFO) : 비교과id(NPI_KEY_ID), 중분류(NPI_AREA_SUB_CD)
var ncrInfoUri_DF = ncrInfoUri_table.select(col("NPI_KEY_ID"), col("NPI_AREA_SUB_CD"))

// 비교과 별점 => 별점 테이블에서 "TYPE"이 "N" :: ex) NCR000000000677
var cpsStarUri_ncr_DF = cpsStarUri_DF.filter(cpsStarUri_DF("TYPE").equalTo("N"))
// 교과 별점 => 별점 테이블에서 "TYPE"이 "C" :: ex) BAQ00028
//var cpsStarUri_sbjt_DF = cpsStarUri_DF.filter(cpsStarUri_DF("TYPE").equalTo("C"))


// var std_NO1 = 20190030
//
// var std_NO2 = 20142820 // 6개
// // 프로그램 id : NCR000000000694, NCR000000000723, NCR000000000679, NCR000000000731, NCR000000000671, NCR000000000780
// // 중분류 code : NCR_T01_P03_C01, NCR_T01_P01_C01, NCR_T01_P03_C03, NCR_T01_P04_C03, NCR_T01_P02_C03, NCR_T01_P02_C03
// var std_NO3 = 20142932 // 5게
// // 프로그램 id : NCR000000000694, NCR000000000723, NCR000000000731, NCR000000000737, NCR000000000743
// // 중분류 code : NCR_T01_P03_C01, NCR_T01_P01_C01, NCR_T01_P04_C03, NCR_T01_P05_C02, NCR_T01_P01_C03
// var std_NO4 = 20152611 // 5개
// // 프로그램 id : NCR000000000737, NCR000000000743, NCR000000000748, NCR000000000723, NCR000000000716
// // (20142820, 20142932, 20152611)

//---------------------------<학생 한명의 비교과중분류 별 별점평균 데이터프레임 생성>----------------------------------------
// 학생 한 명에 대해서 별점테이블을 조회해서 교과/비교과 관련 활동 KEY_ID를 가져옴 => List 생성
// var key_id_temp = cpsStarUri_ncr_DF.select(col("NPI_KEY_ID")).filter(cpsStarUri_DF("STD_NO").equalTo(s"${std_NO2}"))
// var key_id_List_byStd = key_id_temp.rdd.map(r=>r(0)).collect.toList

//학생 한명에 대한 중분류별 별점 평균 리스트 만들기 (star_subcd_avg_DF를 list로 변환)

import org.apache.spark.sql.types.{
    StructType, StructField, StringType, IntegerType}
import org.apache.spark.sql.Row

val schema3 = StructType(
    StructField("STAR_POINT", StringType, true) ::
    StructField("NPI_KEY_ID", StringType, true) :: Nil)
var star_subcd_DF = spark.createDataFrame(sc.emptyRDD[Row], schema3)

val schema4 = StructType(
    StructField("NPI_AREA_SUB_CD", StringType, true) :: Nil)
var subcd_byStd_DF = spark.createDataFrame(sc.emptyRDD[Row], schema4)

//----------------------------------------------------------------------------------------------------------------------------

//-----------------------------------------------<학과의 비교과중분류 리스트 생성>------------------------------------------------
// 2-1. 학과
var std_NO = 20152611
var clPassUri_DF = clPassUri_table.select(col("STD_NO"), col("SUST_CD_NM"), col("SBJT_KOR_NM"), col("SBJT_KEY_CD")).distinct.toDF
var departNM_by_stdNO = clPassUri_DF.filter(clPassUri_DF("STD_NO").equalTo(s"${std_NO}")).select(col("SUST_CD_NM")).distinct
var departNM = departNM_by_stdNO.collect().map(_.getString(0)).mkString("")

var clPassUri_DF_ncr = clPassUri_table.select(col("SUST_CD_NM"), col("STD_NO")).distinct.toDF
var ncrInfoUri_DF = ncrInfoUri_table.select(col("NPI_KEY_ID"), col("NPI_AREA_SUB_CD"))
//map연산은 dataframe에 쓸 수 없기 때문에 list로 변환해야 하며 dataframe을 list로 변환하려면 df의 값 하나하나에 접근하기 위해 map 연산이 필요함
//광홍과df(clpass 교과목 수료 테이블에서 학과 별 학번 dataframe을 생성한 뒤 list로 변환)

// var departNM = "컴퓨터공학과"
var stdNO_in_departNM = clPassUri_DF_ncr.filter(clPassUri_DF_ncr("SUST_CD_NM").equalTo(s"${departNM}")).select(col("STD_NO")).rdd.map(r=>r(0)).collect.toList

case class starPoint(subcd:String, starpoint:Any)

// Map 타입의 변수 (string, Array)를 인자로 받음
// String : 학번, Array : (중분류, 별점)
val subcd_star_byDepart_Map = collection.mutable.Map[String, Array[starPoint]]()
val subcd_byDepart_Map_temp = collection.mutable.Map[String, Array[String]]()
var subcd_byDepart_List = List[Any]()

// 학과별 중분류 중복 제거를 위해 Set으로 데이터타입 선언
val tmp_set = scala.collection.mutable.Set[String]()

var myResStr = ""

val schema1 = StructType(
    StructField("STAR_POINT", StringType, true) ::
    StructField("NPI_KEY_ID", StringType, true) :: Nil)

val schema2 = StructType(
    StructField("NPI_AREA_SUB_CD", StringType, true) ::
    StructField("NPI_KEY_ID", StringType, true) :: Nil)

var arr01 = Array(20142820, 20142932, 20152611)

  arr01.foreach{ stdNO =>
    var star_keyid_DF = spark.createDataFrame(sc.emptyRDD[Row], schema1)
    var subcd_keyid_DF = spark.createDataFrame(sc.emptyRDD[Row], schema2)

    val key_id_temp = cpsStarUri_ncr_DF.select(col("NPI_KEY_ID")).filter(cpsStarUri_DF("STD_NO").equalTo(s"${stdNO}"))
    val key_id_List_byStd = key_id_temp.rdd.map{r=> r(0)}.collect.toList

    val kidList = key_id_List_byStd.map { keyid =>

      // var std_NO1 = 20142820 // 5게
      // 프로그램 id : NCR000000000694, NCR000000000723, NCR000000000679, NCR000000000731, NCR000000000671, NCR000000000780
      // 별점 : 4.2, 4.5, 4.0, 3.8, 3.2, 4.5
      // 중분류 code : NCR_T01_P03_C01, NCR_T01_P01_C01, NCR_T01_P03_C03, NCR_T01_P04_C03, NCR_T01_P02_C03, NCR_T01_P02_C03
      //
      // var std_NO2 = 20142932 // 5게
      // 프로그램 id : NCR000000000694, NCR000000000723, NCR000000000731, NCR000000000737, NCR000000000743
      // 별점 : 3.9, 5.0, 4.7, 4.8, 2.9
      // 중분류 code : NCR_T01_P03_C01, NCR_T01_P01_C01, NCR_T01_P04_C03, NCR_T01_P05_C02, NCR_T01_P01_C03
      //
      // var std_NO3 = 20152611 // 5게
      // 프로그램 id : NCR000000000737, NCR000000000743, NCR000000000748, NCR000000000723, NCR000000000716
      // 별점 : 2.7, 3.6, 4.0, 4.1, 3.4
      // 중분류 code : NCR_T01_P05_C02, NCR_T01_P01_C03, NCR_T01_P04_C03, NCR_T01_P01_C01, NCR_T01_P04_C07
      //
      // Map(
      //   20142932 -> Array(starPoint(NCR_T01_P01_C03,2.9), starPoint(NCR_T01_P05_C02,4.8), starPoint(NCR_T01_P04_C03,3.8), starPoint(NCR_T01_P01_C01,4.5), starPoint(NCR_T01_P02_C03,3.85), starPoint(NCR_T01_P03_C03,4.0), starPoint(NCR_T01_P03_C01,4.2)),
      //   20152611 -> Array(starPoint(NCR_T01_P01_C03,2.9), starPoint(NCR_T01_P05_C02,4.8), starPoint(NCR_T01_P04_C03,3.9), starPoint(NCR_T01_P04_C07,3.4), starPoint(NCR_T01_P01_C01,4.5), starPoint(NCR_T01_P02_C03,3.85), starPoint(NCR_T01_P03_C03,4.0), starPoint(NCR_T01_P03_C01,4.2)),
      //   20142820 -> Array(starPoint(NCR_T01_P04_C03,3.8), starPoint(NCR_T01_P01_C01,4.5), starPoint(NCR_T01_P02_C03,3.85), starPoint(NCR_T01_P03_C03,4.0), starPoint(NCR_T01_P03_C01,4.2)))

      //별점 테이블에서 모든 학생의 비교과키, 별점이 있는데 학번 하나에 대한 것만 가져옴 : 학번, 비교과키, 별점, 타입(N)
      var getStar_by_stdNO = cpsStarUri_ncr_DF.filter(cpsStarUri_DF("STD_NO").equalTo(s"${stdNO}")).toDF
      //비교과정보DF에서 모든 학생의 중분류, 비교과키가 있는데 학번 하나에 대한 것만 가져옴
      val subcd_keyid_DF_temp = ncrInfoUri_DF.select(col("NPI_AREA_SUB_CD"),col("NPI_KEY_ID")).filter(ncrInfoUri_DF("NPI_KEY_ID").equalTo(s"${keyid}"))
      //학번, 비교과키, 별점, 타입(N)이 있는 DF에서 학번 하나의 비교과키에 해당하는 별점, 비교과키만 가져오기
      val star_keyid_DF_temp = getStar_by_stdNO.select(col("STAR_POINT"),col("NPI_KEY_ID")).filter(getStar_by_stdNO("NPI_KEY_ID").equalTo(s"${keyid}"))
      // println("==============================" + s"${stdNO}")
      //한줄씩 나옴..ㅋ
      // println("star_keyid_DF_temp=====================>")
      // star_keyid_DF_temp.show
      // println("subcd_keyid_DF_temp=====================>")
      // subcd_keyid_DF_temp.show

      star_keyid_DF = star_keyid_DF.union(star_keyid_DF_temp)
      subcd_keyid_DF = subcd_keyid_DF.union(subcd_keyid_DF_temp)

      //학생1 : 6, 학생2 : 5, 학생3 : 5줄의 데이터프레임 출력됨
      // println("star_keyid_DF=====================>")
      // star_keyid_DF.show
      // subcd_keyid_DF.show
      //
      // val subcd_byStd_DF_temp = ncrInfoUri_DF.select(col("NPI_AREA_SUB_CD")).filter(ncrInfoUri_DF("NPI_KEY_ID").equalTo(s"${keyid}"))
      // subcd_byStd_DF_temp.show
      // subcd_byStd_DF = subcd_byStd_DF.union(subcd_byStd_DF_temp)
      // println("union=====================>")
      // subcd_byStd_DF.show

      // star_keyid_DF = star_keyid_DF.dropDuplicates("NPI_KEY_ID")
      // subcd_keyid_DF = subcd_keyid_DF.dropDuplicates("NPI_KEY_ID")

      // star_keyid_DF.show
      // subcd_keyid_DF.show

      val star_subcd_DF_temp = star_keyid_DF.join(subcd_keyid_DF, Seq("NPI_KEY_ID"), "left_outer")
      // println("==========std===========" + (s"${stdNO}"))
      // println("==========star_subcd_DF_temp===========")
      // star_subcd_DF_temp.show

      // agg 연산을 위해 필요없는 비교과 keyid를 지움!!
      val star_subcd_DF = star_subcd_DF_temp.drop("NPI_KEY_ID")
      // star_subcd_DF.show
      val star_subcd_avg_DF = star_subcd_DF.groupBy("NPI_AREA_SUB_CD").agg(avg("STAR_POINT"))
      // star_subcd_avg_DF.show
      //
      //
      val subcd_byStd_DF2 = star_subcd_DF.drop("STAR_POINT")
      // println("==========subcd_byStd_DF2===========")
      // subcd_byStd_DF2.show
      subcd_byStd_DF = subcd_byStd_DF.union(subcd_byStd_DF2)
      // println("==========subcd_byStd_DF2===========")
      // subcd_byStd_DF2.show
      //
      // println("==========testzz===========")
      // testzz.show

      val subcd_star_temp = star_subcd_avg_DF.collect.map{ row =>
        val str = row.toString
        val size = str.length
        val res = str.substring(1, size-1).split(",")
        val starP = starPoint(res(0), res(1))
        starP
      }
      // println(subcd_star_temp)
      //
      val subcd_star_record = (stdNO.toString, subcd_star_temp)
      // println(s"star 1 ==== ${subcd_star_record.mkString(",")} \n ====")
      subcd_star_byDepart_Map+=(subcd_star_record)
      //
      val subcd_byDepart_temp = subcd_byStd_DF.collect.map{ row =>
        // 별점만 가져온거
          // println(row)
          val str = row.toString
          val size = str.length
          val res = str.substring(1, size-1).split(",")(0)
          if(tmp_set.add(res)) {
            // println(s"insert new value : ${res}")
            tmp_set.+(res)
          }
          res
      }.sortBy(x => x)

      val subcd_record_byDepart = (s"$stdNO", subcd_byDepart_temp)
      subcd_byDepart_Map_temp += subcd_record_byDepart
      val t1 = subcd_byDepart_Map_temp.map(x => x._2).flatMap(x => x).toList.distinct

      // star_subcd_DF_temp.show // TO DEBUG
      //-===========================================이건 학과 전체 중분류 목록 !!!!!!!!!!! 8개가 맞음 학생거를 다 중복제거하고 더함
      // List(NCR_T01_P01_C01, NCR_T01_P01_C03, NCR_T01_P02_C03, NCR_T01_P03_C01, NCR_T01_P03_C03, NCR_T01_P04_C03, NCR_T01_P05_C02, NCR_T01_P04_C07)

      // println("t1-------------------->" + t1)
    }

   val t1 = subcd_byDepart_Map_temp.map(x => x._2).flatMap(x => x).toList.distinct
   val xres = s"result stdno : ${stdNO} size : ${t1.length} ==--------------------> ${t1}"
   myResStr = myResStr.concat("\n"+xres)
  // Map(
  //   20142932 -> Array(starPoint(NCR_T01_P01_C03,2.9), starPoint(NCR_T01_P05_C02,4.8), starPoint(NCR_T01_P04_C03,4.7), starPoint(NCR_T01_P01_C01,5.0), starPoint(NCR_T01_P03_C01,3.9)),
  //   20152611 -> Array(starPoint(NCR_T01_P01_C03,3.6), starPoint(NCR_T01_P05_C02,2.7), starPoint(NCR_T01_P04_C03,4.0), starPoint(NCR_T01_P04_C07,3.4), starPoint(NCR_T01_P01_C01,4.1)),
  //   20142820 -> Array(starPoint(NCR_T01_P04_C03,3.8), starPoint(NCR_T01_P01_C01,4.5), starPoint(NCR_T01_P02_C03,3.85), starPoint(NCR_T01_P03_C03,4.0), starPoint(NCR_T01_P03_C01,4.2)))

  subcd_byDepart_List = t1
} //학번 루프 끝

  // 학과의 중분류 List
  // var std_NO3 = 20142932 // 5게
  // 프로그램 id : NCR000000000694, NCR000000000723, NCR000000000731, NCR000000000737, NCR000000000743
  // 중분류 code : NCR_T01_P03_C01, NCR_T01_P01_C01, NCR_T01_P04_C03, NCR_T01_P05_C02, NCR_T01_P01_C03
  //
  // Map(20142932 -> Array(starPoint(NCR_T01_P01_C03,null), starPoint(NCR_T01_P05_C02,null), starPoint(NCR_T01_P04_C03,3.8), starPoint(NCR_T01_P01_C01,4.5), starPoint(NCR_T01_P02_C03,3.85), starPoint(NCR_T01_P03_C03,4.0), starPoint(NCR_T01_P03_C01,4.2)),
  // 20152611 -> Array(starPoint(NCR_T01_P01_C03,null), starPoint(NCR_T01_P05_C02,null), starPoint(NCR_T01_P04_C03,3.8), starPoint(NCR_T01_P04_C07,null), starPoint(NCR_T01_P01_C01,4.5), starPoint(NCR_T01_P02_C03,3.85), starPoint(NCR_T01_P03_C03,4.0), starPoint(NCR_T01_P03_C01,4.2)),
  // 20142820 -> Array(starPoint(NCR_T01_P04_C03,3.8), starPoint(NCR_T01_P01_C01,4.5), starPoint(NCR_T01_P02_C03,3.85), starPoint(NCR_T01_P03_C03,4.0), starPoint(NCR_T01_P03_C01,4.2)))

  // println(subcd_byDepart_List)

  // 학과의 모든 학번의 (중분류, 별점) Map
  subcd_star_byDepart_Map
//----------------------------------------------------------------------------------------------------------------------------

var arr01 = Array(20142820, 20142932, 20152611)
//
// subcd_star_byDepart_Map("20142932")(0).subcd
// subcd_star_byDepart_Map("20142932")(0).starpoint

//최종적인 학번 별 별점 리스트 값이 들어있는 시퀀스
var ncr_tuples = Seq[(String, List[Double])]()

for(s<-0 until subcd_star_byDepart_Map.size){ // 학과 학생 학번 List 를 for문
  var star_point_List = List[Any]() // 학번당 별점을 저장
  var orderedIdx_byStd = List[Int]() //학번 당 중분류 리스트
  //학과 전체 중분류 코드 List => 학번당 별점을 중분류 갯수만금 0.0으로 초기화
  for(i<-0 until subcd_byDepart_List.size){
    star_point_List = 0.0::star_point_List
  }
  //Map연산을 위해 학번을 string으로 변환(arr01(s).toString)
  // 학번당 중분류를 orderedIdx_byStd에 넣음
  //i : 학번 하나가 가진 중분류 개수만큼 반복

  //학과 모든 학생의 중분류-별점 Map 에서 학번 하나의 값(중분류-별점)을 가져옴(Map연산을 위해 toString으로 변환)
  var valueBystdNo_from_Map = subcd_star_byDepart_Map(arr01(s).toString)

  for(i<-0 until valueBystdNo_from_Map.size){
    //학생 한명의 중분류-별점 맵에서 중분류 키에 접근 : valueBystdNo_from_Map(i).subcd)
    //학생 한명이 들은 중분류 리스트를 가져옴
    orderedIdx_byStd = subcd_byDepart_List.indexOf(valueBystdNo_from_Map(i).subcd)::orderedIdx_byStd
    // println("orderedIdx_byStd ===> " + orderedIdx_byStd)
  }
  // orderedIdx_byStd를 정렬(중분류 코드 정렬)
  orderedIdx_byStd = orderedIdx_byStd.sorted
  // println("orderedIdx_byStdsorted ===>" + orderedIdx_byStd)

  for(i<-0 until orderedIdx_byStd.size){ // orderedIdx_byStd 크기 (학번당 들은 중분류를 for문 돌림)
    var k=0;
    //print(k)
    // 학과 전체의 중분류 리스트와 학생의 중분류 리스트의 값이 같을때까지 k를 증가
    while(subcd_byDepart_List(orderedIdx_byStd(i))!= valueBystdNo_from_Map(k).subcd){
    k+=1;
    }
    // 같은 값이 나오면 0으로 설정돼있던 값을 (그 자리의 값을) 학생의 별점으로 바꿔줌
    star_point_List = star_point_List.updated(orderedIdx_byStd(i), valueBystdNo_from_Map(k).starpoint)
    // println(s"$star_point_List")
  }
  val star_list = star_point_List.map(x => x.toString.toDouble)
  // println(">>"+star_list)
  ncr_tuples = ncr_tuples :+ (arr01(s).toString, star_list)
}

var ncr_df = ncr_tuples.toDF("STD_NO", "NCR_STAR")

//===========================================================================================================
//===========================================================================================================

//-------------------- # # # 자율활동 리스트 # # # ------------------------------
//from.교외활동 CPS_OUT_ACTIVITY_MNG : 학번(OAM_STD_NO), 활동구분코드(OAM_TYPE_CD), 활동명(OAM_TITLE)
//자격증(CD01) : 이름(OAM_TITLE) / ex. 토익800~900, FLEX 일본어 2A,  FLEX 일본어 1A,  FLEX 중국어 1A
//어학(CD02) : 이름(OAM_TITLE)
//봉사(CD03), 대외활동(CD04), 기관현장실습(CD05) : 활동구분코드(OAM_TYPE_CD)

import spark.implicits._

val schema6 = StructType(
    StructField("STAR_POINT", StringType, true) ::
    StructField("NPI_KEY_ID", StringType, true) :: Nil)
var star_subcd_DF = spark.createDataFrame(sc.emptyRDD[Row], schema3)

// 2-1. 학과
var std_NO = 20152611
var clPassUri_DF = clPassUri_table.select(col("STD_NO"), col("SUST_CD_NM"), col("SBJT_KOR_NM"), col("SBJT_KEY_CD")).distinct.toDF
var departNM_by_stdNO = clPassUri_DF.filter(clPassUri_DF("STD_NO").equalTo(s"${std_NO}")).select(col("SUST_CD_NM")).distinct
var departNM = departNM_by_stdNO.collect().map(_.getString(0)).mkString("")

var outActUri_DF = outActUri_table.select(col("OAM_STD_NO"), col("OAM_TYPE_CD"), col("OAM_TITLE"))
// var departNM = "광고홍보학과"
// var departNM = "컴퓨터공학과"
// var std_NO1 = 201937039
var std_NO1 = 20142820
var std_NO2 = 20130001
var clPassUri_DF_act = clPassUri_table.select(col("SUST_CD_NM"), col("STD_NO")).distinct.toDF
//map연산은 dataframe에 쓸 수 없기 때문에 list로 변환해야 하며 dataframe을 list로 변환하려면 df의 값 하나하나에 접근하기 위해 map 연산이 필요함

var stdNO_in_departNM = clPassUri_DF_act.filter(clPassUri_DF_act("SUST_CD_NM").equalTo(s"${departNM}")).select(col("STD_NO")).rdd.map(r=>r(0)).collect.toList.map(_.toString)

// var arr01 = Array(201937039, 20153128, 20132019)
var arr01 = Array(20142820, 20142932, 20152611)
// var arr01 = Array(20142820)
var arr02 = arr01.toList.map(_.toString)

//광홍과 학생 중 자율활동 데이터가 있는 학생은 극소수
// var stdNo_test_df =  outActUri_DF.filter(outActUri_DF("OAM_STD_NO").equalTo(s"${20132019}")).select(col("OAM_TYPE_CD"), col("OAM_TITLE"))

var depart_activity_temp = List[Any]()
var depart_activity_List = List[Any]()
var activity_List_byStd = List[Any]()
var act_tuples = Seq[(String, List[Int])]()
//광홍과 학번을 돌면서
arr02.foreach{ stdNO =>

  var depart_code_list = List[Any]("OAMTYPCD03", "OAMTYPCD04", "OAMTYPCD05")

  //List1 : 코드(중복제거x) -> map 함수로 df에서 list 변환
  //List2 : 이름(중복제거) -> map 함수로 df에서 list 변환
  //List3 : List1 + List2 = 코드리스트 + 이름리스트 (학생 한명이 수행한 자율활동내용)
  //---------------------자율활동 code list(자격증01, 어학02)----------------------
  //5개의 코드 (학생 한명이 수행한 봉사03, 대외04, 기관05을 코드 별로 groupby count list)
  var outAct_code_temp1 = outActUri_DF.filter(outActUri_DF("OAM_STD_NO").equalTo(s"${stdNO}")).select(col("OAM_STD_NO"),col("OAM_TYPE_CD"), col("OAM_TITLE"))
  //3개의 코드만 필터링
  var outAct_code_temp2 = outAct_code_temp1.filter($"OAM_TYPE_CD" === "OAMTYPCD03" || $"OAM_TYPE_CD" ==="OAMTYPCD04" || $"OAM_TYPE_CD" ==="OAMTYPCD05")
  //학생 한명의 활동 코드만 존재하는 dataframe

  var outAct_code_temp3 = outAct_code_temp2.drop("OAM_STD_NO", "OAM_TITLE").groupBy("OAM_TYPE_CD").count()

  //모든 학과 모든 학생이 수행한 자율활동은 총 845개인데
  //광홍과 201937039학생이 수행한 활동만 260개이고 나머지 광홍과 학생들의 데이터는 존재하지 않음

  //이제 코드 3개에 대해 groupby와 agg 연산을 사용하여 코드 별로 count
  // var outAct_code_List = outAct_code_temp3.select(col("count")).rdd.map(r=>r(0)).collect.toList
  val maps = scala.collection.mutable.Map[String, Int]()

  for(i<-0 until depart_code_list.size){
  	maps(depart_code_list(i).toString) = 0
  }

  val s = outAct_code_temp3.collect

  for(i<-0 until s.size){
  	maps(s(i)(0).toString) = s(i)(1).toString.toInt
  }
  val maps_ = maps.toSeq.sortBy(_._1).toMap
  val head = maps_.values.toList
  // print("head.size: " + head.size)

  //------------------------------------------------------------------------------

  //---------------------자율활동 name list(자격증01, 어학02)----------------------
  //5개의 코드
  var outAct_name_temp1 = outActUri_DF.filter(outActUri_DF("OAM_STD_NO").equalTo(s"${stdNO}")).select(col("OAM_STD_NO"), col("OAM_TITLE"), col("OAM_TYPE_CD")).distinct
  //3개의 코드만 필터링
  var outAct_name_temp2 = outAct_name_temp1.drop("OAM_STD_NO", "OAM_TYPE_CD").filter($"OAM_TYPE_CD" === "OAMTYPCD01" || $"OAM_TYPE_CD" ==="OAMTYPCD02").distinct

  var outAct_name_List = outAct_name_temp2.rdd.map(r=>r(0)).collect.toList
  // println("outAct_name====>" + outAct_name_List)
  // var t_size = outAct_name_List.length
  // if(t_size > 0) {
    // println(s"##### SIZE : ${t_size} RESULT => " + outAct_name_List)
  // }

  depart_activity_temp = depart_activity_temp ++ outAct_name_List

  //----------codeList + nameList = 학생 하나의 리스트-----------------------------
  // var outAct_std_List = outAct_code_List ::: outAct_name_List
  // outAct_std_List
  //------------------------------------------------------------------------------

  //학과 리스트
  depart_activity_List = depart_activity_temp.distinct
  // println("depart::::::::::::" + depart_activity_List)

  //namelist로 유무 비교
  //학생 name list 랑 학과 list를 비교해서 contain으로 1, 0

  var activity_List_byStd_temp1 = depart_activity_List.map(x => (x, 0)).map{ activity =>
    //x : record_1
    //0 : record_2
    //isListend면 1로 바뀜
    val actName = activity._1
    println("actName ===> " + actName)
    val act =
      if(outAct_name_List.contains(actName)) {
        1
      }
      else 0
    var activity_List_byStd_temp2 = (actName, act)
    print(activity_List_byStd_temp2)
    //리턴하려면 이름을 쳐야 함
    //최종적으로 isListened_List_temp1 = isListened_List_temp2 값이 담기는 것 !!
    activity_List_byStd_temp2
  }
  var activity_List_byStd_temp3 = activity_List_byStd_temp1.map(_._2)

  // println("activity_List_byStd: " + activity_List_byStd_temp3)

  //학과 리스트를 돌면서 일치 여부 세는데
  //봉사03, 대외04, 기관05 = 횟수 count
  //자격증01, 어학02 = 유무(1 또는 0)

 // activity_List_byStd = outAct_code_List ++ activity_List_byStd_temp3
 activity_List_byStd = head ++ activity_List_byStd_temp3

 //학생 별 코드 횟수, 이름 유무 리스트 출력
 // println(stdNO)
 // println("activity List !!! : " + activity_List_byStd)

 val act_list = activity_List_byStd.map(x => x.toString.toInt)

 act_tuples = act_tuples :+ (stdNO, act_list)
}

var act_df = act_tuples.toDF("STD_NO", "ACTING_COUNT")

// var act_df_test = act_df.filter(act_df("STD_NO").equalTo("201937039")).show

//------------------------------------------------------------------------------

//===========================================================================================================
//===========================================================================================================
//---------------------------------교과(sbjt_df), 비교과(ncr_df), 자율활동(act_df) join -------------------------------

val join_df_temp = sbjt_df.join(ncr_df, Seq("STD_NO"), "outer")
val join_df = join_df_temp.join(act_df, Seq("STD_NO"), "outer")

join_df.show

// setMongoDF_USER_LIST(spark, join_df)
//mongodb에 저장할 때 중복 제거해서 넣기

MongoSpark.save(
  join_df.write
    .option("spark.mongodb.output.uri", "mongodb://127.0.0.1/cpmongo_distinct.USER_LIST_FOR_SIMILARITY")
    .mode("overwrite"))


    // mongodb collection 제거
    // db.USER_SIMILARITY.drop()

    var userforSimilarity_df = userforSimilarity_table.select(col("STD_NO"), col("SUBJECT_STAR"), col("NCR_STAR"), col("ACTING_COUNT"))
    userforSimilarity_df = userforSimilarity_df.drop("_id")

    //질의자
    var querySTD_NO = 20142820
    var querySTD = userforSimilarity_df.filter(userforSimilarity_df("STD_NO").equalTo(s"${querySTD_NO}")).drop("STD_NO")

    val exStr = "WrappedArray|\\(|\\)|\\]|\\["


    var querySTD_List = querySTD.collect.toList.mkString.replaceAll(exStr, "").split(",").map(x => (x.trim.toDouble * 10).toInt)
    var sbjt_star = querySTD.select(col("SUBJECT_STAR")).collect.toList.mkString.replaceAll(exStr, "").split(",").map(x=>(x.trim.toDouble * 10).toInt)
    var ncr_star = querySTD.select(col("NCR_STAR")).collect.toList.mkString.replaceAll(exStr, "").split(",").map(x=>(x.trim.toDouble * 10).toInt)
    var acting_count = querySTD.select(col("ACTING_COUNT")).collect.toList.mkString.replaceAll(exStr, "").split(",").map(x=>(x.trim.toDouble * 10).toInt)

    var w1, w2, w3 = 0.33 //가중치 값
    var a, b, r = 1 //알파, 베타, 감마

    object CosineSimilarity {
       def dotProduct(x: Array[Int], y: Array[Int]): Int = {
         (for((a, b) <- x zip y) yield a * b) sum
       }
       def magnitude(x: Array[Int]): Double = {
         math.sqrt(x map(i => i*i) sum)
       }
      def cosineSimilarity(x: Array[Int], y: Array[Int]): Double = {
        require(x.size == y.size)
        dotProduct(x, y)/(magnitude(x) * magnitude(y))
      }
    }

    var user_sim_tuples = Seq[(String, Double)]()
    var user_sim_tuples_sbjt = Seq[(String, Double)]()
    var user_sim_tuples_ncr = Seq[(String, Double)]()
    var user_sim_tuples_act = Seq[(String, Double)]()
    var stdNO_inDepart_List = userforSimilarity_df.select(col("STD_NO")).rdd.map(x => x(0)).collect().toList

    stdNO_inDepart_List.foreach(stdNO => {
      // var i = 0; //토탈 구해줄 떄 바꿀라고

      println(stdNO)
      //유사사용자
      var std_inDepart = userforSimilarity_df.filter(userforSimilarity_df("STD_NO").equalTo(s"${stdNO}")).drop("STD_NO")
      var std_inDepart_List = std_inDepart.collect.toList.mkString.replaceAll(exStr, "").split(",").map(x => (x.trim.toDouble * 10).toInt)

      //교과
      var sbjt_star_ = std_inDepart.select(col("SUBJECT_STAR")).collect.toList.mkString.replaceAll(exStr, "").split(",").map(x=>(x.trim.toDouble * 10).toInt)
      //비교과
      var ncr_star_ = std_inDepart.select(col("NCR_STAR")).collect.toList.mkString.replaceAll(exStr, "").split(",").map(x=>(x.trim.toDouble * 10).toInt)
      //자율활동
      var acting_count_ = std_inDepart.select(col("ACTING_COUNT")).collect.toList.mkString.replaceAll(exStr, "").split(",").map(x=>(x.trim.toDouble * 10).toInt)

      //val sim = CosineSimilarity.cosineSimilarity(querySTD_List, std_inDepart_List)
      val sbjt_sim = CosineSimilarity.cosineSimilarity(sbjt_star, sbjt_star_)
      val ncr_sim = CosineSimilarity.cosineSimilarity(ncr_star, ncr_star_)
      val acting_sim = CosineSimilarity.cosineSimilarity(acting_count, acting_count_)

      //println(sim)
      //user_sim_tuples = user_sim_tuples :+ (s"${stdNO}", sim)
      user_sim_tuples_sbjt = user_sim_tuples_sbjt :+ (s"${stdNO}", sbjt_sim)
      user_sim_tuples_ncr = user_sim_tuples_ncr :+ (s"${stdNO}", ncr_sim)
      user_sim_tuples_act = user_sim_tuples_act :+ (s"${stdNO}", acting_sim)

      // var total_sim = (user_sim_tuples_sbjt(i)._2 * w1) + (user_sim_tuples_ncr(i)._2 * w2) + (user_sim_tuples_act(i)._2 * w3)
      var total_sim = (sbjt_sim * w1) + (ncr_sim * w2) + (acting_sim * w3)
      println(total_sim)
      //
      // i += 1;

      //전체 유사도 구하기
      user_sim_tuples = user_sim_tuples :+ (s"${stdNO}", total_sim)

    })

    var user_sim_df = user_sim_tuples.toDF("STD_NO", "similarity")
    var user_sim_sbjt_df = user_sim_tuples_sbjt.toDF("STD_NO", "sbjt_similarity")
    var user_sim_ncr_df = user_sim_tuples_ncr.toDF("STD_NO", "ncr_similarity")
    var user_sim_acting_df = user_sim_tuples_act.toDF("STD_NO", "acting_similarity")


    var join_df_temp1 = user_sim_sbjt_df.join(user_sim_ncr_df, Seq("STD_NO"), "outer")
    var join_df_temp2 = join_df_temp1.join(user_sim_acting_df, Seq("STD_NO"), "outer")
    val user_sim_join_df = join_df_temp2.join(user_sim_df, Seq("STD_NO"), "outer")

    // setMongoDF_USER_SIM(spark, join_df)

    MongoSpark.save(
    user_sim_join_df.write
        .option("spark.mongodb.output.uri", "mongodb://127.0.0.1/cpmongo_distinct.USER_SIMILARITY")
        .mode("overwrite"))


//============================유사도(연희,소민) end=======================================



//============================추천결과생성(보경,도웅) start=======================================


//유사도+신뢰도 join

val user_Result_df = user_trust_df.join(user_sim_df, Seq("STD_NO"), "outer")
val user_Result_df_NaN = user_Result_df.na.fill(0.0, List("SIMILARITY")).na.fill(0.0, List("TRUST"))

//val newDf = user_Result_df_NaN.select(col("SIMILARITY").map(col("TRUST")).reduce((c1, c2) => c1 + c2) as "sum")

//유사도+신뢰도 join DF->list변환

val a = double2Double(1)
val b = double2Double(1)
val w1 = 0.5
val w2 = 0.5

val res_ex_str = "\\[|\\]"
val user_Result1 = user_Result_df_NaN.select("STD_NO", "SIMILARITY", "TRUST").collect.map(_.toString.replaceAll(res_ex_str, "")).map{ row =>
  val x = row.split(",")
  val stdNo = x(0)
  val sim = x(1).toDouble * a * w1
  val tru = x(2).toDouble * b * w2
  (stdNo, sim, tru, sim + tru)
}.sortBy(x=> x._4).reverse


//유사도 + 신뢰도결합 최종 추천학생 list
val std_arr = user_Result1.map(x=>x._1).toSeq.toList.take(10)

// 교과목수료 테이블 중 학번, 학과, 교과목번호, 과목명

val clPassUri_table =  getMongoDF(spark, clPassUri) //교과목 수료(class pass)

//유사도팀이랑 합치면 없애야되는부분 => 중복제거하면 값 1개만불러와짐
var clPassUri_DF = clPassUri_table.select(col("STD_NO"), col("SUST_CD_NM"), col("SBJT_KOR_NM"), col("SBJT_KEY_CD")).distinct.toDF

//한학생이 들은 수업리스트(질의자)
var std_NO = 20152611
var sbjtNM_by_stdNO = clPassUri_DF.filter(clPassUri_DF("STD_NO").equalTo(s"${std_NO}")).select(col("SBJT_KEY_CD")).distinct
var sbjtNM_by_stdNO_List = sbjtNM_by_stdNO.rdd.map(r=>r(0)).collect.toList.distinct.map(_.toString)
///////////////////////////////////////

//val std_arr = Seq(20190030, 20170063, 20142915, 20152634, 20142824, 20161627)

val std_list = std_arr.map{ stdno =>
  val res = clPassUri_DF.filter(clPassUri_DF("STD_NO").equalTo(s"${stdno}"))
  res
}.map{ x =>
  val res = x.select("SBJT_KEY_CD").collect.map( x=> x.toString)
  res
}.flatMap( x=> x)

//질의학생이 들은 과목 중복제거
val exStr = "\\[|\\]"
val t2 = std_list.map(x => x.replaceAll(exStr, ""))
val std_filter = t2.diff(sbjtNM_by_stdNO_List)

val std_sbjt_arr = std_filter.groupBy(x => x).mapValues(_.length).toList.sortBy(x => x._2).reverse

//콘텐츠 신뢰도 DF->list변환(reduceDF_con1)
import org.apache.spark.sql.functions.collect_list
var reducedDF_con1 = con1.select("STAR_KEY_ID", "STAR_POINT").distinct()

//콘텐츠 신뢰도 형변환
val reducedDF_con2 = reducedDF_con1.groupBy("STAR_KEY_ID").agg(collect_list($"STAR_POINT").as("STAR_POINT")).rdd.map(row => (row(0).toString -> row(1).toString)).collect.map{ x =>
  val sbjt = x._1
  val starPoint = x._2.slice(13,x._2.length-1).toDouble
  (sbjt, starPoint)
}

//콘텐츠신뢰도 STAR_KEY_ID와 교과목수료 SBJT_KEY_CD일치 여부에 따라 값 곱하기
val res_arr = std_sbjt_arr.map { x =>
  val sbjt = x._1
  val filtered_sbjt_arr = reducedDF_con2.filter( x=> x._1 == sbjt)
  val v = if(filtered_sbjt_arr.isEmpty) x._2
  else filtered_sbjt_arr(0)._2 * x._2
  (sbjt, v)
}

//교과목 결과값에 콘텐츠 신뢰도를 곱해 재 랭킹
val PassUri_top5_list = res_arr.sortBy(x => x._2).reverse


//비교과 신청학생 테이블 중 학번, 비교과 프로그램 학생키아이디, 비교과 프로그램 학생키아이디, 과목명
//NPS_STATE 코드 (NCR_T07_P00 : 대기신청, NCR_T07_P01 : 승인대기, NCR_T07_P02 :승인, NCR_T07_P03 : 학생취소,
//NCR_T07_P04 : 관리자취소, NCR_T07_P05 : 이수, NCR_T07_P06 : 미이수, NCR_T07_P07 : 반려)
val ncrStdInfoUri_table =  getMongoDF(spark, ncrStdInfoUri)

var ncrInfoUri_DF = ncrStdInfoUri_table.select(col("NPS_STD_NO"), col("NPS_KEY_ID"), col("NPI_KEY_ID"), col("NPS_STATE")).distinct.toDF
ncrInfoUri_DF.show()

//한학생이 들은 비교과리스트
var std_NO = 20152611
var ncrStdInfoUri_stdNO = ncrInfoUri_DF.filter(ncrInfoUri_DF("NPS_STD_NO").equalTo(s"${std_NO}")).select(col("NPI_KEY_ID")).distinct
var ncrStdInfoUri_stdNO_List = ncrStdInfoUri_stdNO.rdd.map(r=>r(0)).collect.toList.distinct.map(_.toString)
////////////////////////////////////////////////////////////////////////////////////////////

val ncrStdInfoUri_arr = std_arr.map{ stdno =>
  val res = ncrInfoUri_DF.filter(ncrInfoUri_DF("NPS_STD_NO").equalTo(s"${stdno}")).filter($"NPS_STATE" === "NCR_T07_P05")
  res
}.map{ x =>
  val res = x.select("NPI_KEY_ID").collect.map( x=> x.toString)
  res
}.flatMap( x=> x)

//질의학생이 들은 과목 중복제거
val exStr = "\\[|\\]"
val ncr_replace = ncrStdInfoUri_arr.map(x => x.replaceAll(exStr, ""))
val ncr_filter = ncr_replace.diff(ncrStdInfoUri_stdNO_List)

val ncr_filter_arr = ncr_filter.groupBy(x => x).mapValues(_.length).toList.sortBy(x => x._2).reverse

//콘텐츠신뢰도 STAR_KEY_ID와 비교과 NPI_KEY_ID 일치 여부에 따라 값 곱하기
val res_arr2 = ncr_filter_arr.map { x =>
  val ncr = x._1
  val filtered_ncrStdInfoUri = reducedDF_con2.filter( x=> x._1 == ncr)
  val v = if(filtered_ncrStdInfoUri.isEmpty) x._2
  else filtered_ncrStdInfoUri(0)._2 * x._2
  (ncr, v)
}


//-------------------- # # # 자율활동 리스트 # # # ------------------------------
//from.교외활동 CPS_OUT_ACTIVITY_MNG : 학번(OAM_STD_NO), 활동구분코드(OAM_TYPE_CD), 활동명(OAM_TITLE)
//자격증(CD01) : 이름(OAM_TITLE) / ex. 토익800~900, FLEX 일본어 2A,  FLEX 일본어 1A,  FLEX 중국어 1A
//어학(CD02) : 이름(OAM_TITLE)
//봉사(CD03), 대외활동(CD04), 기관현장실습(CD05) : 활동구분코드(OAM_TYPE_CD)

val outActUri_table =  getMongoDF(spark, outActUri)  //교외활동
val clPassUri_table =  getMongoDF(spark, clPassUri) //교과목 수료(class pass)
// 자율활동 테이블 중 학번, 학과, 교과목번호, 과목명
var outActUri_DF = outActUri_table.select(col("OAM_STD_NO"), col("OAM_TYPE_CD"), col("OAM_TITLE"))
outActUri_DF.show()

//유사도팀
var outAct_name_temp1 = outActUri_DF.filter(outActUri_DF("OAM_STD_NO").equalTo(s"${std_NO}")).select(col("OAM_STD_NO"), col("OAM_TITLE"), col("OAM_TYPE_CD")).distinct
//3개의 코드만 필터링
var outAct_name_temp2 = outAct_name_temp1.drop("OAM_STD_NO", "OAM_TYPE_CD").filter($"OAM_TYPE_CD" === "OAMTYPCD01" || $"OAM_TYPE_CD" ==="OAMTYPCD02").distinct
var outAct_name_List = outAct_name_temp2.rdd.map(r=>r(0)).collect.toList

//---------------------자율활동 추천 code list(자격증 CD01, 어학 CD02)----------------------


val outActUri_CD01_arr = std_arr.map{ stdno =>
  val res = outActUri_DF.filter(outActUri_DF("OAM_STD_NO").equalTo(s"${stdno}"))
  res
}.map{ x =>
  val res = x.select("OAM_TITLE").filter($"OAM_TYPE_CD" === "OAMTYPCD01").collect.toList.map( x=> x.toString)
  res
}.flatMap( x=> x).groupBy(x => x).mapValues(_.length).toList.sortBy(x => x._2).reverse


val outActUri_CD02_arr = std_arr.map{ stdno =>
  val res = outActUri_DF.filter(outActUri_DF("OAM_STD_NO").equalTo(s"${stdno}"))
  res
}.map{ x =>
  val res = x.select("OAM_TITLE").filter($"OAM_TYPE_CD" ==="OAMTYPCD02").collect.toList.map( x=> x.toString)
  res
}.flatMap( x=> x).groupBy(x => x).mapValues(_.length).toList.sortBy(x => x._2).reverse


//---------------------자율활동 추천 code list(봉사 CD03, 대외활동 CD04, 기관현장실습 CD05)----------------------

val outActUri_CD03 = std_arr.map{ stdno =>
  val res = outActUri_DF.filter(outActUri_DF("OAM_STD_NO").equalTo(s"${stdno}"))
  res
}.map{ x =>
  val res = x.select("OAM_TYPE_CD").filter($"OAM_TYPE_CD" === "OAMTYPCD03").collect.toList
  res
}.flatMap( x=> x).groupBy(x => x).mapValues(_.length).toList.sortBy(x => x._2).reverse

val outActUri_CD03_result = outActUri_CD03.map(x => x._2)

case class OAM_TYPE_CD(OAM_TYPE_CD: Int, count : Int)
val outActUri_CD03_arr = outActUri_CD03_result.map{ row =>
  val avg = row / std_arr.length
  val res3 = ("OAM_TYPE_CD03", avg)
  res3
}

val outActUri_CD04 = std_arr.map{ stdno =>
  val res = outActUri_DF.filter(outActUri_DF("OAM_STD_NO").equalTo(s"${stdno}"))
  res
}.map{ x =>
  val res = x.select("OAM_TYPE_CD").filter($"OAM_TYPE_CD" ==="OAMTYPCD04").collect.toList
  res
}.flatMap( x=> x).groupBy(x => x).mapValues(_.length).toList.sortBy(x => x._2).reverse

val outActUri_CD04_result = outActUri_CD04.map(x => x._2)

case class OAM_TYPE_CD(OAM_TYPE_CD: Int, count : Int)
val outActUri_CD04_arr = outActUri_CD04_result.map{ row =>
  val avg = row / std_arr.length
  val res3 = ("OAM_TYPE_CD04", avg)
  res3
}

val outActUri_CD05 = std_arr.map{ stdno =>
  val res = outActUri_DF.filter(outActUri_DF("OAM_STD_NO").equalTo(s"${stdno}"))
  res
}.map{ x =>
  val res = x.select("OAM_TYPE_CD").filter($"OAM_TYPE_CD" ==="OAMTYPCD05").collect.toList
  res
}.flatMap( x=> x).groupBy(x => x).mapValues(_.length).toList.sortBy(x => x._2).reverse

val outActUri_CD05_result = outActUri_CD05.map(x => x._2)

case class OAM_TYPE_CD(OAM_TYPE_CD: Int, count : Int)
val outActUri_CD05_arr = outActUri_CD05_result.map{ row =>
  val avg = row / std_arr.length
  val res3 = ("OAM_TYPE_CD05", avg)
  res3
}

////---------------------------------교과(sbjt_df), 비교과(ncr_df), 자율활동(act_df) join (Key값 필요) -------------------------------

val maxSize = 5
var colSet = scala.collection.mutable.Set[String]()

val PassUri_top5 = res_arr.sortBy(x => x._2).reverse.take(maxSize).zipWithIndex.map(x => (x._2 + 1, x._1._1)).toDF("Rank","SBJT_KEY_CD")
val NCR_std_Info_top5 = res_arr2.sortBy(x => x._2).reverse.take(maxSize).zipWithIndex.map(x => (x._2 + 1, x._1._1)).toDF("Rank","NPI_KEY_ID")
val CD01_top5 = outActUri_CD01_arr.take(maxSize).zipWithIndex.map(x => (x._2 + 1, x._1._1)).toDF("Rank","OAMTYPCD01")
val CD02_top5 = outActUri_CD02_arr.take(maxSize).zipWithIndex.map(x => (x._2 + 1, x._1._1)).toDF("Rank","OAMTYPCD02")
val CD03_avg = outActUri_CD03_arr.zipWithIndex.map(x => (x._2 + 1, x._1._2)).toDF("Rank", "OAMTYPCD03_AVG")
val CD04_avg = outActUri_CD04_arr.zipWithIndex.map(x => (x._2 + 1, x._1._2)).toDF("Rank", "OAMTYPCD04_AVG")
val CD05_avg = outActUri_CD05_arr.zipWithIndex.map(x => (x._2 + 1, x._1._2)).toDF("Rank", "OAMTYPCD05_AVG")

//최종적으로 join해서 합치기
val rankList = Seq(NCR_std_Info_top5, CD01_top5, CD02_top5, CD03_avg, CD04_avg, CD05_avg)

var Result_All = PassUri_top5
rankList.foreach{ DF =>
  Result_All = Result_All.join(DF, Seq("Rank"), "outer")
}
Result_All.sort("Rank").show

setMongoDF_result(spark, Result_All)

//============================추천결과생성(보경,도웅) end=======================================


