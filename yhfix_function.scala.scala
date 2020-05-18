import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import org.apache.spark.sql.Row
import spark.implicits._


var stdNo = 20152611
def calSim (spark:SparkSession, stdNo: Int) : DataFrame = {
//============================유사도(연희,소민)start=======================================

//-------------------- # # # 교과목 리스트 # # # --------------------------------
//--------------------from. 교과목수료 테이블 : 학과명, 학번, 학점----------------------
//<학과 DataFrame> : departDF / 전체 학과의 모든 학생

var std_NO = 20152611

// 2-1. 학과
var clPassUri_DF = clPassUri_table.select(col("STD_NO"), col("SUST_CD_NM"), col("SBJT_KOR_NM"), col("SBJT_KEY_CD")).distinct.toDF
var departNM_by_stdNO = clPassUri_DF.filter(clPassUri_DF("STD_NO").equalTo(s"${std_NO}")).select(col("SUST_CD_NM")).distinct
var departNM = departNM_by_stdNO.collect().map(_.getString(0)).mkString("")

// 2-2. 학과 학생 리스트
var stdNO_in_departNM_sbjt = clPassUri_DF.filter(clPassUri_DF("SUST_CD_NM").equalTo(s"${departNM}")).select(col("STD_NO"))
var stdNO_in_departNM_List = stdNO_in_departNM_sbjt.rdd.map(r=>r(0)).collect.toList.distinct

// 3-1. 학생의 수업 리스트
//^^
// var sbjtNM_by_stdNO = clPassUri_DF.filter(clPassUri_DF("STD_NO").equalTo(s"${std_NO}")).select(col("SBJT_KEY_CD"))
// var sbjtNM_by_stdNO_List = sbjtNM_by_stdNO.rdd.map(r=>r(0)).collect.toList.distinct

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

var arr01 = Array(20142820, 20142932, 20152611)

case class starPoint(sbjtCD:String, starpoint:Any)

var sbjtCD_star_byStd_Map = arr01.flatMap{ stdNO =>
  // 학번 별 학번-교과코드-별점
  val star_temp_bystdNO_DF = cpsStarUri_sbjt_DF.filter(cpsStarUri_sbjt_DF("STD_NO").equalTo(s"${stdNO}"))

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

var cpsStarUri_DF_ncr = cpsStarUri_table.select(col("STD_NO"), col("STAR_KEY_ID").as("NPI_KEY_ID"), col("STAR_POINT"), col("TYPE"))
// from. 비교과 관련 테이블(CPS_NCR_PROGRAM_INFO) : 비교과id(NPI_KEY_ID), 중분류(NPI_AREA_SUB_CD)
var ncrInfoUri_DF = ncrInfoUri_table.select(col("NPI_KEY_ID"), col("NPI_AREA_SUB_CD"))

// 비교과 별점 => 별점 테이블에서 "TYPE"이 "N" :: ex) NCR000000000677
var cpsStarUri_DF_ncr_typeN = cpsStarUri_DF_ncr.filter(cpsStarUri_DF_ncr("TYPE").equalTo("N"))
// 교과 별점 => 별점 테이블에서 "TYPE"이 "C" :: ex) BAQ00028

val schema3 = StructType(
    StructField("STAR_POINT", StringType, true) ::
    StructField("NPI_KEY_ID", StringType, true) :: Nil)
var star_subcd_DF = spark.createDataFrame(sc.emptyRDD[Row], schema3)

val schema4 = StructType(
    StructField("NPI_AREA_SUB_CD", StringType, true) :: Nil)
var subcd_byStd_DF = spark.createDataFrame(sc.emptyRDD[Row], schema4)

//----------------------------------------------------------------------------------------------------------------------------

//-----------------------------------------------<학과의 비교과중분류 리스트 생성>------------------------------------------------
var clPassUri_DF_ncr = clPassUri_table.select(col("SUST_CD_NM"), col("STD_NO")).distinct.toDF

//map연산은 dataframe에 쓸 수 없기 때문에 list로 변환해야 하며 dataframe을 list로 변환하려면 df의 값 하나하나에 접근하기 위해 map 연산이 필요함
//광홍과df(clpass 교과목 수료 테이블에서 학과 별 학번 dataframe을 생성한 뒤 list로 변환)

// var departNM = "컴퓨터공학과"
var stdNO_in_departNM_ncr = clPassUri_DF_ncr.filter(clPassUri_DF_ncr("SUST_CD_NM").equalTo(s"${departNM}")).select(col("STD_NO")).rdd.map(r=>r(0)).collect.toList

case class starPoint2(subcd:String, starpoint2:Any)

// Map 타입의 변수 (string, Array)를 인자로 받음
// String : 학번, Array : (중분류, 별점)
val subcd_star_byDepart_Map = collection.mutable.Map[String, Array[starPoint2]]()
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

  arr01.foreach{ stdNO =>
    var star_keyid_DF = spark.createDataFrame(sc.emptyRDD[Row], schema1)
    var subcd_keyid_DF = spark.createDataFrame(sc.emptyRDD[Row], schema2)

    val key_id_temp = cpsStarUri_DF_ncr_typeN.select(col("NPI_KEY_ID")).filter(cpsStarUri_DF_ncr("STD_NO").equalTo(s"${stdNO}"))
    val key_id_List_byStd = key_id_temp.rdd.map{r=> r(0)}.collect.toList

    val kidList = key_id_List_byStd.map { keyid =>

      //별점 테이블에서 모든 학생의 비교과키, 별점이 있는데 학번 하나에 대한 것만 가져옴 : 학번, 비교과키, 별점, 타입(N)
      var getStar_by_stdNO = cpsStarUri_DF_ncr_typeN.filter(cpsStarUri_DF_ncr("STD_NO").equalTo(s"${stdNO}")).toDF
      //비교과정보DF에서 모든 학생의 중분류, 비교과키가 있는데 학번 하나에 대한 것만 가져옴
      val subcd_keyid_DF_temp = ncrInfoUri_DF.select(col("NPI_AREA_SUB_CD"),col("NPI_KEY_ID")).filter(ncrInfoUri_DF("NPI_KEY_ID").equalTo(s"${keyid}"))
      //학번, 비교과키, 별점, 타입(N)이 있는 DF에서 학번 하나의 비교과키에 해당하는 별점, 비교과키만 가져오기
      val star_keyid_DF_temp = getStar_by_stdNO.select(col("STAR_POINT"),col("NPI_KEY_ID")).filter(getStar_by_stdNO("NPI_KEY_ID").equalTo(s"${keyid}"))

      star_keyid_DF = star_keyid_DF.union(star_keyid_DF_temp)
      subcd_keyid_DF = subcd_keyid_DF.union(subcd_keyid_DF_temp)

      val star_subcd_DF_temp = star_keyid_DF.join(subcd_keyid_DF, Seq("NPI_KEY_ID"), "left_outer")

      // agg 연산을 위해 필요없는 비교과 keyid를 지움!!
      val star_subcd_DF = star_subcd_DF_temp.drop("NPI_KEY_ID")
      // star_subcd_DF.show
      val star_subcd_avg_DF = star_subcd_DF.groupBy("NPI_AREA_SUB_CD").agg(avg("STAR_POINT"))
      // star_subcd_avg_DF.show

      val subcd_byStd_DF2 = star_subcd_DF.drop("STAR_POINT")

      subcd_byStd_DF = subcd_byStd_DF.union(subcd_byStd_DF2)

      val subcd_star_temp = star_subcd_avg_DF.collect.map{ row =>
        val str = row.toString
        val size = str.length
        val res = str.substring(1, size-1).split(",")
        val starP = starPoint2(res(0), res(1))
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

    }

   val t1 = subcd_byDepart_Map_temp.map(x => x._2).flatMap(x => x).toList.distinct
   val xres = s"result stdno : ${stdNO} size : ${t1.length} ==--------------------> ${t1}"
   myResStr = myResStr.concat("\n"+xres)

  subcd_byDepart_List = t1
} //학번 루프 끝

  // subcd_star_byDepart_Map
//----------------------------------------------------------------------------------------------------------------------------

//최종적인 학번 별 별점 리스트 값이 들어있는 시퀀스
var ncr_tuples = Seq[(String, List[Double])]()

for(s<-0 until subcd_star_byDepart_Map.size){ // 학과 학생 학번 List 를 for문
  var star_point_List = List[Any]() // 학번당 별점을 저장
  var orderedIdx_byStd = List[Int]() //학번 당 중분류 리스트
  //학과 전체 중분류 코드 List => 학번당 별점을 중분류 갯수만금 0.0으로 초기화
  for(i<-0 until subcd_byDepart_List.size){
    star_point_List = 0.0::star_point_List
  }

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
    star_point_List = star_point_List.updated(orderedIdx_byStd(i), valueBystdNo_from_Map(k).starpoint2)
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

var outActUri_DF = outActUri_table.select(col("OAM_STD_NO"), col("OAM_TYPE_CD"), col("OAM_TITLE"))
var clPassUri_DF_act = clPassUri_table.select(col("SUST_CD_NM"), col("STD_NO")).distinct.toDF
//map연산은 dataframe에 쓸 수 없기 때문에 list로 변환해야 하며 dataframe을 list로 변환하려면 df의 값 하나하나에 접근하기 위해 map 연산이 필요함

var stdNO_in_departNM_act = clPassUri_DF_act.filter(clPassUri_DF_act("SUST_CD_NM").equalTo(s"${departNM}")).select(col("STD_NO")).rdd.map(r=>r(0)).collect.toList.map(_.toString)

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

  //학과 리스트를 돌면서 일치 여부 세는데
  //봉사03, 대외04, 기관05 = 횟수 count
  //자격증01, 어학02 = 유무(1 또는 0)

 // activity_List_byStd = outAct_code_List ++ activity_List_byStd_temp3
 activity_List_byStd = head ++ activity_List_byStd_temp3

 val act_list = activity_List_byStd.map(x => x.toString.toInt)

 act_tuples = act_tuples :+ (stdNO, act_list)
}

var act_df = act_tuples.toDF("STD_NO", "ACTING_COUNT")

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
    .mode("overwrite")
  )

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
        .mode("overwrite")
      )
//============================유사도(연희,소민) end=======================================
user_sim_join_df
}
