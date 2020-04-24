
//-------------------- # # # 비교과 리스트 # # # --------------------------------
// from. 교과/비교과용 별점 테이블(CPS_STAR_POINT) : 학번(STD_NO), 비교과id(STAR_KEY_ID), 별점(STAR_POINT)
// from. 비교과 관련 테이블(CPS_NCR_PROGRAM_INFO) : 비교과id(NPI_KEY_ID), 중분류(NPI_AREA_SUB_CD)

//비교과 아이디(학번, 비교과id 사용 from.교과/비교과별점테이블)로 중분류 가져오기(비교과id, 중분류 from.비교과 관련 테이블)

//학과 별 학생들이 수강한 비교과의 중분류 list 로 포맷 잡고 : 학과 - 학번 돌면서 list 만들고 , 중분류로 바꿔주기
//학생 한명이 수강한 비교과 list -> 별점 가져오기(from. 교과/비교과 별점 테이블) -> 중분류 가져오기 -> 중분류 별 별점 avg 계산


// ---------------------------------------------------------------------------
// from. 비교과 테이블 : NCR~ ID를 이용해 별점 테이블에 데이터를 생성해줌 !!!!***
// var test = ncrInfoUri_DF.select(col("NPI_KEY_ID"),col("NPI_AREA_SUB_CD")).filter(ncrInfoUri_DF("NPI_KEY_ID").equalTo("NCR000000000718"))
var ncrInfoUri_DF = ncrInfoUri_table.select(col("NPI_KEY_ID"), col("NPI_AREA_SUB_CD"))
var get_NPI_KEY_ID = ncrInfoUri_DF.select(col("NPI_KEY_ID"),col("NPI_AREA_SUB_CD"))
//<학생 DataFrame> / 하나의 학과의 학생 => 지금 컴퓨터공학과 학생을 조회했으니까 컴퓨터공학과 학번을 조회해서 별점 테이블에 데이터를 삽입해줌
// var departNM = "컴퓨터공학과"
//from. 교과 수료 테이블 : departNM에 담긴 학과의 학생들의 학번을 가져옴
var students_in_departNM = clPassUri_DF.filter(clPassUri_DF("SUST_CD_NM").equalTo(s"${departNM}")).select(col("STD_NO"))
students_in_departNM.show
// ---------------------------------------------------------------------------
// 컴퓨터 공학과 학생 4명에 대해 별점 테이블 데이터 추가함 : |20142820||20142932|   |20152611| |20152615|
// ---------------------------------------------------------------------------


// from. 교과/비교과용 별점 테이블(CPS_STAR_POINT) : 학번(STD_NO), 비교과id(STAR_KEY_ID), 별점(STAR_POINT), 타입(TYPE)
var cpsStarUri_DF = cpsStarUri_table.select(col("STD_NO"), col("STAR_KEY_ID"), col("STAR_POINT"), col("TYPE"))
// from. 비교과 관련 테이블(CPS_NCR_PROGRAM_INFO) : 비교과id(NPI_KEY_ID), 중분류(NPI_AREA_SUB_CD)
var ncrInfoUri_DF = ncrInfoUri_table.select(col("NPI_KEY_ID"), col("NPI_AREA_SUB_CD"))

// 비교과 별점 => 별점 테이블에서 "TYPE"이 "N" :: ex) NCR000000000677
var cpsStarUri_ncr_DF = cpsStarUri_DF.filter(cpsStarUri_DF("TYPE").equalTo("N"))
// 교과 별점 => 별점 테이블에서 "TYPE"이 "C" :: ex) BAQ00028
var cpsStarUri_sbjt_DF = cpsStarUri_DF.filter(cpsStarUri_DF("TYPE").equalTo("C"))

var departNM = "컴퓨터공학과"
var std_NO1 = 20190030

var std_NO2 = 20142820 // 6개
// 프로그램 id : NCR000000000694, NCR000000000723, NCR000000000679, NCR000000000731, NCR000000000671, NCR000000000780
// 중분류 code : NCR_T01_P03_C01, NCR_T01_P01_C01, NCR_T01_P03_C03, NCR_T01_P04_C03, NCR_T01_P02_C03, NCR_T01_P02_C03
var std_NO3 = 20142932 // 5게
// 프로그램 id : NCR000000000694, NCR000000000723, NCR000000000731, NCR000000000737, NCR000000000743
// 중분류 code : NCR_T01_P03_C01, NCR_T01_P01_C01, NCR_T01_P04_C03, NCR_T01_P05_C02, NCR_T01_P01_C03
var std_NO4 = 20152611 // 5개
// 프로그램 id : NCR000000000737, NCR000000000743, NCR000000000748, NCR000000000723, NCR000000000716
// (20142820, 20142932, 20152611)

// 학과별 중분류 code (distinct)
// NCR_T01_P03_C01, NCR_T01_P01_C01, NCR_T01_P03_C03, NCR_T01_P04_C03, NCR_T01_P05_C02, NCR_T01_P01_C03, NCR_T01_P02_C03,
// List(
//   NCR_T01_P01_C01 NCR_T01_P02_C03 NCR_T01_P03_C01 NCR_T01_P03_C03 NCR_T01_P04_C03,
//   NCR_T01_P01_C01 NCR_T01_P02_C03 NCR_T01_P03_C01 NCR_T01_P03_C03 NCR_T01_P04_C03,
//   NCR_T01_P01_C01 NCR_T01_P02_C03 NCR_T01_P03_C01 NCR_T01_P03_C03 NCR_T01_P04_C03)

// List(NCR_T01_P01_C01 NCR_T01_P03_C01 NCR_T01_P04_C03,
//      NCR_T01_P01_C01 NCR_T01_P02_C03 NCR_T01_P02_C03 NCR_T01_P03_C01 NCR_T01_P03_C03 NCR_T01_P04_C03,
//      NCR_T01_P01_C01 NCR_T01_P03_C01 NCR_T01_P04_C03)
//
// List(NCR_T01_P01_C01 NCR_T01_P02_C03 NCR_T01_P02_C03 NCR_T01_P03_C01 NCR_T01_P03_C03 NCR_T01_P04_C03,
//      NCR_T01_P01_C01 NCR_T01_P02_C03 NCR_T01_P02_C03 NCR_T01_P03_C01 NCR_T01_P03_C03 NCR_T01_P04_C03,
//      NCR_T01_P01_C01 NCR_T01_P02_C03 NCR_T01_P02_C03 NCR_T01_P03_C01 NCR_T01_P03_C03 NCR_T01_P04_C03)
//
// List(NCR_T01_P01_C01 NCR_T01_P03_C01 NCR_T01_P04_C03,
//   NCR_T01_P01_C01 NCR_T01_P02_C03 NCR_T01_P02_C03 NCR_T01_P03_C01 NCR_T01_P03_C03 NCR_T01_P04_C03)


// List(NCR_T01_P01_C01,NCR_T01_P03_C01,NCR_T01_P04_C03,
//   NCR_T01_P01_C01,NCR_T01_P02_C03,NCR_T01_P02_C03,NCR_T01_P03_C01,NCR_T01_P03_C03,NCR_T01_P04_C03,
//   NCR_T01_P01_C01,NCR_T01_P03_C01,NCR_T01_P04_C03)
// List(NCR_T01_P01_C01,NCR_T01_P03_C01,NCR_T01_P04_C03,
//   NCR_T01_P01_C01,NCR_T01_P02_C03,NCR_T01_P02_C03,NCR_T01_P03_C01,NCR_T01_P03_C03,NCR_T01_P04_C03)


// from. 교과목수료 테이블 : 학과명, 학번 => 질의를 내린 학생의 학과를 찾기 위해 사용
var clPassUri_DF = clPassUri_table.select(col("SUST_CD_NM"), col("STD_NO")).distinct.toDF

//학번으로 학생의 학과 찾기 (dataframe) => 컴퓨터공학과 학생
var showDepart_by_stdNO = clPassUri_DF.filter(clPassUri_DF("STD_NO").equalTo(s"${std_NO2}"))

//학번으로 별점 가져오기 (dataframe) => 학생 한 명에 대한 별점
var getStar_by_stdNO = cpsStarUri_DF.filter(cpsStarUri_DF("STD_NO").equalTo(s"${std_NO2}")).toDF

// from. 비교과 신청학생 테이블 : 학번, 비교과 KEYID
var ncrStdInfoUri_DF = ncrStdInfoUri_table.select(col("NPS_STD_NO"), col("NPI_KEY_ID"))

//---------------------------<학생 한명의 비교과중분류 별 별점평균 데이터프레임 생성>----------------------------------------
// 학생 한 명에 대해서 별점테이블을 조회해서 교과/비교과 관련 활동 KEY_ID를 가져옴 => List 생성
// var key_id_temp = cpsStarUri_ncr_DF.select(col("STAR_KEY_ID")).filter(cpsStarUri_DF("STD_NO").equalTo(s"${std_NO2}"))
// var key_id_List_byStd = key_id_temp.rdd.map(r=>r(0)).collect.toList

//학생 한명에 대한 중분류별 별점 평균 리스트 만들기 (star_subcd_avg_DF를 list로 변환)

import org.apache.spark.sql.types.{
    StructType, StructField, StringType, IntegerType}
import org.apache.spark.sql.Row

val schema1 = StructType(
    StructField("STAR_POINT", StringType, true) ::
    StructField("STAR_KEY_ID", StringType, true) :: Nil)
var star_keyid_DF = spark.createDataFrame(sc.emptyRDD[Row], schema1)

val schema2 = StructType(
    StructField("NPI_AREA_SUB_CD", StringType, true) ::
    StructField("NPI_KEY_ID", StringType, true) :: Nil)
var subcd_keyid_DF = spark.createDataFrame(sc.emptyRDD[Row], schema2)

val schema3 = StructType(
    StructField("STAR_POINT", StringType, true) ::
    StructField("NPI_KEY_ID", StringType, true) :: Nil)
var star_subcd_DF = spark.createDataFrame(sc.emptyRDD[Row], schema3)

val schema4 = StructType(
    StructField("NPI_AREA_SUB_CD", StringType, true) :: Nil)
var subcd_byStd_DF = spark.createDataFrame(sc.emptyRDD[Row], schema4)

val schema5 = StructType(
    StructField("NPI_AREA_SUB_CD", StringType, true) :: Nil)
var subcd_byDepart_DF = spark.createDataFrame(sc.emptyRDD[Row], schema5)


//----------------------------------------------------------------------------------------------------------------------------

//-----------------------------------------------<학과의 비교과중분류 리스트 생성>------------------------------------------------
val clPassUri_DF = clPassUri_table.select(col("SUST_CD_NM"), col("STD_NO")).distinct.toDF
val ncrInfoUri_DF = ncrInfoUri_table.select(col("NPI_KEY_ID"), col("NPI_AREA_SUB_CD"))
val students_in_departNM = clPassUri_DF.filter(clPassUri_DF("SUST_CD_NM").equalTo(s"${departNM}")).select(col("STD_NO"))
//map연산은 dataframe에 쓸 수 없기 때문에 list로 변환해야 하며 dataframe을 list로 변환하려면 df의 값 하나하나에 접근하기 위해 map 연산이 필요함

//광홍과df(clpass 교과목 수료 테이블에서 학과 별 학번 dataframe을 생성한 뒤 list로 변환)
val stdNO_in_departNM = clPassUri_DF.filter(clPassUri_DF("SUST_CD_NM").equalTo(s"${departNM}")).select(col("STD_NO")).rdd.map(r=>r(0)).collect.toList

case class starPoint(subcd:String, starpoint:Any)

// Map 타입의 변수 (string, Array)를 인자로 받음
// String : 학번, Array : (중분류, 별점)
val subcd_star_byStd_Map = collection.mutable.Map[String, Array[starPoint]]()
val subcd_byDepart_Map_temp = collection.mutable.Map[String, Array[String]]()
var subcd_byDepart_List = List[Any]()

// 학과별 중분류 중복 제거를 위해 Set으로 데이터타입 선언
val tmp_set = scala.collection.mutable.Set[String]()

var myResStr = ""
var star_keyid_DF = spark.createDataFrame(sc.emptyRDD[Row], schema1)
var subcd_keyid_DF = spark.createDataFrame(sc.emptyRDD[Row], schema2)

val arr01 = Array(20142820, 20142932, 20152611)

val stdNO_ncr_temp1 = Array(20142820, 20142932, 20152611)
val stdNO_ncr_temp2 = stdNO_sbjt_temp1.toList.map(_.toString)

arr01.foreach{ stdNO =>
  val key_id_temp = cpsStarUri_ncr_DF.select(col("STAR_KEY_ID")).filter(cpsStarUri_DF("STD_NO").equalTo(s"${stdNO}"))
  val key_id_List_byStd = key_id_temp.rdd.map{r=> r(0)}.collect.toList

  val kidList = key_id_List_byStd.map { keyid =>
    // var std_NO3 = 20142932 // 5게
    // 프로그램 id : NCR000000000694, NCR000000000723, NCR000000000731, NCR000000000737, NCR000000000743
    // 중분류 code : NCR_T01_P03_C01, NCR_T01_P01_C01, NCR_T01_P04_C03, NCR_T01_P05_C02, NCR_T01_P01_C03
    var getStar_by_stdNO = cpsStarUri_DF.filter(cpsStarUri_DF("STD_NO").equalTo(s"${stdNO}")).toDF
    val subcd_keyid_DF_temp = ncrInfoUri_DF.select(col("NPI_AREA_SUB_CD"),col("NPI_KEY_ID")).filter(ncrInfoUri_DF("NPI_KEY_ID").equalTo(s"${keyid}"))
    val star_keyid_DF_temp = getStar_by_stdNO.select(col("STAR_POINT"),col("STAR_KEY_ID")).filter(getStar_by_stdNO("STAR_KEY_ID").equalTo(s"${keyid}"))

    star_keyid_DF = star_keyid_DF.union(star_keyid_DF_temp)
    subcd_keyid_DF = subcd_keyid_DF.union(subcd_keyid_DF_temp)

    val subcd_byStd_DF_temp = ncrInfoUri_DF.select(col("NPI_AREA_SUB_CD")).filter(ncrInfoUri_DF("NPI_KEY_ID").equalTo(s"${keyid}"))

    subcd_byStd_DF = subcd_byStd_DF.union(subcd_byStd_DF_temp)
    star_keyid_DF = star_keyid_DF.dropDuplicates("STAR_KEY_ID")
    subcd_keyid_DF = subcd_keyid_DF.dropDuplicates("NPI_KEY_ID")

    // val star_subcd_DF_temp = star_keyid_DF.join(subcd_keyid_DF, col("STAR_KEY_ID") === col("NPI_KEY_ID"), "inner")
    val star_subcd_DF_temp = star_keyid_DF.join(subcd_keyid_DF, col("STAR_KEY_ID") === col("NPI_KEY_ID"), "outer")
    val star_subcd_DF = star_subcd_DF_temp.drop("STAR_KEY_ID", "NPI_KEY_ID")
    val star_subcd_avg_DF = star_subcd_DF.groupBy("NPI_AREA_SUB_CD").agg(avg("STAR_POINT"))
    val subcd_byStd_DF2 = star_subcd_DF.drop("STAR_POINT")

    val subcd_star_temp = star_subcd_avg_DF.collect.map{ row =>
      val str = row.toString
      val size = str.length
      val res = str.substring(1, size-1).split(",")
      val starP = starPoint(res(0), res(1))
      starP
    }

    val subcd_star_record = (stdNO.toString, subcd_star_temp)
    // println(s"star 1 ==== ${subcd_star_record.mkString(",")} \n ====")
    subcd_star_byStd_Map+=(subcd_star_record)

    val subcd_byDepart_temp = subcd_byStd_DF2.collect.map{ row =>
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

    star_subcd_DF_temp.show // TO DEBUG

    // println("t1-------------------->" + t1)
 }

 val t1 = subcd_byDepart_Map_temp.map(x => x._2).flatMap(x => x).toList.distinct
 val xres = s"result stdno : ${stdNO} size : ${t1.length} ==--------------------> ${t1}"
 myResStr = myResStr.concat("\n"+xres)

subcd_byDepart_List = t1.sorted
} //학번 루프 끝


// 학과의 중분류 List
// Map(20142932 -> Array(starPoint(NCR_T01_P01_C03,null), starPoint(NCR_T01_P05_C02,null), starPoint(NCR_T01_P04_C03,3.8), starPoint(NCR_T01_P01_C01,4.5), starPoint(NCR_T01_P02_C03,3.85), starPoint(NCR_T01_P03_C03,4.0), starPoint(NCR_T01_P03_C01,4.2)),
// 20152611 -> Array(starPoint(NCR_T01_P01_C03,null), starPoint(NCR_T01_P05_C02,null), starPoint(NCR_T01_P04_C03,3.8), starPoint(NCR_T01_P04_C07,null), starPoint(NCR_T01_P01_C01,4.5), starPoint(NCR_T01_P02_C03,3.85), starPoint(NCR_T01_P03_C03,4.0), starPoint(NCR_T01_P03_C01,4.2)),
// 20142820 -> Array(starPoint(NCR_T01_P04_C03,3.8), starPoint(NCR_T01_P01_C01,4.5), starPoint(NCR_T01_P02_C03,3.85), starPoint(NCR_T01_P03_C03,4.0), starPoint(NCR_T01_P03_C01,4.2)))

println(subcd_byDepart_List)

// 학과의 모든 학번의 (중분류, 별점) Map
subcd_star_byStd_Map
//----------------------------------------------------------------------------------------------------------------------------

val arr01 = Array(20142820, 20142932, 20152611)
//
subcd_star_byStd_Map("20142932")(0).subcd
subcd_star_byStd_Map("20142932")(0).starpoint

//최종적인 학번 별 별점 리스트 값이 들어있는 시퀀스
var ncr_tuples = Seq[(String, List[Float])]()

for(s<-0 until subcd_star_byStd_Map.size){ // 학과 학생 학번 List 를 for문
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
  var valueBystdNo_from_Map = subcd_star_byStd_Map(arr01(s).toString)

  for(i<-0 until valueBystdNo_from_Map.size){
    //학생 한명의 중분류-별점 맵에서 중분류 키에 접근 : valueBystdNo_from_Map(i).subcd)
    //학생 한명이 들은 중분류 리스트를 가져옴
    orderedIdx_byStd = subcd_byDepart_List.indexOf(valueBystdNo_from_Map(i).subcd)::orderedIdx_byStd
    println("orderedIdx_byStd ===> " + orderedIdx_byStd)
  }
  // orderedIdx_byStd를 정렬(중분류 코드 정렬)
  orderedIdx_byStd = orderedIdx_byStd.sorted
  println("orderedIdx_byStdsorted ===>" + orderedIdx_byStd)

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
  val star_list = star_point_List.map(x => x.toString.toFloat)
  println(">>"+star_list)
  ncr_tuples = ncr_tuples :+ (arr01(s).toString, star_list)
}

var ncr_df = ncr_tuples.toDF("STD_NO", "RATING")
