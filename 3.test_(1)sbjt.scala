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
var std_NO = 20142820
// 컴공과 데이터 목록 |20142820||20142932| |20152611|
//--------------------from. 교과목수료 테이블 V_STD_CDP_SUBJECT : 학과이름, 학번, 수업명, 교과목코드-------------------------
// var studentNO = students_in_departNM.filter(students_in_departNM("STD_NO").equalTo(s"${std_NO}"))

// COLLUM NAMES ~~ //
// val STD_NO = TABLE_INFO.STD_NO
//
// ...
// COLLUM NAMES ~~ //

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

case class starPoint(sbjtCD:String, starpoint:Any)

val sbjtCD_star_byStd_Map = collection.mutable.Map[String, Array[starPoint]]()





//학생 하나의
var star_temp = starPoint(교과목코드, 별점)
//Map 형태로 학번이 교과목코드-별점을 가리키도록

학번 교과목코드 별점 DF
Map

var cpsStarUri_DF = cpsStarUri_table.select(col("STD_NO"), col("STAR_KEY_ID"), col("STAR_POINT"), col("TYPE"))
// 교과 별점 => 별점 테이블에서 "TYPE"이 "C" :: ex) BAQ00028

//STAR_KEY_ID(교과목코드, sbjtCD), STAR_POINT이 있는 dataframe
var cpsStarUri_sbjt_DF = cpsStarUri_DF.filter(cpsStarUri_DF("TYPE").equalTo("C")).drop("TYPE")

var sbjtCD_star_Array = Array[String, Double]
val sbjtCD_star_Array = cpsStarUri_sbjt_DF.rdd.map{r=> r._1, r._2}.collect

var stdNo_sbjtCD_star_DF1 =





/*
// --------------------------------------------별점 추가하기---------------------------------------------
// 컴퓨터 공학과 학생 4명에 대해 별점 테이블 데이터 추가함 : |20142820||20142932| |20152611| |20152615|
// 일단 지금은 이 학생들에 대핸 비교과 활동에 대한 별점을 추가했음
// 이 학생들의 교과 리스트를 가져와서 (Key) 그 Key에 해당하는 별점을 mongodb에 추가해줌

// |20142820||20142932| |20152611| /////////////// |20152615|
// 과목마다 같은 과목이여도 코드가 다를 수 있으므로 한번에 select를 하여 code를 가져와야함
var std_NO1 = 20142820 (6개)
var sbjtNM_by_stdNO1 = clPassUri_DF.filter(clPassUri_DF("STD_NO").equalTo(s"${std_NO1}")).select(col("SBJT_KOR_NM"), col("SBJT_KEY_CD")).show
// 네트워크프로그래밍, 시스템분석설계, 인공지능, 네트워크보안, 정보기술세미나, 서버구축및관리
// AAM00351, AAM00341, AAM00361, AAM00331, AAM00371, AAM00121

var std_NO2 = 20142932 (7개)
var sbjtNM_by_stdNO2 = clPassUri_DF.filter(clPassUri_DF("STD_NO").equalTo(s"${std_NO2}")).select(col("SBJT_KOR_NM"), col("SBJT_KEY_CD")).show
// 시스템분석설계, 네트워크보안, 일본어회화Ⅰ, 네트워크프로그래밍, "MOSⅡ ", 인공지능, 정보기술세미나
// AAM00341, AAM00331, TAA02231, AAM00351, TAA04151, AAM00361, AAM00371

var std_NO3 = 20152611 (7개)
var sbjtNM_by_stdNO3 = clPassUri_DF.filter(clPassUri_DF("STD_NO").equalTo(s"${std_NO3}")).select(col("SBJT_KOR_NM"), col("SBJT_KEY_CD")).show
// 컴퓨터구조, 취업연계현장실습Ⅰ, 인간중심감성공학의이해, 운영체제, 자바프로그래밍, 데이터베이스응용, 정보통신
// AAM00231, COM00051, BCD00017, AAM00191, AAM00241, AAM00201, AAM00211

// --------------------------------------------별점 추가하기 끝---------------------------------------------
*/



//==================================================일단 계산ㅇ ㅣ너무 느리니까 다른 방법으로,,=======================================
var sbjt_tuples = Seq[(String, String)]()

// 학번이 1452개 -> distinct 지정 -> 223 명
var stdNO_in_departNM = clPassUri_DF.filter(clPassUri_DF("SUST_CD_NM").equalTo(s"${departNM}")).select(col("STD_NO")).rdd.map(r=>r(0)).collect.toList.distinct

val stdNO_sbjt_temp1 = Array(20142820, 20142932, 20152611)
val stdNO_sbjt_temp2 = stdNO_sbjt_temp1.toList.map(_.toString)

stdNO_sbjt_temp2.foreach{ stdNO =>

  // 학생별로 (stdNO) 들은 교과목 테이블
  var student_have_sbjt_temp1 = sbjtCD_in_departNM.filter(sbjtCD_in_departNM("STD_NO").equalTo(s"${stdNO}"))
  // var student_have_sbjt_temp2 = student_have_sbjt_temp1.select(col("SBJT_KOR_NM"))
  var student_have_sbjt_temp2 = student_have_sbjt_temp1.select(col("SBJT_KEY_CD"))
  // student_have_sbjt_temp2.show

  // from. 교과/비교과용 별점 테이블(CPS_STAR_POINT) : 학번(STD_NO), 비교과id(STAR_KEY_ID), 별점(STAR_POINT), 타입(TYPE)
  var cpsStarUri_DF = cpsStarUri_table.select(col("STD_NO"), col("STAR_KEY_ID"), col("STAR_POINT"), col("TYPE"))

  // 비교과 별점 => 별점 테이블에서 "TYPE"이 "N" :: ex) NCR000000000677
  var cpsStarUri_ncr_DF = cpsStarUri_DF.filter(cpsStarUri_DF("TYPE").equalTo("N"))
  // 교과 별점 => 별점 테이블에서 "TYPE"이 "C" :: ex) BAQ00028
  var cpsStarUri_sbjt_DF = cpsStarUri_DF.filter(cpsStarUri_DF("TYPE").equalTo("C"))

  // 학번 별 과목 ID, 별점을 가져감

  // var getStar_by_stdNO = cpsStarUri_sbjt_DF.filter(cpsStarUri_sbjt_DF("STD_NO").equalTo(s"${20142820}"))
  // var star_key_id = "AAM00351"
  // var getStar = getStar_by_stdNO.filter(getStar_by_stdNO("STAR_KEY_ID").equalTo(s"${star_key_id}")).select(col("STAR_POINT"))
  // var temp = getStar.collect().map(_.getDouble(0)).mkString("")

  // 학과 전체 교과목 리스트를 순회 (교과, 0)으로 만들어놓음

  val getStar_List_temp1 = sbjtCD_in_departNM_List.map(x => (x, 0)).map{ record =>
    // println(s"stdNO : ${stdNO} ============= sbjtNM : ${record}")
    //
    //@@@ 컴퓨터공학과의 학생 한명이 수강한 수업 리스트를 생성
    //학과별 교과목코드와 학생의 교과목 코드를 인덱스 비교 하기 위해 정렬함(sorted)
    var student_have_sbjt_List = student_have_sbjt_temp2.select("SBJT_KEY_CD").rdd.map(r=>r(0).toString).collect.toList.distinct.sorted
    // println(s"student_have_sbjt_List--${student_have_sbjt_List}")
    //x : record_1
    //0 : record_2
    //isListend면 1로 바뀜
    val sbjtCD = record._1

    val getStar =
      if(student_have_sbjt_List.contains(sbjtCD)) {
        var getStar_by_stdNO = cpsStarUri_sbjt_DF.filter(cpsStarUri_sbjt_DF("STD_NO").equalTo(s"${stdNO}"))
        // var star_key_id = "AAM00351"

        //if문 추가 : 수강은 했는데 별점을 내리지 않은 경우에(-1) / 별점을 내린 경우(별점) / 수강하지 않은 경우(0)

        var getStar_temp1 = getStar_by_stdNO.filter(getStar_by_stdNO("STAR_KEY_ID").equalTo(s"${sbjtCD}")).select(col("STAR_POINT"))
        var getStar_temp2 = getStar_temp1.collect().map(_.getDouble(0)).mkString("")
        // println("-------------------")
        // println(temp)
        getStar_temp2
      }
      else 0

    val getStar_List_temp2 = (sbjtCD, getStar)
    // print(isListened_List_temp2)
    //리턴하려면 이름을 쳐야 함
    //최종적으로 isListened_List_temp1 = isListened_List_temp2 값이 담기는 것 !!
    getStar_List_temp2
  }

  val getStar_List = getStar_List_temp1.map(_._2).toString
  println(getStar_List)
  sbjt_tuples = sbjt_tuples :+ (stdNO, getStar_List)
}



var arr01 = Array(20142820, 20142932, 20152611)


case class starPoint(sbjtCD:String, starpoint:Any)
val sbjtCD_star_byStd_Map = collection.mutable.Map[String, Array[starPoint]]()



val cpsStarUri_DF = cpsStarUri_table.select(col("STD_NO"), col("STAR_KEY_ID"), col("STAR_POINT"), col("TYPE"))
// 교과 별점 => 별점 테이블에서 "TYPE"이 "C" :: ex) BAQ00028

//STAR_KEY_ID(교과목코드, sbjtCD), STAR_POINT이 있는 dataframe
val cpsStarUri_sbjt_DF = cpsStarUri_DF.filter(cpsStarUri_DF("TYPE").equalTo("C")).drop("TYPE")

val sbjtCD_star_Array = Array[String, Double]
val sbjtCD_star_Array = cpsStarUri_sbjt_DF.rdd.map{r=> r._1, r._2}.collect



val temp001 = arr01.flatMap{ stdNO =>

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

  // star_temp_bystdNO_DF.collect.map{record =>
  //   val key = record(0)
  //   println(key)
  //   val starP = starPoint(record(1).toString, record(2).toString)
  //   println(starP)
  //   val sbjtcd_star_record = (stdNO.toString, starP)
  //   println(sbjtcd_star_record)
  //   sbjtCD_star_byStd_Map += (sbjtcd_star_record)
  // }
  res
}.toMap


    var getStar_by_stdNO = cpsStarUri_sbjt_DF.filter(cpsStarUri_sbjt_DF("STD_NO").equalTo(s"${arr01(s)}"))
    // var star_key_id = "AAM00351"

    //if문 추가 : 수강은 했는데 별점을 내리지 않은 경우에(-1) / 별점을 내린 경우(별점) / 수강하지 않은 경우(0)
    var sbjtCD = sbjtCD_star_byStd_Map(s"${arr01(s)}")(i).sbjtCD
    var getStar_temp1 = getStar_by_stdNO.filter(getStar_by_stdNO("STAR_KEY_ID").equalTo(s"${sbjtCD}")).select(col("STAR_POINT"))

    if(getStar_temp1.count == 0){
       star_point_List = -1.0::star_point_List
    }else{
      star_point_List = 0.0::star_point_List
    }

  }
  //
  for(i<-0 until sbjtNM_by_stdNO_List.size){

    var student_have_sbjt_temp1 = sbjtCD_in_departNM.filter(sbjtCD_in_departNM("STD_NO").equalTo(s"${arr01(s)}"))
    // var student_have_sbjt_temp2 = student_have_sbjt_temp1.select(col("SBJT_KOR_NM"))
    var student_have_sbjt_temp2 = student_have_sbjt_temp1.select(col("SBJT_KEY_CD"))
    student_have_sbjt_temp2.show

    var getStar_by_stdNO = cpsStarUri_sbjt_DF.filter(cpsStarUri_sbjt_DF("STD_NO").equalTo(s"${arr01(s)}"))
    // var star_key_id = "AAM00351"
    getStar_by_stdNO.show
    // //if문 추가 : 수강은 했는데 별점을 내리지 않은 경우에(-1) / 별점을 내린 경우(별점) / 수강하지 않은 경우(0)
    var sbjtCD = sbjtCD_star_byStd_Map(s"${arr01(s)}")(i).sbjtCD
    println("sbjtCD : " + sbjtCD)

    var getStar_temp1 = getStar_by_stdNO.filter(getStar_by_stdNO("STAR_KEY_ID").equalTo(s"${sbjtCD}")).select(col("STAR_POINT"))

    getStar_temp1.show
    // if(getStar_temp1.count == 0){
    //    -1
    // }

  }

  var valueBystdNo_from_Map = sbjtCD_star_byStd_Map(arr01(s).toString)

  for(i<-0 until valueBystdNo_from_Map.size){
    //학생 한명의 중분류-별점 맵에서 중분류 키에 접근 : valueBystdNo_from_Map(i).subcd)
    //학생 한명이 들은 중분류 리스트를 가져옴
    orderedIdx_byStd = sbjtCD_in_departNM_List.indexOf(valueBystdNo_from_Map(i).sbjtCD)::orderedIdx_byStd
    println("orderedIdx_byStd ===> " + orderedIdx_byStd)
  }

  orderedIdx_byStd = orderedIdx_byStd.sorted
  println("orderedIdx_byStdsorted ===>" + orderedIdx_byStd)

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
