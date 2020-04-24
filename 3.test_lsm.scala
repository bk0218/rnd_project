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

// 2-1. 학과
var clPassUri_DF = clPassUri_table.select(col("STD_NO"), col("SUST_CD_NM"), col("SBJT_KOR_NM"), col("SBJT_KEY_CD")).distinct.toDF
var departNM_by_stdNO = clPassUri_DF.filter(clPassUri_DF("STD_NO").equalTo(s"${std_NO}")).select(col("SUST_CD_NM")).distinct
var departNM = departNM_by_stdNO.collect().map(_.getString(0)).mkString("")

// 2-2. 학과 학생 리스트
var stdNO_in_departNM = clPassUri_DF.filter(clPassUri_DF("SUST_CD_NM").equalTo(s"${departNM}")).select(col("STD_NO"))
var stdNO_in_departNM_List = stdNO_in_departNM.rdd.map(r=>r(0)).collect.toList.distinct

// 3-1. 학생의 수업 리스트
var sbjtNM_by_stdNO = clPassUri_DF.filter(clPassUri_DF("STD_NO").equalTo(s"${std_NO}")).select(col("SBJT_KOR_NM"))
var sbjtNM_by_stdNO_List = sbjtNM_by_stdNO.rdd.map(r=>r(0)).collect.toList.distinct


// 3-2. 학과의 수업 리스트
// 컴퓨터공학과에서 개설된 수업명을 리스트로 생성 : 과목명이 1452개 -> distinct 지정distinct하게 자름 => 108개
var sbjtNM_in_departNM = clPassUri_DF.filter(clPassUri_DF("SUST_CD_NM").equalTo(s"${departNM}")).select("SBJT_KOR_NM")
var sbjtNM_in_departNM_List = sbjtNM_in_departNM.rdd.map(r=>r(0)).collect.toList.distinct


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

// from. 교과/비교과용 별점 테이블(CPS_STAR_POINT) : 학번(STD_NO), 비교과id(STAR_KEY_ID), 별점(STAR_POINT), 타입(TYPE)
var cpsStarUri_DF = cpsStarUri_table.select(col("STD_NO"), col("STAR_KEY_ID"), col("STAR_POINT"), col("TYPE"))

// 비교과 별점 => 별점 테이블에서 "TYPE"이 "N" :: ex) NCR000000000677
var cpsStarUri_ncr_DF = cpsStarUri_DF.filter(cpsStarUri_DF("TYPE").equalTo("N"))
// 교과 별점 => 별점 테이블에서 "TYPE"이 "C" :: ex) BAQ00028
var cpsStarUri_sbjt_DF = cpsStarUri_DF.filter(cpsStarUri_DF("TYPE").equalTo("C"))

// 학번 별 과목 ID, 별점을 가져감
var getStar_by_stdNO = cpsStarUri_sbjt_DF.filter(cpsStarUri_sbjt_DF("STD_NO").equalTo(s"${std_NO}")).show





//-----------------------------------------------학생 한명----------------------------------------------------
var student_have_sbjt_temp1 = clPassUri_DF.filter(clPassUri_DF("SUST_CD_NM").equalTo(s"${departNM}"))
var student_have_sbjt_temp2 = student_have_sbjt_temp1.filter(student_have_sbjt_temp1("STD_NO").equalTo(s"${std_NO}"))
var student_have_sbjt_temp3 = student_have_sbjt_temp2.select(col("SBJT_KOR_NM"))

//@@@ 컴퓨터공학과의 학생 한명이 수강한 수업 리스트를 생성
var student_have_sbjt_List = student_have_sbjt_temp3.select("SBJT_KOR_NM").rdd.map(r=>r(0)).collect.toList.distinct

val isListened_List_temp1 = sbjtNM_List.map(x => (x, 0)).map{ record =>
  //x : record_1
  //0 : record_2
  //isListend면 1로 바뀜
  val name = record._1
  val isListened =
    if(student_have_sbjt_List.contains(name)) {
      1
    }
    else 0
  val isListened_List_temp2 = (name, isListened)
  print(isListened_List_temp2)
  //리턴하려면 이름을 쳐야 함
  //최종적으로 isListened_List_temp1 = isListened_List_temp2 값이 담기는 것 !!
  isListened_List_temp2
}
val isListened_List = isListened_List_temp1.map(_._2)
//-----------------------------------------------=----------------------------------------------------------------


//==================================================일단 계산ㅇ ㅣ너무 느리니까 다른 방법으로,,=======================================
var tuples = Seq[(Int, List[Int])]()

// 학번이 1452개 -> distinct 지정 -> 223 명
var stdNO_in_departNM = clPassUri_DF.filter(clPassUri_DF("SUST_CD_NM").equalTo(s"${departNM}")).select(col("STD_NO")).rdd.map(r=>r(0)).collect.toList.distinct

stdNO_in_departNM.foreach{ stdNO =>

  // 학생별로 (stdNO) 들은 교과목 테이블
  var student_have_sbjt_temp1 = sbjtNM_in_departNM.filter(sbjtNM_in_departNM("STD_NO").equalTo(s"${stdNO}"))
  var student_have_sbjt_temp2 = student_have_sbjt_temp1.select(col("SBJT_KOR_NM"))
  student_have_sbjt_temp2.show

  // 학과 전체 교과목 리스트를 순회 (교과, 0)으로 만들어놓음
  val isListened_List_temp1 = sbjtNM_List.map(x => (x, 0)).map{ record =>
    // println(s"stdNO : ${stdNO} ============= sbjtNM : ${record}")
    //
    //@@@ 컴퓨터공학과의 학생 한명이 수강한 수업 리스트를 생성
    var student_have_sbjt_List = student_have_sbjt_temp2.select("SBJT_KOR_NM").rdd.map(r=>r(0)).collect.toList.distinct
    // println(s"student_have_sbjt_List--${student_have_sbjt_List}")
    //x : record_1
    //0 : record_2
    //isListend면 1로 바뀜
    val name = record._1

    val isListened =
      if(student_have_sbjt_List.contains(name)) {
        1
      }
      else 0
    val isListened_List_temp2 = (name, isListened)
    // print(isListened_List_temp2)
    //리턴하려면 이름을 쳐야 함
    //최종적으로 isListened_List_temp1 = isListened_List_temp2 값이 담기는 것 !!
    isListened_List_temp2
  }
  val isListened_List = isListened_List_temp1.map(_._2).toString
  // println(isListened_List)


  sbjt_tuples = sbjt_tuples :+ (stdNO, isListened_List)
}

var sbjt_df = sbjt_tuples.toDF("STD_NO", "SUBJECT")

//==========================================================================================================================






//-------------------- # # # 자율활동 리스트 # # # ------------------------------
//from.교외활동 CPS_OUT_ACTIVITY_MNG : 학번(OAM_STD_NO), 활동구분코드(OAM_TYPE_CD), 활동명(OAM_TITLE)
//자격증(CD01) : 이름(OAM_TITLE) / ex. 토익800~900, FLEX 일본어 2A,  FLEX 일본어 1A,  FLEX 중국어 1A
//어학(CD02) : 이름(OAM_TITLE)
//봉사(CD03), 대외활동(CD04), 기관현장실습(CD05) : 활동구분코드(OAM_TYPE_CD)


//학과 학생들 서치
//학과df
//for문으로 거기 있는 학번을 입력하고 자율활동내역을 긁어오고 -> 학생리스트
//학과 리스트 생성
//횟수 리스트



//학번으로 학과 찾기
// var showDepart_by_stdNO = clPassUri_DF.filter(clPassUri_DF("STD_NO").equalTo(s"${201937029}"))

import spark.implicits._

var outActUri_DF = outActUri_table.select(col("OAM_STD_NO"), col("OAM_TYPE_CD"), col("OAM_TITLE"))
var departNM = "광고홍보학과"
var std_NO1 = 201937039
var std_NO2 = 20130001
var clPassUri_DF = clPassUri_table.select(col("SUST_CD_NM"), col("STD_NO")).distinct.toDF
var students_in_departNM = clPassUri_DF.filter(clPassUri_DF("SUST_CD_NM").equalTo(s"${departNM}")).select(col("STD_NO"))
//map연산은 dataframe에 쓸 수 없기 때문에 list로 변환해야 하며 dataframe을 list로 변환하려면 df의 값 하나하나에 접근하기 위해 map 연산이 필요함

//광홍과df(clpass 교과목 수료 테이블에서 학과 별 학번 dataframe을 생성한 뒤 list로 변환)
var stdNO_in_departNM = clPassUri_DF.filter(clPassUri_DF("SUST_CD_NM").equalTo(s"${departNM}")).select(col("STD_NO")).rdd.map(r=>r(0)).collect.toList

//광홍과 학생 중 자율활동 데이터가 있는 학생은 극소수
// var stdNo_test_df =  outActUri_DF.filter(outActUri_DF("OAM_STD_NO").equalTo(s"${20132019}")).select(col("OAM_TYPE_CD"), col("OAM_TITLE"))


var activity_List_byStd = List[Any]()
//광홍과 학번을 돌면서
stdNO_in_departNM.foreach{ stdNO =>
  var depart_activity_temp = List[Any]()
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
  var outAct_code_List = outAct_code_temp3.select(col("count")).rdd.map(r=>r(0)).collect.toList
  //------------------------------------------------------------------------------


  //---------------------자율활동 name list(자격증01, 어학02)----------------------
  //5개의 코드
  var outAct_name_temp1 = outActUri_DF.filter(outActUri_DF("OAM_STD_NO").equalTo(s"${stdNO}")).select(col("OAM_STD_NO"), col("OAM_TITLE")).distinct
  //3개의 코드만 필터링
  var outAct_name_temp2 = outAct_name_temp1.drop("OAM_STD_NO", "OAM_TYPE_CD").filter($"OAM_TYPE_CD" === "OAMTYPCD01" || $"OAM_TYPE_CD" ==="OAMTYPCD02").distinct

  var outAct_name_List = outAct_name_temp2.rdd.map(r=>r(0)).collect.toList
  println(outAct_name_List)
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
  var depart_activity_List = depart_activity_temp.distinct
  println(depart_activity_List)

  //namelist로 유무 비교
  //학생 name list 랑 학과 list를 비교해서 contain으로 1, 0

  var activity_List_byStd_temp1 = depart_activity_List.map(x => (x, 0)).map{ activity =>
    //x : record_1
    //0 : record_2
    //isListend면 1로 바뀜
    val actName = activity._1
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
  println("activity_List_byStd:" + activity_List_byStd_temp3)
  //학과 리스트를 돌면서 일치 여부 세는데
  //봉사03, 대외04, 기관05 = 횟수 count
  //자격증01, 어학02 = 유무(1 또는 0)

 activity_List_byStd = outAct_code_List ++ activity_List_byStd_temp3

 //학생 별 코드 횟수, 이름 유무 리스트 출력
 println("activity List !!! : " + activity_List_byStd)
}

//------------------------------------------------------------------------------



//--------------------mongodb에서 collection 복제---------------------
// db.CPS_NCR_PROGRAM_INFO.find().forEach( function(x){db.CPS_NCR_PROGRAM_INFO2.insert(x)} )
//--------------------mongodb에서 데이터 삭제 : 지우기 전에 원본 데이터 복사해두고 하나만 insert------------
//db.CPS_NCR_PROGRAM_INFO2.remove({NPI_KEY_ID:"NCR000000000718"})





//-------------------- # # # 비교과 리스트 # # # --------------------------------
// from. 교과/비교과용 별점 테이블(CPS_STAR_POINT) : 학번(STD_NO), 비교과id(STAR_KEY_ID), 별점(STAR_POINT)
// from. 비교과 관련 테이블(CPS_NCR_PROGRAM_INFO) : 비교과id(NPI_KEY_ID), 중분류(NPI_AREA_SUB_CD)

//비교과 아이디(학번, 비교과id 사용 from.교과/비교과별점테이블)로 중분류 가져오기(비교과id, 중분류 from.비교과 관련 테이블)

//학과 별 학생들이 수강한 비교과의 중분류 list 로 포맷 잡고 : 학과 - 학번 돌면서 list 만들고 , 중분류로 바꿔주기
//학생 한명이 수강한 비교과 list -> 별점 가져오기(from. 교과/비교과 별점 테이블) -> 중분류 가져오기 -> 중분류 별 별점 avg 계산


// ---------------------------------------------------------------------------`
// from. 비교과 테이블 : NCR~ ID를 이용해 별점 테이블에 데이터를 생성해줌 !!!!***`
// var test = ncrInfoUri_DF.select(col("NPI_KEY_ID"),col("NPI_AREA_SUB_CD")).filter(ncrInfoUri_DF("NPI_KEY_ID").equalTo("NCR000000000718"))
var ncrInfoUri_DF = ncrInfoUri_table.select(col("NPI_KEY_ID"), col("NPI_AREA_SUB_CD"))
var get_NPI_KEY_ID = ncrInfoUri_DF.select(col("NPI_KEY_ID"),col("NPI_AREA_SUB_CD"))
//<학생 DataFrame> / 하나의 학과의 학생 => 지금 컴퓨터공학과 학생을 조회했으니까 컴퓨터공학과 학번을 조회해서 별점 테이블에 데이터를 삽입해줌
var departNM = "컴퓨터공학과"
//from. 교과 수료 테이블 : departNM에 담긴 학과의 학생들의 학번을 가져옴
var students_in_departNM = clPassUri_DF.filter(clPassUri_DF("SUST_CD_NM").equalTo(s"${departNM}")).select(col("STD_NO"))
students_in_departNM.show
// ---------------------------------------------------------------------------
// 컴퓨터 공학과 학생 4명에 대해 별점 테이블 데이터 추가함 : |20142820||20142932||20152611| ////////////////////// |20152615| 잘못넣음
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

var getStar_by_stdNO = cpsStarUri_DF.filter(cpsStarUri_DF("STD_NO").equalTo(s"${std_NO2}")).show

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

// 학과의 중분류 List
// Map(20142932 -> Array(starPoint(NCR_T01_P01_C03,null), starPoint(NCR_T01_P05_C02,null), starPoint(NCR_T01_P04_C03,3.8), starPoint(NCR_T01_P01_C01,4.5), starPoint(NCR_T01_P02_C03,3.85), starPoint(NCR_T01_P03_C03,4.0), starPoint(NCR_T01_P03_C01,4.2)),
// 20152611 -> Array(starPoint(NCR_T01_P01_C03,null), starPoint(NCR_T01_P05_C02,null), starPoint(NCR_T01_P04_C03,3.8), starPoint(NCR_T01_P04_C07,null), starPoint(NCR_T01_P01_C01,4.5), starPoint(NCR_T01_P02_C03,3.85), starPoint(NCR_T01_P03_C03,4.0), starPoint(NCR_T01_P03_C01,4.2)),
// 20142820 -> Array(starPoint(NCR_T01_P04_C03,3.8), starPoint(NCR_T01_P01_C01,4.5), starPoint(NCR_T01_P02_C03,3.85), starPoint(NCR_T01_P03_C03,4.0), starPoint(NCR_T01_P03_C01,4.2)))

println(subcd_byDepart_List)

// 학과의 모든 학번의 (중분류, 별점) Map
subcd_star_byStd_Map





//----------------------------------------------------------------------------------------------------------------------------

val arr01 = Array(20142820, 20142932, 20152611)

subcd_star_byStd_Map("20142932")(0).subcd
subcd_star_byStd_Map("20142932")(0).starpoint
var sub_cd = List[Any]()
var star_point = List[Any]()
var star_point_list = List[Any]()
/*
arr01.foreach{ stdNO =>

}*/
//
// for(i <- 0 until arr01.size){
//   for ( j <- 0 until subcd_star_byStd_Map(arr01(i).toString).size){
//     // sub_cd = subcd_star_byStd_Map(arr01(i).toString)(j).subcd :: sub_cd
//
//     star_point = subcd_star_byStd_Map(arr01(i).toString)(j).starpoint :: star_point
//
//     // sub_cd.flatMap(x => Some(x))
//     // star_point = star_point ++ subcd_star_byStd_Map(arr01(i).toString)(j).starpoint
//     // star_point.flatMap(x => Some(x))
//   }
//   // star_point_list =
// }
//
//
//
// for(i <- 0 until arr01.size){
//     sub_cd = sub_cd ++ subcd_star_byStd_Map(arr01(i).toString)(i).subcd
//     sub_cd.flatMap(x => Some(x))
//     star_point = star_point ++ subcd_star_byStd_Map(arr01(i).toString)(i).starpoint
//     star_point.flatMap(x => Some(x))
// }
//
// var star_point = List[Any]()
//
// subcd_byDepart_List.foreach { subcd =>
//   for(i <- 0 until arr01.size){
//     var temp = subcd_star_byStd_Map(arr01(i).toString)(i).subcd
//     println(temp)
//     // for ( j <- 0 until subcd_star_byStd_Map(arr01(i).toString).size){
//     //   // sub_cd = subcd_star_byStd_Map(arr01(i).toString)(j).subcd :: sub_cd
//     //
//     //   // star_point = subcd_star_byStd_Map(arr01(i).toString)(j).starpoint :: star_point
//     //   // for()
//     //
//     //
//     //   // sub_cd.flatMap(x => Some(x))
//     //   // star_point = star_point ++ subcd_star_byStd_Map(arr01(i).toString)(j).starpoint
//     //   // star_point.flatMap(x => Some(x))
//     // }
//     // star_point_list =
//   }
// }


//var star_point = List[Any]()
var names = List[String]()
var tuples = Seq[(Int, String)]()

for(s<-0 until subcd_star_byStd_Map.size){ // 학과 학생 학번 List 를 for문
  var star_point = List[Any]() // 학번당 별점을 저장
  var order = List[Int]() //
  for(i<-0 until subcd_byDepart_List.size){ //학과 전체 중분류 코드 List => 학번당 별점을 중분류 갯수만금 0.0으로 셋팅
    star_point = 0.0::star_point
  }

  // subcd_star_byStd_Map(arr01(0).toString)
  // Array(starPoint(NCR_T01_P04_C03,3.8), starPoint(NCR_T01_P01_C01,4.5), starPoint(NCR_T01_P02_C03,3.85), starPoint(NCR_T01_P03_C03,4.0), starPoint(NCR_T01_P03_C01,4.2))
  // 학번에 대해서 (중분류, 별점)

  // subcd_star_byStd_Map(arr01(0).toString)(0)
  // starPoint = starPoint(NCR_T01_P04_C03,3.8)

  // subcd_star_byStd_Map(arr01(0).toString)(0).subcd
  // String = NCR_T01_P04_C03

  for(i<-0 until subcd_star_byStd_Map(arr01(s).toString).size){ // 학번당 중분류를 order에 넣음
    order = subcd_byDepart_List.indexOf(subcd_star_byStd_Map(arr01(s).toString)(i).subcd)::order
    //names = subcd_star_byStd_Map(arr01(s).toString)(i).subcd::names
  }

  order = order.sorted // order를 정렬

  for(i<-0 until order.size){ // order 크기 (학번당 들은 중분류를 for문 돌림)
    var k=0;
    //print(k)
    // 학과 전체의 중분류 리스트와 학생의 중분류 리스트의 값이 같을때까지 k를 증가
    while(subcd_byDepart_List(order(i))!=subcd_star_byStd_Map(arr01(s).toString)(k).subcd){
    k+=1;
    }
    // 같은 값이 나오면 0으로 설정돼있던 값을 (그 자리의 값을) 학생의 별점으로 바꿔줌
    star_point = star_point.updated(order(i), subcd_star_byStd_Map(arr01(s).toString)(k).starpoint)
  }
  tuples = tuples :+ (arr01(s), star_point.toString)
}

var df = tuples.toDF("STD_NO", "RATING")
