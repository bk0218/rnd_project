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
// var stdNO_in_departNM = clPassUri_DF.filter(clPassUri_DF("SUST_CD_NM").equalTo(s"${departNM}")).select(col("STD_NO")).rdd.map(r=>r(0)).collect.toList

var stdNO_in_departNM = clPassUri_DF.filter(clPassUri_DF("SUST_CD_NM").equalTo(s"${departNM}")).select(col("STD_NO")).rdd.map(r=>r(0)).collect.toList.map(_.toString)

var arr01 = Array(201937039, 20153128, 20132019)
var arr02 = arr01.toList.map(_.toString)

//광홍과 학생 중 자율활동 데이터가 있는 학생은 극소수
// var stdNo_test_df =  outActUri_DF.filter(outActUri_DF("OAM_STD_NO").equalTo(s"${20132019}")).select(col("OAM_TYPE_CD"), col("OAM_TITLE"))


var activity_List_byStd = List[Any]()
var act_tuples = Seq[(String, String)]()
//광홍과 학번을 돌면서
arr02.foreach{ stdNO =>

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

  var activity_List_byStd_temp3 = activity_List_byStd_temp1.map(_._2)
  println("activity_List_byStd:" + activity_List_byStd_temp3)
  //학과 리스트를 돌면서 일치 여부 세는데
  //봉사03, 대외04, 기관05 = 횟수 count
  //자격증01, 어학02 = 유무(1 또는 0)

 activity_List_byStd = outAct_code_List ++ activity_List_byStd_temp3

 //학생 별 코드 횟수, 이름 유무 리스트 출력
 println(stdNO)
 println("activity List !!! : " + activity_List_byStd)

 act_tuples = act_tuples :+ (stdNO, activity_List_byStd.toString)
}

var act_df = act_tuples.toDF("STD_NO", "ACTING")

// var act_df_test = act_df.filter(act_df("STD_NO").equalTo("201937039")).show

//------------------------------------------------------------------------------
