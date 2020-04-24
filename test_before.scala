
//------------------------------------------------------------------------------
//mongodb 안켜져있으면  cd $SPARK_HOME에서 mongod 하고 다른 쉘에서 같은 위치로 이동하고 mongo 하고 또 다른 쉘에서 다시 spark 실행

//학교 구분해서 가져옴
val university = getMongoDF(spark, clInfoUri)
val univDF = university.toDF
val univAll = univDF.select(col("COLG_CD_NM"))
val univDistinct = univAll.distinct
univDistinct.show

//------------------------------------------------------------------------------
//1.학교 별 학과 : 학생 정보 테이블에서 대학코드 1개로 학과 select
// val departmentInfo = university.select(col("SUST_CD_NM"))
// val departDistinct = departmentInfo.distinct
// departDistinct.show

// db.collection이름.find()



// 대학교 별 학과 긁어오기
//from 학생정보 테이블
val studentInfoTable = getMongoDF(spark, stInfoUri)
studentInfoTable.show
val univAndDepart = studentInfoTable.select(col("COLG_CD"), col("COLG_CD_NM"), col("SUST_CD"), col("SUST_CD_NM"))
univAndDepart.show


val mechanic_01 = univAndDepart.where($"COLG_CD"===1)
val language_02 = univAndDepart.where($"COLG_CD"===2)
val design_03 = univAndDepart.where($"COLG_CD"===3)

//학교 별 학과 고유하게 출력
val mechanic = mechanic_01.distinct
val language = language_02.distinct
val design = design_03.distinct
mechanic.show
language.show
design.show


//------------------------------------------------------------------------------
//2.학과 별 교과과목 테이블
//from 교과정보 테이블
val classInfoTable = getMongoDF(spark, clInfoUri)
classInfoTable.show

//학과명이 같아도 학과 코드가 다른 경우가 있기 때문에 학과명으로만 조건 부여,(학과코드로 했을 시 데이터 일부는 안가져오는 문제 방지)
val classInfo = classInfoTable.select(col("SUST_CD_NM"), col("SBJT_KOR_NM"))
classInfoDistinct.show(50)


val computerScience = classInfo.where($"SUST_CD_NM"==="컴퓨터공학과")
computerScience.show(50)

val testComputer = computerScience.filter(computerScience("SUST_CD_NM"==="컴퓨터공학과").collect
val testComputer = computerScience.toDF
print(testComputer)


val testDF = computerScience.filter($"SUST_CD_NM"==="컴퓨터공학과")
testDF.show
val comSci_SUST = testDF.where($"SBJT_KOR_NM")
comSci_SUST.show

val departArray = new Araray[String]()
val departArr = Array("교과목1", "교과목2")
//show는 df에 적용되는 함수

val collectTemp = testDF.collect




//array?는 print로 출력
print(departArr(0))

//컴퓨터공학과의 첫번재 교과목을 넣기

departArray(0)=


val mechanicDepart = classInfo.where($"COLG_CD_NM"==="공과대학")
mechanicDepart.show(30)
val mechanicDepartDistinct = mechanicDepart.where($"SUST_CD_NM"==="컴퓨터공학과")
mechanicDepartDistinct.show(40)


//from 교과목수료 테이블
val clPassTable = getMongoDF(spark, clPassUri)

val clPass = clPassTable.select(col("STD_NO"), col("COLG_CD"), col("COLG_CD_NM"), col("SUST_CD_NM"), col("SBJT_KOR_NM"))
clPass.show(30)
//열 지정해서 중복 제거 : dropDuplicates
//distinct는 모든 열의 값이 같을 경우에만 중복이라고 판단 !! 되도록이면 사용하지 말 것


//학번 1개로 서치
val businessDepart = clPass.where($"COLG_CD_NM"==="경영대학")
val std1 = businessDepart.where($"STD_NO"==="20131705")
val std1_distinct = std1.dropDuplicates("SBJT_KOR_NM")
std1_distinct.show(30)


val std2 = businessDepart.where($"STD_NO"==="20131717")
val std2_distinct = std2.dropDuplicates("SBJT_KOR_NM")
std2_distinct.show(30)
//------------------------------------------------------------------------------
//학과 이름을 배열로 넣어논 다음에
//for문을 돌면서 조건절로 학과 변수를 넣음
//그거가지고 매트릭스 생성


//같은 학교, 학과의 모든 학생들의 교과정보를 서치
//중복 제거해서
//교과정보 테이블 생성



//학교 정보를 배열에 저장
//교과정보 테이블에서 학교, 학과 정보 가져오기


//!!!for문으로 대학 별 학과 가져와보기
val classInfoTable = getMongoDF(spark, clInfoUri)
val classInfo = classInfoTable.select(col("COLG_CD"), col("COLG_CD_NM"), col("SUST_CD_NM"))
val classInfoDrop = classInfo.dropDuplicates("SUST_CD_NM")
classInfoDrop.show

val univ1 = classInfo.where($"COLG_CD_NM"==="공연영상예술대학")
univ1.show

val univ10 = classInfo.where($"COLG_CD_NM"==="공과대학")
val univ10_1 = univ10.dropDuplicates("SUST_CD_NM")
univ10_1.show


val univ11 = classInfo.where($"COLG_CD_NM"==="이공대학")
val univ11_1 = univ11.dropDuplicates("SUST_CD_NM")
univ11_1.show

val univArray =

//학교 temp 변수에 학교 명을 for문 돌면서 한개씩 입력해줌!
val University

//학교 별 학과 정보를 for문 돌면서 입력해줌

//학과 별 학생 정보를 for문 돌면서 입력해줌

//모든 학생들의 교과과목을 모은 뒤

//중복제거




//arraylist로 생성

//df를 정의 : 열 값을 학교 이름으로 넣기!
//df 값 입력 : 학교 순서에 맞게 학과 이름 넣기

//from 교과정보 table


//------------------------------------------------------------------------------
//3.학과 별 비교과 과목 테이블



//------------------------------------------------------------------------------
//4.학과 별 자율활동 테이블
//from. CPS_OUT_ACTIVITY_MNG(교외활동 테이블)
val outAct_Table = getMongoDF(spark, outActUri)
val outAct = outAct_Table.select(col("OAM_TYPE_CD"))
outAct.show

//배열에 들어있는 자율활동 정보가 리스트의 순서로 들어가야 함?
//배열로 일단 배열 만들기
//자율활동정보 배열에서 하나의 값에 접근 + 학생배열 하나 = 학생이 해당 자율활동을 수행했는지!
val outActDistinct = outAct.distinct
outActDistinct.show
val outActArray = outActDistinct.collect
print(outActArray.mkString(","))
print(outActArray(1))
//------------------------------------------------------------------------------
//from. VSTD_CDP_STEG(학생정보테이블)
val stInfoUri_table = getMongoDF(spark, stInfoUri)

studentInfoTable.show
val univAndDepart = studentInfoTable.select(col("SUST_CD"), col("SUST_CD_NM"), col("STD_NO"))
univAndDepart.show

val computerStudent = studentInfoTable.select(col("SUST_CD_NM"))


val classInfo = classInfoTable.select(col("COLG_CD"), col("COLG_CD_NM"), col("SUST_CD_NM"))
val classInfoDrop = classInfo.dropDuplicates("SUST_CD_NM")
classInfoDrop.show

val univ1 = classInfo.where($"COLG_CD_NM"==="공연영상예술대학")







//학과배열 !
val departArray = studentInfoTable.select(col("SUST_CD_NM")) //~.select 의 반환값이 spark.sql.row ~
departArray.show
val departArrDistinct = departArray.distinct
departArrDistinct.show

val departArr = departArrDistinct.collect
print(departArr.mkString(","))

departArr.size




//학생배열
val univAndDepart = studentInfoTable.select(col("SUST_CD"), col("SUST_CD_NM"), col("STD_NO"))
val univ1 = univAndDepart.where($"SUST_CD_NM"==="사회복지학과")
univ1.show
val univ1Distinct = univ1.drop("SUST_CD","SUST_CD_NM")
univ1Distinct.show



// val studentArray = univ1Distinct.collect
// print(studentArray.mkString(","))





var tmp_arr = departArray.collect().distinct

//val(value) : 변경불가
//var(variable) : 변경가능

var studentArray



tmp_arr.foreach{ col =>
  var col_toString = col.toString
  var dept = col_toString.substring(1, col_toString.size-1)
  println(s"this col : ${dept}")
  var univ1 = univAndDepart.select($"SUST_CD_NM", $"STD_NO").where($"SUST_CD_NM"===s"$dept")


  studentArray = univ1.select($"STD_NO").collect
  print(s"${studentArray}")
  //:paste로 해서 넣어야 eof 에러 안남 !!
}




//------------------------------------------------------------------------------


// list 구현 !
//리스트에는 자율활동 값 5개가 들어가야 함 !

val listtemp = outActDistinct.select("OAM_TYPE_CD").collect().map(_(0)).toList
listtemp.foreach(println)





//string 값을 가진 배열


val arr= Array(0, 2, 4, 6, 8)





val array = new Array[String](5)
val array = outActDistinct.collect
print(array.mkString(","))


// val matrix = Array.ofDim[String](1,5)
// val matrix = outActDistinct.collect
// println(matrix(0)(4))


val matrix = Array.ofDim[String](5)
for(i <- 0 to 4)
{
  arr2 = arr +  outActArray(i)
  print(arr2(1))
}


for(i<- 0 to 4)
{
   // Accessing the values
   print(" "+arr(i))
}


//배열 만들 필요가 없음!
//배열로 매트릭스만 구현하면 끝


//학생 한명에 대해서 5개의 값 중 수강한 여부를 1 또는 0으로 입력
//1x5 행렬 생성해야 함

// val matrix = Array.ofDim[Int](1,5)
// matrix(1)(1) = 0
// matrix(1)(2) = 1
// matrix(1)(3) = 1
// matrix(1)(4) = 1
// matrix(1)(5) = 1
// print(matrix)








val mymultiarr= Array.ofDim[Int](2, 2)

        //Assigning the values
        mymultiarr(0)(0) = 2
        mymultiarr(0)(1) = 7
        mymultiarr(1)(0) = 3
        mymultiarr(1)(1) = 4

        for(i<-0 to 1; j<-0 until 2)
        {
            print(i, j)

           //Accessing the elements
           println("=" + mymultiarr(i)(j))
        }
