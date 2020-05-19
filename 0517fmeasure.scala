val list_origin = List("삼성", "네이버", "카카오", "다음", "구글", "하이닉스", "대우", "한화", "엔씨소프트", "현대")
val list_recommend =  List("LG", "삼성", "카카오", "다음", "하이닉스")


var contain_count = 0
list_recommend.foreach{ list =>
  if(list_origin.contains(list)){
    contain_count = contain_count+1
  }
  contain_count
}

val precision = contain_count.toFloat/list_origin.length
val recall = contain_count.toFloat/list_recommend.length
val f_measure = 2*((recall*precision)/(recall+precision)

//// 최종 Running Runtime 확인

import java.util.concurrent.TimeUnit.NANOSECONDS
def time[T](f: => T): T = {
  val start = System.nanoTime()
  val ret = f
  val end = System.nanoTime() // scalastyle:off println
  println(s"Time taken: ${NANOSECONDS.toMillis(end - start)} ms")
  // scalastyle:on println
  ret
}


//spark.time(함수명)
