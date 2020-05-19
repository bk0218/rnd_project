import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils


// =============================원본 start===================================
// // Load training data in LIBSVM format
// val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_binary_classification_data.txt")
//
// // Split data into training (60%) and test (40%)
// val Array(training, test) = data.randomSplit(Array(0.6, 0.4), seed = 11L)
// training.cache()
//
// // Run training algorithm to build the model
// val model = new LogisticRegressionWithLBFGS()
//   .setNumClasses(2)
//   .run(training)
//
// // Clear the prediction threshold so the model will return probabilities
// model.clearThreshold
//
// // Compute raw scores on the test set
// val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
//   val prediction = model.predict(features)
//   (prediction, label)
// }
//
// // Instantiate metrics object
// val metrics = new BinaryClassificationMetrics(predictionAndLabels)
//
// // Precision by threshold
// val precision = metrics.precisionByThreshold
// precision.foreach { case (t, p) =>
//   println(s"Threshold: $t, Precision: $p")
// }
//
// // Recall by threshold
// val recall = metrics.recallByThreshold
// recall.foreach { case (t, r) =>
//   println(s"Threshold: $t, Recall: $r")
// }
//
// // Precision-Recall Curve
// val PRC = metrics.pr
//
// // F-measure
// val f1Score = metrics.fMeasureByThreshold
// f1Score.foreach { case (t, f) =>
//   println(s"Threshold: $t, F-score: $f, Beta = 1")
// }
//
// val beta = 0.5
// val fScore = metrics.fMeasureByThreshold(beta)
// f1Score.foreach { case (t, f) =>
//   println(s"Threshold: $t, F-score: $f, Beta = 0.5")
// }
//
// // AUPRC
// val auPRC = metrics.areaUnderPR
// println("Area under precision-recall curve = " + auPRC)
//
// // Compute thresholds used in ROC and PR curves
// val thresholds = precision.map(_._1)
//
// // ROC Curve
// val roc = metrics.roc
//
// // AUROC
// val auROC = metrics.areaUnderROC
// println("Area under ROC = " + auROC)

// =============================원본 end===================================




val List_origin = List("삼성", "네이버", "카카오", "다음", "구글", "하이닉스", "대우", "한화", "엔씨소프트", "현대")
val List_predict =  List("lg", "삼성", "카카오", "다음", "하이닉스", "대우", "한화", "구글", "엔씨소프트", "현대")


// Load training data in LIBSVM format
//val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_binary_classification_data.txt")

// Split data into training (60%) and test (40%)
//val Array(training, test) = data.randomSplit(Array(0.6, 0.4), seed = 11L)
val training = List("삼성", "네이버", "카카오", "다음", "구글", "하이닉스", "대우", "한화", "엔씨소프트", "현대")
val test = List("네이버", "삼성", "카카오", "다음", "하이닉스", "대우", "한화", "구글", "엔씨소프트", "현대")

// training.cache()

// Run training algorithm to build the model
val model = new LogisticRegressionWithLBFGS().setNumClasses(2).run(training)
//model이 우리꺼

// Clear the prediction threshold so the model will return probabilities
model.clearThreshold

// Compute raw scores on the test set
val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
  val prediction = model.predict(features)
  (prediction, label)
}

// Instantiate metrics object
val metrics = new BinaryClassificationMetrics(predictionAndLabels)

// Precision by threshold
val precision = metrics.precisionByThreshold
precision.foreach { case (t, p) =>
  println(s"Threshold: $t, Precision: $p")
}

// Recall by threshold
val recall = metrics.recallByThreshold
recall.foreach { case (t, r) =>
  println(s"Threshold: $t, Recall: $r")
}

// F-measure
val f1Score = metrics.fMeasureByThreshold
f1Score.foreach { case (t, f) =>
  println(s"Threshold: $t, F-score: $f, Beta = 1")
}








private[ml] def evaluateClassificationModel(
      model: Transformer,
      data: DataFrame,
      labelColName: String): Unit = {
    val fullPredictions = model.transform(data).cache()
    val predictions = fullPredictions.select("prediction").rdd.map(_.getDouble(0))
    val labels = fullPredictions.select(labelColName).rdd.map(_.getDouble(0))
    // Print number of classes for reference.
    val numClasses = MetadataUtils.getNumClasses(fullPredictions.schema(labelColName)) match {
      case Some(n) => n
      case None => throw new RuntimeException(
        "Unknown failure when indexing labels for classification.")
    }
    val accuracy = new MulticlassMetrics(predictions.zip(labels)).accuracy
    println(s"  Accuracy ($numClasses classes): $accuracy")
  }









  import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
  import org.apache.spark.mllib.evaluation.MulticlassMetrics
  import org.apache.spark.mllib.regression.LabeledPoint
  import org.apache.spark.mllib.util.MLUtils

  // Load training data in LIBSVM format
  val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_multiclass_classification_data.txt")

  // Split data into training (60%) and test (40%)
  val Array(training, test) = data.randomSplit(Array(0.6, 0.4), seed = 11L)
  training.cache()

  // Run training algorithm to build the model
  val model = new LogisticRegressionWithLBFGS()
    .setNumClasses(3)
    .run(training)

  // Compute raw scores on the test set
  val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
    val prediction = model.predict(features)
    (prediction, label)
  }

  // Instantiate metrics object
  val metrics = new MulticlassMetrics(predictionAndLabels)

  // Confusion matrix
  println("Confusion matrix:")
  println(metrics.confusionMatrix)

  // Overall Statistics
  val accuracy = metrics.accuracy
  println("Summary Statistics")
  println(s"Accuracy = $accuracy")

  // Precision by label
  val labels = metrics.labels
  labels.foreach { l =>
    println(s"Precision($l) = " + metrics.precision(l))
  }

  // Recall by label
  labels.foreach { l =>
    println(s"Recall($l) = " + metrics.recall(l))
  }

  // False positive rate by label
  labels.foreach { l =>
    println(s"FPR($l) = " + metrics.falsePositiveRate(l))
  }

  // F-measure by label
  labels.foreach { l =>
    println(s"F1-Score($l) = " + metrics.fMeasure(l))
  }

  // Weighted stats
  println(s"Weighted precision: ${metrics.weightedPrecision}")
  println(s"Weighted recall: ${metrics.weightedRecall}")
  println(s"Weighted F1 score: ${metrics.weightedFMeasure}")
  println(s"Weighted false positive rate: ${metrics.weightedFalsePositiveRate}")
