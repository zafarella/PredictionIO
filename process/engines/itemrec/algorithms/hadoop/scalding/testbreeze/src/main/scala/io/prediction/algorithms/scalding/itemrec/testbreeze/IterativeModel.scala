package io.prediction.algorithms.scalding.itemrec.testbreeze

import breeze.linalg._

/**
 * Created by jeremy on 2/26/14.
 */
abstract class IterativeModel {
  def forecast(x: DenseVector[Double], n: Int): DenseVector[Double]
}
