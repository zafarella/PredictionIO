package io.prediction.algorithms.scalding.itemrec.trending

import breeze.linalg._

abstract class ForecastingModel {
  def forecast(x: DenseVector[Double], n: Int): DenseVector[Double]
}