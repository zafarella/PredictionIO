package io.prediction.algorithms.scalding.itemrec.trending

import breeze.linalg._

/**
 * Created by jeremy on 2/9/14.
 */
class PolynomialModel(degree: Int) extends ForecastingModel {

  def getCoefficients(y: DenseVector[Double]): DenseVector[Double] = {
    var x = DenseVector[Double]((0 until y.length).map(_.toDouble).toArray)
    var X = DenseVector.ones[Double](x.length).asDenseMatrix.t
    for (i <- 1 to degree) {
      X = DenseMatrix.horzcat(X, x.map(math.pow(_, i)).asDenseMatrix.t)
    }
    val coeff = X \ y
    coeff
  }

  def forecast(y: DenseVector[Double], n: Int): DenseVector[Double] = {
    val coeff = getCoefficients(y)
    val forecasts = DenseVector.zeros[Double](n)
    for (i <- 0 until n) {
      forecasts(i) = coeff(0)
      var time = y.length + i + 1
      for (j <- 1 to degree) {
        forecasts(i) += coeff(j) * math.pow(time, j)
      }
    }
    forecasts
  }

}
