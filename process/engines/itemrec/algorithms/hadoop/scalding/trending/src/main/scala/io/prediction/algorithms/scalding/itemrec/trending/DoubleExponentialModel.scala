package io.prediction.algorithms.scalding.itemrec.trending

import breeze.linalg._

/**
 * http://www.itl.nist.gov/div898/handbook/pmc/section4/pmc433.htm
 */
class DoubleExponentialModel(alpha: Double, gamma: Double) extends ForecastingModel {

  require(alpha >= 0 && alpha <= 1)
  require(gamma >= 0 && gamma <= 1)

  override def forecast(x: DenseVector[Double], n: Int): DenseVector[Double] = {

    require(x.length >= 2)

    val St = DenseVector.zeros[Double](x.length)
    val Bt = DenseVector.zeros[Double](x.length)
    val Ft = DenseVector.zeros[Double](n)
    St(0) = x(0)
    Bt(0) = x(1) - x(0)
    for (i <- 1 until x.length) {
      St(i) = alpha * x(i) + (1 - alpha) * (St(i - 1) + Bt(i - 1))
      Bt(i) = gamma * (St(i) - St(i - 1)) + (1 - gamma) * Bt(i - 1)
    }
    for (i <- 0 until n) {
      Ft(i) = St(x.length - 1) + (i + 1) * Bt(x.length - 1)
    }
    Ft
  }

}