package io.prediction.algorithms.scalding.itemrec.trending

import breeze.linalg._

class SavitzkyGolayFilter(points: Int) {

  val hvalues = Array[Int](35, 21, 231, 429, 143, 1105, 323, 2261, 3059, 805, 5175)
  val avalues = Array[Array[Int]](
    Array[Int](17, 12, -3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
    Array[Int](7, 6, 3, -2, 0, 0, 0, 0, 0, 0, 0, 0, 0),
    Array[Int](59, 54, 39, 14, -21, 0, 0, 0, 0, 0, 0, 0, 0),
    Array[Int](89, 84, 69, 44, 9, -36, 0, 0, 0, 0, 0, 0, 0),
    Array[Int](25, 24, 21, 16, 9, 0, -11, 0, 0, 0, 0, 0, 0),
    Array[Int](167, 162, 147, 122, 87, 42, -13, -78, 0, 0, 0, 0, 0),
    Array[Int](43, 42, 39, 34, 27, 18, 7, -6, -21, 0, 0, 0, 0),
    Array[Int](269, 264, 249, 224, 189, 144, 89, 24, -51, -136, 0, 0, 0),
    Array[Int](329, 324, 309, 284, 249, 204, 149, 84, 9, -76, -171, 0, 0),
    Array[Int](79, 78, 75, 70, 63, 54, 43, 30, 15, -2, -21, -42, 0),
    Array[Int](467, 462, 447, 422, 387, 343, 287, 222, 147, 62, -33, -138, -253))

  val validPoints = Set[Int](5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25)

  require(validPoints(points))
  val index = (points - 5) / 2
  val h = hvalues(index)
  val avals = avalues(index)
  val marginsize = (points + 1) / 2

  def filter(x: DenseVector[Double]): DenseVector[Double] = {
    val n = x.length
    val filtered: DenseVector[Double] = x.copy
    for (i <- marginsize until n - marginsize) {
      var value = avals(0) * x(i)
      for (j <- 1 until marginsize) {
        value += avals(j) * (x(i + j) + x(i - j))
      }
      value /= h
      if (value < 0) {
        value = 0
      }
      filtered(i) = value
    }
    filtered
  }

}