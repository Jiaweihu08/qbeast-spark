/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.core.transform

import io.qbeast.core.model.{
  DecimalDataType,
  DoubleDataType,
  FloatDataType,
  IntegerDataType,
  LongDataType,
  OrderedDataType,
  QDataType
}

import scala.util.Random

object LinearTransformer extends TransformerType {
  override def transformerSimpleName: String = "linear"

}

/**
 * Linear Transformer specification of a column
 * @param columnName the column name
 * @param dataType the data type of the column
 */
case class LinearTransformer(
    columnName: String,
    override val dataType: QDataType,
    override val optionalNullValue: Option[Any])
    extends Transformer {
  private def colMax = s"${columnName}_max"
  private def colMin = s"${columnName}_min"

  private def getValue(row: Any): Any = {
    row match {
      case d: java.math.BigDecimal => d.doubleValue()
      case other => other
    }
  }

  private def generateRandomNumber(min: Any, max: Any): Any = {
    dataType match {
      case DoubleDataType =>
        min.asInstanceOf[Double] + (Random
          .nextDouble() * (max.asInstanceOf[Double] - min.asInstanceOf[Double]))
      case IntegerDataType =>
        min.asInstanceOf[Int] + (Random
          .nextDouble() * (max.asInstanceOf[Int] - min.asInstanceOf[Int])).toInt
      case LongDataType =>
        min.asInstanceOf[Long] + (Random
          .nextDouble() * (max.asInstanceOf[Long] - min.asInstanceOf[Long])).toLong
      case FloatDataType =>
        min.asInstanceOf[Float] + (Random
          .nextDouble() * (max.asInstanceOf[Float] - min.asInstanceOf[Float])).toFloat
      case DecimalDataType =>
        min.asInstanceOf[Double] + (Random
          .nextDouble() * (max.asInstanceOf[Double] - min.asInstanceOf[Double]))
    }
  }

  override def stats: ColumnStats =
    ColumnStats(
      statsNames = Seq(colMax, colMin),
      statsSqlPredicates = Seq(s"max($columnName) AS $colMax", s"min($columnName) AS $colMin"))

  override def makeTransformation(row: String => Any): Transformation = {
    val min = getValue(row(colMin))
    val max = getValue(row(colMax))
    val nullV = optionalNullValue.getOrElse(generateRandomNumber(min, max))
    val nullValue = getValue(nullV)
    assert(min != null && max != null)
    dataType match {
      case ordered: OrderedDataType =>
        LinearTransformation(min, max, nullValue, ordered)

    }
  }

  override protected def transformerType: TransformerType = LinearTransformer
}
