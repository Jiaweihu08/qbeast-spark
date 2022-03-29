/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import io.qbeast.IISeq
import io.qbeast.core.model.{DoubleDataType, OrderedDataType}
import io.qbeast.core.transform.{HashTransformation, LinearTransformation, Transformation}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.AccumulatorV2

object SinglePassColStatsUtils {

  val supportedTypes = Set(
    "DoubleDataType",
    "DecimalDataType",
    "StringDataType",
    "IntegerDataType",
    "FloatDataType",
    "LongDataType")

  def initializeColStats(columnsToIndex: Seq[String], schema: StructType): Seq[ColStats] = {
    columnsToIndex.map { name =>
      val dType = SparkRevisionFactory.getColumnQType(name, schema).name
      require(supportedTypes.contains(dType), s"Type: $dType is not currently supported.")
      ColStats(name, dType)
    }
  }

  def mergeColStats(global: ColStats, local: ColStats): ColStats = {
    assert(global.colName == local.colName && global.dType == local.dType)
    if (global.dType == "StringDataType") {
      global
    } else {
      global.copy(min = global.min.min(local.min), max = global.max.max(local.max))
    }
  }

  def updateColStats(stats: ColStats, row: Row): ColStats = {
    if (stats.dType == "StringDataType") {
      stats
    } else {
      val value = stats.dType match {
        case "DoubleDataType" => row.getAs[Double](stats.colName)
        case "IntegerDataType" => row.getAs[Int](stats.colName)
        case "FloatDataType" => row.getAs[Float](stats.colName)
        case "LongDataType" => row.getAs[Long](stats.colName)
        case "DecimalDataType" =>
          row.getDecimal(row.schema.fieldIndex(stats.colName)).doubleValue()
      }
      stats.copy(min = stats.min.min(value), max = stats.max.max(value))
    }
  }

  def getTransformations(columnStats: Seq[ColStats]): IISeq[Transformation] = {
    columnStats.map { stats =>
      if (stats.dType == "StringDataType") {
        HashTransformation()
      } else {
        val (minNumber, maxNumber) = stats.dType match {
          case "DoubleDataType" | "DecimalDataType" =>
            (stats.min, stats.max)
          case "IntegerDataType" =>
            (stats.min.asInstanceOf[Int], stats.max.asInstanceOf[Int])
          case "FloatDataType" =>
            (stats.min.asInstanceOf[Float], stats.max.asInstanceOf[Float])
          case "LongDataType" =>
            (stats.min.asInstanceOf[Long], stats.max.asInstanceOf[Long])
        }
        if (minNumber == maxNumber) {
          val orderedDataType = OrderedDataType(stats.dType)
          import orderedDataType.ordering._

          val epsilon = 42.0
          LinearTransformation(
            minNumber.toDouble() - epsilon,
            maxNumber.toDouble() + epsilon,
            DoubleDataType)
        } else {
          LinearTransformation(minNumber, maxNumber, OrderedDataType(stats.dType))
        }
      }
    }.toIndexedSeq
  }

  def toGlobalCoordinates(
      cubeFrom: IISeq[Double],
      cubeTo: IISeq[Double],
      localColStats: Seq[ColStats],
      globalColStats: Seq[ColStats]): Seq[(Double, Double)] = {
    cubeFrom.indices.map { i =>
      val local = localColStats(i)
      val global = globalColStats(i)
      val from = cubeFrom(i)
      val to = cubeTo(i)
      if (global.dType == "StringDataType" || global.min == local.min && global.max == local.max) {
        (from, to)
      } else {
        val (gScale, lScale) = (global.max - global.min, local.max - local.min)
        val scale = lScale / gScale
        val offset = (local.min - global.min) / gScale
        (from * scale + offset, to * scale + offset)
      }
    }
  }

}

class ColStatsAccumulator(colStats: Seq[ColStats])
    extends AccumulatorV2[Seq[ColStats], Seq[ColStats]] {

  private var columnStats: Seq[ColStats] = colStats

  override def isZero: Boolean = {
    columnStats.forall { stats => stats.min == Double.MaxValue && stats.max == Double.MinValue }
  }

  override def copy(): AccumulatorV2[Seq[ColStats], Seq[ColStats]] = this

  override def reset(): Unit = {
    columnStats = columnStats.map { stats =>
      stats.copy(min = Double.MaxValue, max = Double.MinValue)
    }
  }

  override def add(v: Seq[ColStats]): Unit = {
    columnStats = columnStats.zip(v).map { case (thisColStats, thatColStats) =>
      thisColStats.copy(
        min = thisColStats.min.min(thatColStats.min),
        max = thisColStats.max.max(thatColStats.max))
    }
  }

  override def merge(other: AccumulatorV2[Seq[ColStats], Seq[ColStats]]): Unit = {
    this.add(other.value)
  }

  override def value: Seq[ColStats] = {
    columnStats
  }

}

case class ColStats(
    colName: String,
    dType: String,
    min: Double = Double.MaxValue,
    max: Double = Double.MinValue)
    extends Serializable

case class CubeWeightAndStats(
    cubeBytes: Array[Byte],
    normalizedWeight: NormalizedWeight,
    colStats: Seq[ColStats])

case class CubeAndOverlap(cubeBytes: Array[Byte], overlap: Double)
