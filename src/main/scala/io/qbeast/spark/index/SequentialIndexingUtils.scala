/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import io.qbeast.core.model.{CubeId, Revision}
import io.qbeast.spark.index.QbeastColumns.cubeColumnName
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, struct, udf}

object SequentialIndexingUtils {

  def computeTreeDepth(elementCount: Long, desiredCubeSize: Long, dimensionCount: Int): Int = {
    // Low fanout to account for imbalanced datasets
    val avgFanoutEst = 1.05 // math.pow(2, dimensionCount)
    val cubeCount = elementCount.toDouble / desiredCubeSize
    val treeDepthEst = logOfBase(avgFanoutEst, 1 - cubeCount * (1 - avgFanoutEst)) - 1

    treeDepthEst.toInt + 1
  }

  def logOfBase(base: Double, value: Double): Double = {
    math.log10(value) / math.log10(base)
  }

  def addCubeId(revision: Revision, maxDepth: Int): DataFrame => DataFrame =
    (df: DataFrame) => {
      val rowStruct = struct(
        revision.columnTransformers.map(column => col(column.columnName)): _*)
      val pointCubeMapper = new PointCubeMapper(revision, maxDepth)
      df.withColumn(cubeColumnName, pointCubeMapper.cubeFromTargetDepth(rowStruct))
    }

  val cubeStringToBytes: UserDefinedFunction = {
    udf((levelCube: String, dimensionCount: Int) => CubeId(dimensionCount, levelCube).bytes)
  }

}

class PointCubeMapper(revision: Revision, maxDepth: Int) extends Serializable {

  val cubeFromTargetDepth: UserDefinedFunction = {
    udf((row: Row) => {
      val point = RowUtils.rowValuesToPoint(row, revision)
      CubeId.container(point, maxDepth).string
    })
  }

}
