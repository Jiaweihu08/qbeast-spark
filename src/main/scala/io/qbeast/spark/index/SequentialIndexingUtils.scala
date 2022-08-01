package io.qbeast.spark.index

import io.qbeast.core.model.{CubeId, Revision}
import io.qbeast.spark.index.QbeastColumns.cubeColumnName
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, struct, udf}

object SequentialIndexingUtils {

  def calculateTreeHeight(elementCount: Long, desiredCubeSize: Long, dimensionCount: Int): Int = {
    val securitySurplus = 15

    val maxFanout = math.pow(2, dimensionCount)
    val cubeCount = elementCount / desiredCubeSize

    val perfectTreeHeight = logOfBase(maxFanout, 1 - cubeCount * (1 - maxFanout)) - 1

    perfectTreeHeight.toInt + securitySurplus
  }

  def logOfBase(base: Double, value: Double): Double = {
    math.log10(value) / math.log10(base)
  }

  def addCubeId(revision: Revision, treeHeight: Int): DataFrame => DataFrame =
    (df: DataFrame) => {
      val columnsToIndex = revision.columnTransformers.map(_.columnName)
      val pointCubeMapper = new PointCubeMapper(revision, treeHeight)
      df.withColumn(
        cubeColumnName,
        pointCubeMapper.cubeFromTargetDepth(struct(columnsToIndex.map(col): _*)))
    }

  val cubeStringToBytes: UserDefinedFunction = {
    udf((levelCube: String, dimensionCount: Int) => CubeId(dimensionCount, levelCube).bytes)
  }

}

class PointCubeMapper(revision: Revision, targetDepth: Int) extends Serializable {

  val cubeFromTargetDepth: UserDefinedFunction = {
    udf((row: Row) => {
      val point = RowUtils.rowValuesToPoint(row, revision)
      CubeId.container(point, targetDepth).string
    })
  }

}
