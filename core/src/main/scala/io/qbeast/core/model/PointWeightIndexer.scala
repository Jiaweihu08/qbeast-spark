/*
 * Copyright 2021 Qbeast Analytics, S.L.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.qbeast.core.model

/**
 * Indexes a point by its weight and cube transformation.
 * @param tableChanges
 *   the table changes
 */
class PointWeightIndexer(val tableChanges: TableChanges) extends Serializable {

  /**
   * Finds the target cube identifiers for given point with the specified weight.
   *
   * @param point
   *   the point
   * @param weight
   *   the point weight
   * @return
   *   the target cube identifiers
   */
  def findTargetCubeId(point: Point, weight: Weight): CubeId = {
    var result: Option[CubeId] = None
    val containers = CubeId.containers(point)
    var continue = true
    while (continue && containers.hasNext) {
      val cubeId = containers.next()
      tableChanges.cubeWeight(cubeId) match {
        case Some(cubeWeight) if weight <= cubeWeight =>
          result = Some(cubeId)
          continue = false
        case None =>
          result = Some(cubeId)
          continue = false
        case _ => ()
      }
    }
    result.get
  }

}

object PointWeightIndexer {

  /**
   * Builds a new point weight indexer from the table changes
   * @param changes
   *   the table changes
   * @return
   *   the PointWeightIndexer
   */
  def apply(changes: TableChanges): PointWeightIndexer = new PointWeightIndexer(changes)

}
