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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Tests of CubeWeights.
 */
class PointWeightIndexerTest extends AnyFlatSpec with Matchers {
  private val point = Point(0.66, 0.28)
  private val List(root, id10, id1001) = CubeId.containers(point).take(3).toList

  case class TableChangesTest(mapCubeWeights: Map[CubeId, Weight]) extends TableChanges {
    val isNewRevision: Boolean = false
    val isOptimizeOperation: Boolean = false

    val updatedRevision: Revision =
      Revision(-1L, System.currentTimeMillis(), QTableID(""), 0, Vector.empty, Vector.empty)

    override def cubeWeight(cubeId: CubeId): Option[Weight] = mapCubeWeights.get(cubeId)
    override def inputBlockElementCounts: Map[CubeId, RevisionID] = Map.empty
  }

  "findTargetCubeIds" should "return the root cube if cube weights is empty" in {
    val pwi = new PointWeightIndexer(TableChangesTest(Map.empty))
    pwi.findTargetCubeId(point, Weight(1)) shouldBe root
  }

  it should "return the CubeId with correct maxWeight" in {
    val tc =
      TableChangesTest(Map(root -> Weight(1), id10 -> Weight(2), id1001 -> Weight(3)))
    val pwi = new PointWeightIndexer(tc)
    pwi.findTargetCubeId(point, Weight(2)) shouldBe id10
    pwi.findTargetCubeId(point, Weight(2.5)) shouldBe id1001
  }

}
