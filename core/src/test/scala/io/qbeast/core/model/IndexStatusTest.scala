package io.qbeast.core.model

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.SortedMap

class IndexStatusTest extends AnyFlatSpec with Matchers {

  "IndexStatus" should "return index values correctly" in {
    val root = CubeId.root(2)
    val Seq(c1, c2) = root.children.take(2).toList
    val c3 = c1.children.next

    val blocks = Seq(
      Block("mockPath", root, Weight(0), Weight(0.1), 10L),
      Block("mockPath", c1, Weight(0.1), Weight(0.7), 10L),
      Block("mockPath", c2, Weight(0.1), Weight(1.0), 8L),
      Block("mockPath", c3, Weight(0.7), Weight(1.0), 2L))

    val cs = blocks.foldLeft(SortedMap.empty[CubeId, CubeStatus]) { (acc, b) =>
      val cs = CubeStatus(b.cubeId, b.maxWeight, b.maxWeight.fraction, b :: Nil)
      acc + (b.cubeId -> cs)
    }

    val rev = Revision.firstRevision(QTableID(""), 10, Vector.empty, Vector.empty)
    val indexStatus = IndexStatus(rev, cs)
    indexStatus.cubeElementCounts() shouldBe Map(root -> 10L, c1 -> 10L, c2 -> 8L, c3 -> 2L)

    indexStatus.cubeMaxWeights() shouldBe Map(
      root -> Weight(0.1),
      c1 -> Weight(0.7),
      c2 -> Weight(1.0),
      c3 -> Weight(1.0))

    indexStatus.cubeNormalizedWeights() shouldBe Map(
      root -> Weight(0.1).fraction,
      c1 -> Weight(0.7).fraction,
      c2 -> Weight(1.0).fraction,
      c3 -> Weight(1.0).fraction)
  }

}
