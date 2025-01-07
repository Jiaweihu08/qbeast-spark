package io.qbeast.core.model

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.SortedMap

class IndexStatusTest extends AnyFlatSpec with Matchers {

  it should "compute tree sizes correctly" in {
    // existing index: Cube(NormalizedWeight, elementCount)
    //            root(0.1, 10)
    //             /        \
    //      c1(0.7, 10)   c2(1.0, 8)
    //           /
    //    c3(1.0, 2)
    val root = CubeId.root(2)
    val Seq(c1, c2) = root.children.take(2).toList
    val c3 = c1.children.next
    val blocks = Seq(
      Block("mockPath", root, Weight(0), Weight(0.1), 10L),
      Block("mockPath", c1, Weight(0.1), Weight(0.7), 10L),
      Block("mockPath", c2, Weight(0.1), Weight(0.99), 8L),
      Block("mockPath", c3, Weight(0.7), Weight(0.99), 2L))
    val cs = blocks.foldLeft(SortedMap.empty[CubeId, CubeStatus]) { (acc, b) =>
      val cs = CubeStatus(b.cubeId, b.maxWeight, b.maxWeight.fraction, b :: Nil)
      acc + (b.cubeId -> cs)
    }

    val rev = Revision.firstRevision(QTableID(""), 10, Vector.empty, Vector.empty)
    val indexStatus = IndexStatus(rev, cs)

    val treeSizes = indexStatus.cubeTreeSizes()
    treeSizes(c3) shouldBe 2d
    treeSizes(c1) shouldBe 12d
    treeSizes(c2) shouldBe 8d
    treeSizes(root) shouldBe 30d
  }

  it should "compute tree sizes for broken branches correctly" in {
    // existing index: Cube(NormalizedWeight, elementCount)
    //                  root(0.1, 10)
    //                 /               \
    //       missing c1(0.3, -)     missing c2(0.5, -)
    //             /        \                     \
    //   c3(0.5, 10)    missing c4(0.6, 10)   c5(1.0, 8)
    //         /              \
    //  c6(1.0, 5)         c7(1.0, 2)
    val root = CubeId.root(2)
    val Seq(c1, c2) = root.children.take(2).toList
    val Seq(c3, c4) = c1.children.take(2).toList
    val c5 = c2.children.next
    val c6 = c3.children.next
    val c7 = c4.children.next
    val blocks = Seq(
      Block("mockPath", root, Weight(0), Weight(0.1), 10L),
      Block("mockPath", c3, Weight(0.3), Weight(0.5), 10L),
      Block("mockPath", c5, Weight(0.5), Weight(0.99), 8L),
      Block("mockPath", c6, Weight(0.5), Weight(0.99), 5L),
      Block("mockPath", c7, Weight(0.6), Weight(0.99), 2L))
    val cs = blocks.foldLeft(SortedMap.empty[CubeId, CubeStatus]) { (acc, b) =>
      val cs = CubeStatus(b.cubeId, b.maxWeight, b.maxWeight.fraction, b :: Nil)
      acc + (b.cubeId -> cs)
    }

    val rev = Revision.firstRevision(QTableID(""), 10, Vector.empty, Vector.empty)
    val indexStatus = IndexStatus(rev, cs)
    val treeSizes = indexStatus.cubeTreeSizes()
    treeSizes(c6) shouldBe 5d
    treeSizes(c7) shouldBe 2d
    treeSizes(c3) shouldBe 15d
    treeSizes(c4) shouldBe 2d
    treeSizes(c5) shouldBe 8d
    treeSizes(c1) shouldBe 17d
    treeSizes(c2) shouldBe 8d
    treeSizes(root) shouldBe 35d
  }

  it should "compute cube domains correctly" in {
    // existing index: Cube(NormalizedWeight, elementCount)
    //            root(0.1, 10)
    //             /        \
    //      c1(0.7, 10)   c2(1.0, 8)
    //           /
    //    c3(1.0, 2)
    val root = CubeId.root(2)
    val Seq(c1, c2) = root.children.take(2).toList
    val c3 = c1.children.next
    val blocks = Seq(
      Block("mockPath", root, Weight(0), Weight(0.1), 10L),
      Block("mockPath", c1, Weight(0.1), Weight(0.7), 10L),
      Block("mockPath", c2, Weight(0.1), Weight(0.99), 8L),
      Block("mockPath", c3, Weight(0.7), Weight(0.99), 2L))
    val cs = blocks.foldLeft(SortedMap.empty[CubeId, CubeStatus]) { (acc, b) =>
      val cs = CubeStatus(b.cubeId, b.maxWeight, b.maxWeight.fraction, b :: Nil)
      acc + (b.cubeId -> cs)
    }

    val rev = Revision.firstRevision(QTableID(""), 10, Vector.empty, Vector.empty)
    val indexStatus = IndexStatus(rev, cs)
    val domains = indexStatus.cubeDomains()
    // tree sizes:
    //            root(30)
    //           /      \
    //      c1(12)     c2(8)
    //       /
    //    c3(2)
    domains(root) shouldBe 30d
    domains(c1) shouldBe domains(root) * (12d / 20)
    domains(c2) shouldBe domains(root) * (8d / 20)
    domains(c3) shouldBe domains(c1)
  }

  it should "compute cube domains for broken branches correctly" in {
    // existing index: Cube(NormalizedWeight, elementCount)
    //                  root(0.1, 10)
    //                 /               \
    //       missing c1(0.3, -)     missing c2(0.5, -)
    //             /        \                     \
    //   c3(0.5, 10)    missing c4(0.6, 10)   c5(1.0, 8)
    //         /              \
    //  c6(1.0, 5)         c7(1.0, 2)
    val root = CubeId.root(2)
    val Seq(c1, c2) = root.children.take(2).toList
    val Seq(c3, c4) = c1.children.take(2).toList
    val c5 = c2.children.next
    val c6 = c3.children.next
    val c7 = c4.children.next
    val blocks = Seq(
      Block("mockPath", root, Weight(0), Weight(0.1), 10L),
      Block("mockPath", c3, Weight(0.3), Weight(0.5), 10L),
      Block("mockPath", c5, Weight(0.5), Weight(0.99), 8L),
      Block("mockPath", c6, Weight(0.5), Weight(0.99), 5L),
      Block("mockPath", c7, Weight(0.6), Weight(0.99), 2L))
    val cs = blocks.foldLeft(SortedMap.empty[CubeId, CubeStatus]) { (acc, b) =>
      val cs = CubeStatus(b.cubeId, b.maxWeight, b.maxWeight.fraction, b :: Nil)
      acc + (b.cubeId -> cs)
    }

    val rev = Revision.firstRevision(QTableID(""), 10, Vector.empty, Vector.empty)
    val indexStatus = IndexStatus(rev, cs)
    val cubeDomains = indexStatus.cubeDomains()
    // tree sizes:
    //                  root(35)
    //                 /       \
    //             c1(17)      c2(8)
    //             /   \         \
    //         c3(15)   c4(2)    c5(8)
    //         /         \
    //      c6(5)        c7(2)
    cubeDomains(root) shouldBe 35d
    cubeDomains(c1) shouldBe cubeDomains(root) * (17d / 25d)
    cubeDomains(c2) shouldBe cubeDomains(root) * (8d / 25d)
    cubeDomains(c3) shouldBe cubeDomains(c1) * (15d / 17d)
    cubeDomains(c4) shouldBe cubeDomains(c1) * (2d / 17d)
    cubeDomains(c5) shouldBe cubeDomains(c2)
    cubeDomains(c6) shouldBe cubeDomains(c3)
    cubeDomains(c7) shouldBe cubeDomains(c4)
  }

}
