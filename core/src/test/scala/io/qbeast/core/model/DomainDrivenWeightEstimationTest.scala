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

import scala.collection.immutable.SortedMap

class DomainDrivenWeightEstimationTest
    extends AnyFlatSpec
    with Matchers
    with DomainDrivenWeightEstimation {

  "hasLeafAncestor" should "assess if a cube has leaf ancestor" in {
    val root = CubeId.root(2)
    val c1 = root.firstChild
    val c2 = c1.firstChild
    val c3 = c2.firstChild

    hasLeafAncestor(root, Map.empty) shouldBe false

    hasLeafAncestor(c1, Map(root -> 0.9)) shouldBe false

    hasLeafAncestor(c2, Map(root -> 0.9, c1 -> 1d)) shouldBe true

    hasLeafAncestor(c3, Map(root -> 0.9, c1 -> 1d)) shouldBe true
  }

  "computeCubeTreeSizesFromIndexStatus" should "compute tree sizes correctly" in {
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
    val treeSizes = computeCubeTreeSizesFromIndexStatus(indexStatus)
    // tree sizes:
    //                  root(20)
    //                 /       \
    //             c1(12)      c2(8)
    //             /
    //         c3(2)
    treeSizes(c3) shouldBe 2d
    treeSizes(c1) shouldBe 12d
    treeSizes(c2) shouldBe 8d
    treeSizes(root) shouldBe 30d
  }

  it should "compute tree sizes correctly when there are broken branches" in {
    // existing index: Cube(NormalizedWeight, elementCount)
    //                  root(0.1, 10)
    //                 /               \
    //       missing c1(0.3, -)     missing c2(0.5, -)
    //             /        \                     \
    //   c3(0.5, 10)    missing c4(0.6, -)   c5(1.0, 8)
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
    val treeSizes = computeCubeTreeSizesFromIndexStatus(indexStatus)
    // tree sizes:
    //                  root(35)
    //                 /       \
    //             c1(17)      c2(8)
    //             /   \         \
    //         c3(15)   c4(2)    c5(8)
    //         /         \
    //      c6(5)        c7(2)
    treeSizes(c6) shouldBe 5d
    treeSizes(c7) shouldBe 2d
    treeSizes(c3) shouldBe 15d
    treeSizes(c4) shouldBe 2d
    treeSizes(c5) shouldBe 8d
    treeSizes(c1) shouldBe 17d
    treeSizes(c2) shouldBe 8d
    treeSizes(root) shouldBe 35d
  }

  it should "compute tree sizes when there are missing branches" in {
    // existing index: Cube(NormalizedWeight, elementCount)
    //                  missing (0.1, -)
    //                 /             \
    //       missing c1(0.3, -)       c2(0.5, 10)
    //             /        \                 \
    //  missing c3(0.5, -)  c4(0.6, 10)       c5(1.0, 8)
    //         /              \
    // missing c6(1.0, -)     c7(1.0, 2)
    val root = CubeId.root(2)
    val Seq(c1, c2) = root.children.take(2).toList
    val Seq(c3, c4) = c1.children.take(2).toList
    val c5 = c2.children.next
    val c6 = c3.children.next
    val c7 = c4.children.next
    val blocks = Seq(
      Block("mockPath", c2, Weight(0.5), Weight(0.5), 10L),
      Block("mockPath", c4, Weight(0.3), Weight(0.6), 10L),
      Block("mockPath", c5, Weight(0.5), Weight(0.99), 8L),
      Block("mockPath", c7, Weight(0.6), Weight(0.99), 2L))
    val cs = blocks.foldLeft(SortedMap.empty[CubeId, CubeStatus]) { (acc, b) =>
      val cs = CubeStatus(b.cubeId, b.maxWeight, b.maxWeight.fraction, b :: Nil)
      acc + (b.cubeId -> cs)
    }

    val rev = Revision.firstRevision(QTableID(""), 10, Vector.empty, Vector.empty)
    val indexStatus = IndexStatus(rev, cs)
    val treeSizes = computeCubeTreeSizesFromIndexStatus(indexStatus)
    // tree sizes:
    //                  root(30)
    //                 /       \
    //             c1(12)      c2(18)
    //                 \         \
    //                 c4(12)    c5(8)
    //                   \
    //                   c7(2)
    treeSizes(c7) shouldBe 2d
    treeSizes(c4) shouldBe 12d
    treeSizes(c1) shouldBe 12d
    treeSizes(c5) shouldBe 8d
    treeSizes(c2) shouldBe 18d
    treeSizes(root) shouldBe 30d
    // Missing from the index
    treeSizes.contains(c3) shouldBe false
    treeSizes.contains(c6) shouldBe false
  }

  "computeCubeDomainsFromIndexStatus" should "compute cube domains correctly" in {
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
    val domains = computeCubeDomainsFromIndexStatus(indexStatus)
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

  it should "compute cube domains when there are broken branches" in {
    // existing index: Cube(NormalizedWeight, elementCount)
    //                  root(0.1, 10)
    //                 /               \
    //       missing c1(0.3, -)     missing c2(0.5, -)
    //             /        \                     \
    //   c3(0.5, 10)    missing c4(0.6, -)   c5(1.0, 8)
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
    val cubeDomains = computeCubeDomainsFromIndexStatus(indexStatus)
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

  it should "compute cube domains when there are missing branches" in {
    // existing index: Cube(NormalizedWeight, elementCount)
    //                  missing (0.1, -)
    //                 /             \
    //       missing c1(0.3, -)       c2(0.5, 10)
    //             /        \                 \
    //  missing c3(0.5, -)  c4(0.6, 10)       c5(1.0, 8)
    //         /              \
    // missing c6(1.0, -)     c7(1.0, 2)
    val root = CubeId.root(2)
    val Seq(c1, c2) = root.children.take(2).toList
    val Seq(c3, c4) = c1.children.take(2).toList
    val c5 = c2.children.next
    val c6 = c3.children.next
    val c7 = c4.children.next
    val blocks = Seq(
      Block("mockPath", c2, Weight(0.5), Weight(0.5), 10L),
      Block("mockPath", c4, Weight(0.3), Weight(0.6), 10L),
      Block("mockPath", c5, Weight(0.5), Weight(0.99), 8L),
      Block("mockPath", c7, Weight(0.6), Weight(0.99), 2L))
    val cs = blocks.foldLeft(SortedMap.empty[CubeId, CubeStatus]) { (acc, b) =>
      val cs = CubeStatus(b.cubeId, b.maxWeight, b.maxWeight.fraction, b :: Nil)
      acc + (b.cubeId -> cs)
    }

    val rev = Revision.firstRevision(QTableID(""), 10, Vector.empty, Vector.empty)
    val indexStatus = IndexStatus(rev, cs)
    val cubeDomains = computeCubeDomainsFromIndexStatus(indexStatus)
    // tree sizes:
    //                  root(30)
    //                 /       \
    //             c1(12)      c2(18)
    //                 \         \
    //                 c4(12)    c5(8)
    //                   \
    //                   c7(2)
    cubeDomains(root) shouldBe 30d
    cubeDomains(c1) shouldBe cubeDomains(root) * (12d / 30d)
    cubeDomains(c2) shouldBe cubeDomains(root) * (18d / 30d)
    cubeDomains(c4) shouldBe cubeDomains(c1)
    cubeDomains(c5) shouldBe cubeDomains(c2)
    cubeDomains(c7) shouldBe cubeDomains(c4)
    // Missing from the index
    cubeDomains.contains(c3) shouldBe false
    cubeDomains.contains(c6) shouldBe false
  }

}
