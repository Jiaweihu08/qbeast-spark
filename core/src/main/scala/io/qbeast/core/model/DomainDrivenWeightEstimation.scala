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

import scala.collection.mutable

trait DomainDrivenWeightEstimation {

  def computeCubeMaxWeightsFromIndexStatus(indexStatus: IndexStatus): Map[CubeId, Weight] = {
    val cubeDomains = computeCubeDomainsFromIndexStatus(indexStatus)
    computeCubeMaxWeightsFromDomains(cubeDomains, indexStatus.revision.desiredCubeSize)
  }

  /**
   * Populate updated NormalizedWeights in a top-down fashion using the input cube domains:
   * c_maxWeight = c_minWeight + desiredCubeSize / domainSize.
   * @param cubeDomains
   *   updated cube domains
   * @param cubeSize
   *   the target cube size to use
   * @return
   */
  def computeCubeMaxWeightsFromDomains(
      cubeDomains: Map[CubeId, Double],
      cubeSize: Int): Map[CubeId, Weight] = {
    var cubeMaxWeights = Map.empty[CubeId, NormalizedWeight]
    cubeDomains.toSeq.sorted.foreach { case (cube, domain) =>
      if (!hasLeafAncestor(cube, cubeMaxWeights)) {
        val minWeight = if (cube.isRoot) 0d else cubeMaxWeights(cube.parent.get)
        val maxWeight = minWeight + cubeSize / domain
        cubeMaxWeights += (cube -> maxWeight)
      }
    }
    cubeMaxWeights.map { case (cubeId, nw) => (cubeId, NormalizedWeight.toWeight(nw)) }
  }

  /**
   * Avoid computing the weight for an input CubeId if any of its ancestors is leaf.
   * @param cube
   *   the input CubeId
   * @param normalizedWeights
   *   computed NormalizedWeights
   * @return
   */
  private[model] def hasLeafAncestor(
      cube: CubeId,
      normalizedWeights: Map[CubeId, NormalizedWeight]): Boolean = {
    if (cube.isRoot) false
    else if (!normalizedWeights.contains(cube.parent.get)) true
    else normalizedWeights(cube.parent.get) >= 1d
  }

  /**
   * Compute domain sizes for each cube from the index. The domain of a given cube c is computed
   * as a fraction f of its parent domain, with f being the ratio between c's tree size and its
   * parent's SUBTREE size. The output map contains the domain sizes of all cubes with no broken
   * branches.
   */
  def computeCubeDomainsFromIndexStatus(indexStatus: IndexStatus): Map[CubeId, Double] = {
    val treeSizes = computeCubeTreeSizesFromIndexStatus(indexStatus)
    val cubeDomains = mutable.Map.empty[CubeId, Double]
    treeSizes.keys.toSeq.sorted.foreach { cube =>
      if (cube.isRoot) {
        // The root domain coincides with its tree size
        cubeDomains += (cube -> treeSizes(cube))
      }
      // Compute the domain of the children
      val children = cube.children.filter(treeSizes.contains).toSeq
      if (children.nonEmpty) {
        val cubeDomain = cubeDomains(cube)
        val childTreeSizes = children.map(c => (c, treeSizes(c)))
        val subtreeSize = childTreeSizes.map(_._2).sum
        childTreeSizes.foreach { case (child, ts) =>
          val f = ts / subtreeSize
          val childDomain = f * cubeDomain
          cubeDomains += (child -> childDomain)
        }
      }
    }
    cubeDomains.toMap
  }

  /**
   * Compute the index cube tree sizes. The process starts from the leaves and goes up to the
   * root, computing the tree size of each cube as the sum of the tree sizes of its children plus
   * its own element counts. The tree size of a leaf is the number of elements in the cube. If a
   * cube is not in the index, its element counts is 0, and its tree size will simply be the sum
   * of that of its children. The output map contains the tree sizes of all cubes with no broken
   * branches.
   */
  def computeCubeTreeSizesFromIndexStatus(indexStatus: IndexStatus): Map[CubeId, Double] = {
    val cubeElementCounts = indexStatus.cubeElementCounts()
    var treeSizes = cubeElementCounts.mapValues(_.toDouble)
    val cubes = new mutable.PriorityQueue()(Ordering.by[CubeId, Int](_.depth))
    cubeElementCounts.keys.foreach(cubes.enqueue(_))
    while (cubes.nonEmpty) {
      val cube = cubes.dequeue()
      val treeSize = treeSizes(cube)
      if (!cube.isRoot) {
        val parent = cube.parent.get
        val updatedParentTreeSize =
          if (treeSizes.contains(parent)) {
            treeSizes(parent) + treeSize
          } else {
            // Add the tree size of the parent to the queue
            cubes.enqueue(parent)
            treeSize
          }
        treeSizes += (parent -> updatedParentTreeSize)
      }
    }
    treeSizes
  }

}
