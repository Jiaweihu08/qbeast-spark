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

import io.qbeast.core.model.Weight.MaxValue

import scala.collection.mutable

object CubeDomainsBuilder {
  val minGroupCubeSize = 30

  /**
   * Estimates the groupCubeSize depending on the input parameters. The formula to compute the
   * estimated value is the following: numGroups = MAX(numPartitions, (numElements /
   * cubeWeightsBufferCapacity)) groupCubeSize = desiredCubeSize / numGroups
   * @param desiredCubeSize
   *   the desired cube size
   * @param numPartitions
   *   the number of partitions
   * @param numElements
   *   the total number of elements in the input data
   * @param bufferCapacity
   *   buffer capacity; number of elements that fit in memory
   * @return
   *   the estimated groupCubeSize as a Double.
   */
  private def estimateGroupCubeSize(
      desiredCubeSize: Int,
      numPartitions: Int,
      numElements: Long,
      bufferCapacity: Long): Int = {
    val numGroups = Math.max(numPartitions, numElements / bufferCapacity)
    val groupCubeSize = desiredCubeSize / numGroups
    Math.max(minGroupCubeSize, groupCubeSize.toInt)
  }

  def apply(
      existingCubeWeights: Map[CubeId, Weight],
      desiredCubeSize: Int,
      numPartitions: Int,
      numElements: Long,
      bufferCapacity: Long): CubeDomainsBuilder = {
    val groupCubeSize =
      estimateGroupCubeSize(desiredCubeSize, numPartitions, numElements, bufferCapacity)
    val factory = new WeightAndCountFactory(existingCubeWeights)
    new CubeDomainsBuilder(groupCubeSize, bufferCapacity, factory)
  }

}

/**
 * Builder for creating cube domains.
 *
 * @param groupCubeSize
 *   the desired number of elements for each cube in the local index
 * @param bufferCapacity
 *   the buffer capacity to store the cube weights in memory
 * @param weightAndCountFactory
 *   the factory for WeightAndCount
 */
class CubeDomainsBuilder protected (
    private val groupCubeSize: Int,
    private val bufferCapacity: Long,
    private val weightAndCountFactory: WeightAndCountFactory)
    extends Serializable {

  private val byWeight = Ordering.by[PointAndWeight, Weight](_.weight).reverse
  private val queue = new mutable.PriorityQueue[PointAndWeight]()(byWeight)
  private var resultBuffer = Seq.empty[CubeDomain]

  /**
   * Updates the builder with given point with weight.
   *
   * @param point
   *   the point
   * @param weight
   *   the weight
   * @return
   *   this instance
   */
  def update(point: Point, weight: Weight): CubeDomainsBuilder = {
    queue.enqueue(PointAndWeight(point, weight))
    if (queue.size >= bufferCapacity) {
      resultBuffer ++= resultInternal()
    }
    this
  }

  /**
   * Builds the resulting cube domain sequence.
   *
   * @return
   *   the resulting cube domain map
   */
  def result(): Seq[CubeDomain] = {
    resultInternal() ++ resultBuffer
  }

  def resultInternal(): Seq[CubeDomain] = {
    val weightAndSize = computeWeightsAndSizes()
    if (weightAndSize.nonEmpty) computeCubeDomains(weightAndSize)
    else Seq.empty[CubeDomain]
  }

  /**
   * Compute OTree cube weights and sizes.
   */
  private def computeWeightsAndSizes(): Map[CubeId, WeightAndTreeSize] = {
    val weights = mutable.Map.empty[CubeId, WeightAndCount]
    while (queue.nonEmpty) {
      val PointAndWeight(point, weight) = queue.dequeue()
      val containers = CubeId.containers(point)
      var continue = true
      while (continue && containers.hasNext) {
        val cubeId = containers.next()
        val weightAndCount =
          weights.getOrElseUpdate(cubeId, weightAndCountFactory.create(cubeId))
        if (weightAndCount.shouldInclude(weight, groupCubeSize)) {
          weightAndCount.update(weight)
          continue = false
        }
      }
    }

    // Convert cube Weight and size into a Map[CubeId, (NormalizedWeight, Double)] from which
    // the cube domains are computed.
    weights.map { case (cubeId, weightAndCount) =>
      cubeId -> weightAndCount.toWeightAndTreeSize(groupCubeSize)
    }.toMap

  }

  /**
   * Compute cube domain from an unpopulated tree, which has cube NormalizedWeight and cube size.
   * Done in a bottom-up, updating at each step the parent tree size with that of the current
   * cube, and compute cube domain using treeSize / (1d - parentWeight).
   * @param weightsAndTreeSizes
   *   NormalizedWeight and size for each cube. Cube size is constantly updated to reach tree size
   *   before it's used to compute cube domain
   * @return
   *   Sequence of cube bytes and domain
   */
  private def computeCubeDomains(
      weightsAndTreeSizes: Map[CubeId, WeightAndTreeSize]): Seq[CubeDomain] = {
    val cubeDomainBuilder = Seq.newBuilder[CubeDomain]
    cubeDomainBuilder.sizeHint(weightsAndTreeSizes.size)

    // Compute cube domain from bottom-up
    val levelCubes = weightsAndTreeSizes.keys.groupBy(_.depth)
    val minLevel = levelCubes.keys.min
    val maxLevel = levelCubes.keys.max

    (maxLevel until minLevel by -1) foreach { level =>
      levelCubes(level).foreach(cube => {
        cube.parent match {
          case Some(parent) =>
            val cubeTreeSize = weightsAndTreeSizes(cube).treeSize
            val parentInfo = weightsAndTreeSizes(parent)

            // Compute cube domain
            val domain = cubeTreeSize / (1d - parentInfo.weight)
            cubeDomainBuilder += CubeDomain(cube.bytes, domain)

            // Update parent treeSize
            parentInfo.treeSize += cubeTreeSize
          case None =>
        }
      })
    }

    // Top level cube domain = treeSize
    levelCubes(minLevel).foreach { cube =>
      val domain = weightsAndTreeSizes(cube).treeSize
      cubeDomainBuilder += CubeDomain(cube.bytes, domain)
    }

    cubeDomainBuilder.result()
  }

}

/**
 * Factory for WeightAndCount. The creation of which depends on whether the associated CubeId is
 * present in the existing index, if it's a inner or leaf cube.
 *
 * @param existingCubeWeights
 *   Cube Weight of the existing index
 */
private class WeightAndCountFactory(existingCubeWeights: Map[CubeId, Weight]) {

  /**
   * Create an instance of WeightAndCount for a given cube depending on its status in the existing
   * index
   * @param cubeId
   *   CubeId to create a WeightAndCount for
   * @return
   *   WeightAndCount
   */
  def create(cubeId: CubeId): WeightAndCount =
    existingCubeWeights.get(cubeId) match {
      case Some(w: Weight) if NormalizedWeight(w) < 1.0 =>
        // cubeId present in the existing index as an inner cube
        new InnerCubeWeightAndCount(w)
      case Some(_: Weight) =>
        // cubeId present in the existing index as a leaf cube
        val leafSize = 0
        new LeafCubeWeightAndCount(leafSize)
      case None =>
        // cubeId not present in the existing index
        new WeightAndCount(MaxValue, 0)
    }

}

/**
 * Weight and count for a CubeId.
 * @param weight
 *   Weight of the associated CubeId
 * @param count
 *   Element count of the associated CubeId
 */
private class WeightAndCount(var weight: Weight, var count: Int) {

  /**
   * Determines whether an instance, with its instanceWeight, should be included in the associated
   * cube
   * @param instanceWeight
   *   Weight of a given instance
   * @param limit
   *   total capacity of the cube i.e. groupCubeSize
   */
  def shouldInclude(instanceWeight: Weight, limit: Int): Boolean =
    count < limit

  /**
   * Update metadata to include the instance
   * @param instanceWeight
   *   Weight of a given instance
   */
  def update(instanceWeight: Weight): Unit = {
    count += 1
    weight = instanceWeight
  }

  /**
   * Convert to WeightAndTreeSize, with cube size i.e. count as the initial tree size value.
   * @param gcs
   *   groupCubeSize
   */
  def toWeightAndTreeSize(gcs: Int): WeightAndTreeSize = {
    val nw = toNormalizedWeight(gcs)
    new WeightAndTreeSize(nw, cubeSize)
  }

  /**
   * @return
   *   the number of added instances to the associated cube
   */
  def cubeSize: Int = count

  /**
   * Compute NormalizedWeight according to the state of the cube
   * @param gcs
   *   groupCubeSize
   */
  def toNormalizedWeight(gcs: Int): NormalizedWeight =
    if (count == gcs) NormalizedWeight(weight)
    else NormalizedWeight(gcs, cubeSize)

}

/**
 * WeightAndCount for an existing leaf cube. It can accept up to (groupCubeSize - start) records.
 */
private class LeafCubeWeightAndCount(start: Int) extends WeightAndCount(MaxValue, start) {

  override def cubeSize: Int = count - start

}

/**
 * WeightAndCount for an existing inner cube. It can accept up to groupCubeSize records, with the
 * additional constraint of instanceWeight being less than existingWeight.
 * @param existingWeight
 *   Weight for the associated existing CubeId
 */
private class InnerCubeWeightAndCount(existingWeight: Weight)
    extends WeightAndCount(existingWeight, 0) {

  override def shouldInclude(instanceWeight: Weight, limit: Int): Boolean = {
    instanceWeight < existingWeight && count < limit
  }

  override def toNormalizedWeight(gcs: Int): NormalizedWeight = {
    // An existing inner cube should always remain as such
    NormalizedWeight(weight)
  }

}

/**
 * Point, weight and parent cube identifier if available.
 *
 * @param point
 *   the point
 * @param weight
 *   the weight
 */
private case class PointAndWeight(point: Point, weight: Weight)

/**
 * NormalizedWeight and tree size of a given cube, with tree size defined as the sum of its own
 * payload and all that of its descendents. The tree size is initialized with cube size and
 * updated to the tree size during computation before being used.
 * @param weight
 *   NormalizedWeight
 * @param treeSize
 *   Cube tree size
 */
private class WeightAndTreeSize(val weight: NormalizedWeight, var treeSize: Double)

/**
 * Cube bytes and its domain size from a given data partition, with domain defined as the number
 * of records in a partition that has values within a cube's space limits.
 * @param cubeBytes
 *   Array[Byte] unique to a cube
 * @param domain
 *   The number of records in a partition that fit in the said cube
 */
final case class CubeDomain(cubeBytes: Array[Byte], domain: Double)
