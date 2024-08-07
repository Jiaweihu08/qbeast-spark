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

import io.qbeast.core.transform.LinearTransformation
import io.qbeast.core.transform.Transformation
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class QuerySpaceFromToTest extends AnyFlatSpec with Matchers {

  private val toSpaceCoordinates
      : (Seq[Option[Any]], Seq[Transformation]) => Seq[Option[NormalizedWeight]] =
    (originalValues: Seq[Option[Any]], transformations: Seq[Transformation]) => {
      originalValues.zip(transformations).map {
        case (Some(f), transformation) => Some(transformation.transform(f))
        case _ => None
      }
    }

  "QuerySpaceFromTo" should "intersect correctly" in {
    val cube = CubeId.root(3)
    val from = Seq(Some(1), Some(2), Some(3))
    val to = Seq(Some(2), Some(4), Some(5))
    val transformation = Seq(
      LinearTransformation(Int.MinValue, Int.MaxValue, IntegerDataType),
      LinearTransformation(Int.MinValue, Int.MaxValue, IntegerDataType),
      LinearTransformation(Int.MinValue, Int.MaxValue, IntegerDataType))
    val querySpaceFromTo = QuerySpace(from, to, transformation)

    querySpaceFromTo shouldBe a[QuerySpaceFromTo]
    querySpaceFromTo.intersectsWith(cube) shouldBe true
  }

  it should "exclude beyond left limit" in {
    val cube = CubeId.root(1)
    val transformation = Seq(LinearTransformation(1, 2, IntegerDataType))
    val from = toSpaceCoordinates(Seq(Some(-1)), transformation)
    val to = toSpaceCoordinates(Seq(Some(0)), transformation)
    val querySpaceFromTo = new QuerySpaceFromTo(from, to)

    querySpaceFromTo.intersectsWith(cube) shouldBe false
  }

  it should "exclude beyond right limit" in {
    val cube = CubeId.root(1)
    val transformation = Seq(LinearTransformation(1, 2, IntegerDataType))
    val from = toSpaceCoordinates(Seq(Some(3)), transformation)
    val to = toSpaceCoordinates(Seq(Some(4)), transformation)
    val querySpaceFromTo = new QuerySpaceFromTo(from, to)

    querySpaceFromTo.intersectsWith(cube) shouldBe false
  }

  it should "include the left limit" in {
    val cube = CubeId.root(1)
    val from = Seq(Some(1))
    val to = Seq(Some(1))
    val transformation = Seq(LinearTransformation(1, 2, IntegerDataType))
    val querySpaceFromTo = QuerySpace(from, to, transformation)

    querySpaceFromTo shouldBe a[QuerySpaceFromTo]
    querySpaceFromTo.intersectsWith(cube) shouldBe true
  }

  it should "include the right SPACE limit" in {
    val cube = CubeId.root(1)
    val from = Seq(Some(2))
    val to = Seq(Some(2))
    val transformation = Seq(LinearTransformation(1, 2, IntegerDataType))
    val querySpaceFromTo = QuerySpace(from, to, transformation)

    querySpaceFromTo shouldBe a[QuerySpaceFromTo]
    querySpaceFromTo.intersectsWith(cube) shouldBe true
  }

  it should "include the left limit and beyond" in {
    val cube = CubeId.root(1)
    val from = Seq(Some(0))
    val to = Seq(Some(1))
    val transformation = Seq(LinearTransformation(1, 2, IntegerDataType))
    val querySpaceFromTo = QuerySpace(from, to, transformation)

    querySpaceFromTo shouldBe a[QuerySpaceFromTo]
    querySpaceFromTo.intersectsWith(cube) shouldBe true
  }

  it should "include the right SPACE limit and beyond" in {
    val cube = CubeId.root(1)
    val from = Seq(Some(2))
    val to = Seq(Some(3))
    val transformation = Seq(LinearTransformation(1, 2, IntegerDataType))
    val querySpaceFromTo = QuerySpace(from, to, transformation)

    querySpaceFromTo shouldBe a[QuerySpaceFromTo]
    querySpaceFromTo.intersectsWith(cube) shouldBe true
  }

  it should "include query range to, though not really desired" in {
    // For the implementation of LessThanOrEqual
    val cube = CubeId.root(1)
    val from = Seq(Some(-1))
    val to = Seq(Some(1))
    val transformation = Seq(LinearTransformation(1, 2, IntegerDataType))
    val querySpaceFromTo = QuerySpace(from, to, transformation)

    querySpaceFromTo shouldBe a[QuerySpaceFromTo]
    querySpaceFromTo.intersectsWith(cube) shouldBe true
  }

  it should "throw error if dimensions size is different" in {
    val from = Seq(Some(1), Some(2), Some(3))
    val to = Seq(Some(2), Some(4), Some(5))
    val transformation = Seq(LinearTransformation(Int.MinValue, Int.MaxValue, IntegerDataType))

    a[AssertionError] shouldBe thrownBy(QuerySpace(from, to, transformation))
  }

  it should "throw error if coordinates size is different" in {
    val from = Seq(Some(1))
    val to = Seq(Some(2), Some(4))
    val transformation = Seq(
      LinearTransformation(Int.MinValue, Int.MaxValue, IntegerDataType),
      LinearTransformation(Int.MinValue, Int.MaxValue, IntegerDataType))
    a[AssertionError] shouldBe thrownBy(QuerySpace(from, to, transformation))
  }

  "QuerySpace" should
    "create an empty space when the query space is larger than the revision right limit" in {
      val cube = CubeId.root(3)
      val from = Seq(Some(3))
      val to = Seq(Some(4))
      val transformation = Seq(LinearTransformation(1, 2, IntegerDataType))
      val emptySpace = QuerySpace(from, to, transformation)

      emptySpace shouldBe a[EmptySpace]
      emptySpace.intersectsWith(cube) shouldBe false
    }

  it should
    "create an empty space when the query space is smaller than the revision left limit" in {
      val from = Seq(Some(-1))
      val to = Seq(Some(0))
      val transformation = Seq(LinearTransformation(1, 2, IntegerDataType))
      val emptySpace = QuerySpace(from, to, transformation)

      emptySpace shouldBe a[EmptySpace]
    }

  it should
    "create an AllSpace instance when the query space is identical to the revision space" in {
      val from = Seq(Some(1))
      val to = Seq(Some(2))
      val transformation = Seq(LinearTransformation(1, 2, IntegerDataType))
      val allSpace = QuerySpace(from, to, transformation)

      allSpace shouldBe a[AllSpace]
    }

  it should
    "create an AllSpace instance when the query space contains the revision space" in {
      val cube = CubeId.root(1)
      val from = Seq(Some(-1))
      val to = Seq(Some(3))
      val transformation = Seq(LinearTransformation(1, 2, IntegerDataType))
      val allSpace = QuerySpace(from, to, transformation)

      allSpace shouldBe a[AllSpace]
      allSpace.intersectsWith(cube)
    }

}
