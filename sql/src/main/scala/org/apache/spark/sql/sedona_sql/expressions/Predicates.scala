/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.spark.sql.sedona_sql.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.sedona_sql.sedonaSerializer
import org.apache.spark.sql.types.BooleanType

abstract class ST_Predicate extends Expression

/**
  * Test if leftGeometry full contains rightGeometry
  *
  * @param inputExpressions
  */
case class ST_Contains(inputExpressions: Seq[Expression])
  extends ST_Predicate with CodegenFallback {

  // This is a binary expression
  assert(inputExpressions.length == 2)

  override def nullable: Boolean = false

  override def toString: String = s" **${ST_Contains.getClass.getName}**  "

  override def children: Seq[Expression] = inputExpressions

  override def eval(inputRow: InternalRow): Any = {
    val leftArray = inputExpressions(0).eval(inputRow).asInstanceOf[ArrayData]
    val rightArray = inputExpressions(1).eval(inputRow).asInstanceOf[ArrayData]

    val leftGeometry = sedonaSerializer.deserialize(leftArray)

    val rightGeometry = sedonaSerializer.deserialize(rightArray)

    leftGeometry.covers(rightGeometry)
  }

  override def dataType = BooleanType
}

/**
  * Test if leftGeometry full intersects rightGeometry
  *
  * @param inputExpressions
  */
case class ST_Intersects(inputExpressions: Seq[Expression])
  extends ST_Predicate with CodegenFallback {
  override def nullable: Boolean = false

  // This is a binary expression
  assert(inputExpressions.length == 2)

  override def toString: String = s" **${ST_Intersects.getClass.getName}**  "

  override def children: Seq[Expression] = inputExpressions

  override def eval(inputRow: InternalRow): Any = {
    val leftArray = inputExpressions(0).eval(inputRow).asInstanceOf[ArrayData]
    val rightArray = inputExpressions(1).eval(inputRow).asInstanceOf[ArrayData]

    val leftGeometry = sedonaSerializer.deserialize(leftArray)

    val rightGeometry = sedonaSerializer.deserialize(rightArray)

    leftGeometry.intersects(rightGeometry)
  }

  override def dataType = BooleanType
}

/**
  * Test if leftGeometry is full within rightGeometry
  *
  * @param inputExpressions
  */
case class ST_Within(inputExpressions: Seq[Expression])
  extends ST_Predicate with CodegenFallback {
  override def nullable: Boolean = false

  // This is a binary expression
  assert(inputExpressions.length == 2)

  override def toString: String = s" **${ST_Intersects.getClass.getName}**  "

  override def children: Seq[Expression] = inputExpressions

  override def eval(inputRow: InternalRow): Any = {
    val leftArray = inputExpressions(0).eval(inputRow).asInstanceOf[ArrayData]
    val rightArray = inputExpressions(1).eval(inputRow).asInstanceOf[ArrayData]

    val leftGeometry = sedonaSerializer.deserialize(leftArray)

    val rightGeometry = sedonaSerializer.deserialize(rightArray)

    leftGeometry.coveredBy(rightGeometry)
  }

  override def dataType = BooleanType
}


/**
  * Test if leftGeometry crosses rightGeometry
  *
  * @param inputExpressions
  */
case class ST_Crosses(inputExpressions: Seq[Expression])
  extends ST_Predicate with CodegenFallback {
  override def nullable: Boolean = false

  override def toString: String = s" **${ST_Crosses.getClass.getName}**  "

  override def children: Seq[Expression] = inputExpressions

  override def eval(inputRow: InternalRow): Any = {
    assert(inputExpressions.length == 2)

    val leftArray = inputExpressions(0).eval(inputRow).asInstanceOf[ArrayData]
    val rightArray = inputExpressions(1).eval(inputRow).asInstanceOf[ArrayData]

    val leftGeometry = sedonaSerializer.deserialize(leftArray)

    val rightGeometry = sedonaSerializer.deserialize(rightArray)

    leftGeometry.crosses(rightGeometry)
  }

  override def dataType = BooleanType
}


/**
  * Test if leftGeometry overlaps rightGeometry
  *
  * @param inputExpressions
  */
case class ST_Overlaps(inputExpressions: Seq[Expression])
  extends ST_Predicate with CodegenFallback {
  override def nullable: Boolean = false

  // This is a binary expression
  assert(inputExpressions.length == 2)

  override def toString: String = s" **${ST_Overlaps.getClass.getName}**  "

  override def children: Seq[Expression] = inputExpressions

  override def eval(inputRow: InternalRow): Any = {
    val leftArray = inputExpressions(0).eval(inputRow).asInstanceOf[ArrayData]
    val rightArray = inputExpressions(1).eval(inputRow).asInstanceOf[ArrayData]

    val leftGeometry = sedonaSerializer.deserialize(leftArray)

    val rightGeometry = sedonaSerializer.deserialize(rightArray)

    leftGeometry.overlaps(rightGeometry)
  }

  override def dataType = BooleanType
}

/**
  * Test if leftGeometry touches rightGeometry
  *
  * @param inputExpressions
  */
case class ST_Touches(inputExpressions: Seq[Expression])
  extends ST_Predicate with CodegenFallback {
  override def nullable: Boolean = false

  // This is a binary expression
  assert(inputExpressions.length == 2)

  override def toString: String = s" **${ST_Touches.getClass.getName}**  "

  override def children: Seq[Expression] = inputExpressions

  override def eval(inputRow: InternalRow): Any = {
    val leftArray = inputExpressions(0).eval(inputRow).asInstanceOf[ArrayData]
    val rightArray = inputExpressions(1).eval(inputRow).asInstanceOf[ArrayData]

    val leftGeometry = sedonaSerializer.deserialize(leftArray)

    val rightGeometry = sedonaSerializer.deserialize(rightArray)

    leftGeometry.touches(rightGeometry)
  }

  override def dataType = BooleanType
}

/**
  * Test if leftGeometry is equal to rightGeometry
  *
  * @param inputExpressions
  */
case class ST_Equals(inputExpressions: Seq[Expression])
  extends ST_Predicate with CodegenFallback {
  override def nullable: Boolean = false

  // This is a binary expression
  assert(inputExpressions.length == 2)

  override def toString: String = s" **${ST_Equals.getClass.getName}**  "

  override def children: Seq[Expression] = inputExpressions

  override def eval(inputRow: InternalRow): Any = {
    val leftArray = inputExpressions(0).eval(inputRow).asInstanceOf[ArrayData]

    val rightArray = inputExpressions(1).eval(inputRow).asInstanceOf[ArrayData]

    val leftGeometry = sedonaSerializer.deserialize(leftArray)

    val rightGeometry = sedonaSerializer.deserialize(rightArray)

    // Returns GeometryCollection object
    val symDifference = leftGeometry.symDifference(rightGeometry)

    val isEqual = symDifference.isEmpty

    isEqual
  }

  override def dataType = BooleanType
}