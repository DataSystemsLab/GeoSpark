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
package org.apache.spark.sql.sedona_sql.strategy.join

import org.apache.sedona.core.geometryObjects.Circle
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{BindReferences, Expression, UnsafeRow}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}
import org.apache.spark.sql.sedona_sql.sedonaSerializer
import org.locationtech.jts.geom.Geometry

// ST_Distance(left, right) <= radius
// radius can be literal or a computation over 'left'
case class DistanceJoinExec(left: SparkPlan,
                            right: SparkPlan,
                            leftShape: Expression,
                            rightShape: Expression,
                            radius: Expression,
                            intersects: Boolean,
                            extraCondition: Option[Expression] = None)
  extends BinaryExecNode
    with TraitJoinQueryExec
    with Logging {

  private val boundRadius = BindReferences.bindReference(radius, left.output)

  override def toSpatialRddPair(
                                 buildRdd: RDD[UnsafeRow],
                                 buildExpr: Expression,
                                 streamedRdd: RDD[UnsafeRow],
                                 streamedExpr: Expression): (SpatialRDD[Geometry], SpatialRDD[Geometry]) =
    (toCircleRDD(buildRdd, buildExpr), toSpatialRDD(streamedRdd, streamedExpr))

  private def toCircleRDD(rdd: RDD[UnsafeRow], shapeExpression: Expression): SpatialRDD[Geometry] = {
    val spatialRdd = new SpatialRDD[Geometry]
    spatialRdd.setRawSpatialRDD(
      rdd
        .map { x => {
          val shape = sedonaSerializer.deserialize(shapeExpression.eval(x).asInstanceOf[ArrayData])
          val circle = new Circle(shape, boundRadius.eval(x).asInstanceOf[Double])
          circle.setUserData(x.copy)
          circle.asInstanceOf[Geometry]
        }
        }
        .toJavaRDD())
    spatialRdd
  }

}
