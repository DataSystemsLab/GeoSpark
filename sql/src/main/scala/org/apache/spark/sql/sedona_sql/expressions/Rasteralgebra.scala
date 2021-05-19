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
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.{ArrayType, DataType, DoubleType}
import org.apache.spark.unsafe.types.UTF8String
import org.geotools.coverage.grid.io.GridFormatFinder
import org.geotools.coverage.grid.{GridCoordinates2D, GridCoverage2D}
import org.geotools.util.factory.Hints
import org.locationtech.jts.geom.{Coordinate, GeometryFactory}

import java.io.IOException


// Calculate Normalized Difference between two bands
case class rs_NormalizedDifference(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length == 2)
    val band1 = inputExpressions(0).eval(inputRow).asInstanceOf[GenericArrayData].toDoubleArray()
    val band2 = inputExpressions(0).eval(inputRow).asInstanceOf[GenericArrayData].toDoubleArray()
    val ndvi = normalizeddifference(band1, band2)

    new GenericArrayData(ndvi)
  }
  private def normalizeddifference(band1: Array[Double], band2: Array[Double]): Array[Double] = {

    val result = new Array[Double](band1.length)
    for (i <- 0 until band1.length) {
      if (band1(i) == 0) {
        band1(i) = -1
      }
      if (band2(i) == 0) {
        band2(i) = -1
      }

      result(i) = (band2(i) - band1(i)) / (band2(i) + band1(i))
    }

    result

  }

  override def dataType: DataType = ArrayType(DoubleType)

  override def children: Seq[Expression] = inputExpressions
}

// Calculate mean value for a particular band
case class rs_Mean(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length == 1)
    val band1 = inputExpressions(0).eval(inputRow).asInstanceOf[GenericArrayData].toDoubleArray()
    val mean = calculateMean(band1)
    mean
  }

  private def calculateMean(band:Array[Double]):Double = {

    band.toList.sum/band.length
  }


  override def dataType: DataType = DoubleType

  override def children: Seq[Expression] = inputExpressions
}

// Calculate mode of a particular band
case class rs_Mode(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length == 1)
    val band1 = inputExpressions(0).eval(inputRow).asInstanceOf[GenericArrayData].toDoubleArray()
    val mean = calculateMode(band1)
    mean
  }

  private def calculateMode(band:Array[Double]):Array[Double] = {

    val grouped = band.toList.groupBy(x => x).mapValues(_.size)
    val modeValue = grouped.maxBy(_._2)._2
    val modes = grouped.filter(_._2 == modeValue).map(_._1)
    modes.toArray

  }
  override def dataType: DataType = DoubleType

  override def children: Seq[Expression] = inputExpressions
}


// Calculate a eucledian distance between two pixels
case class rs_EucledianDistance(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length > 1 && inputExpressions.length <=5)
    val imageURL = inputExpressions(0).eval(inputRow).asInstanceOf[UTF8String].toString
    val x1 = inputExpressions(1).eval(inputRow).asInstanceOf[Int]
    val y1 = inputExpressions(2).eval(inputRow).asInstanceOf[Int]
    val x2 = inputExpressions(3).eval(inputRow).asInstanceOf[Int]
    val y2 = inputExpressions(4).eval(inputRow).asInstanceOf[Int]
    findDistanceBetweenTwo(imageURL, x1, y1, x2, y2)

  }

  private def findDistanceBetweenTwo(url: String, x1: Int, y1: Int, x2: Int, y2: Int):Double = {

    val format = GridFormatFinder.findFormat(url)
    val hints = new Hints(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, true)
    val reader = format.getReader(url, hints)
    var coverage:GridCoverage2D = null

    try coverage = reader.read(null)
    catch {
      case giveUp: IOException =>
        throw new RuntimeException(giveUp)
    }
    reader.dispose()
    val coordinate2D_source = new GridCoordinates2D(x1, y1)
    val coordinates2D_target = new GridCoordinates2D(x2, y2)
    val result_source = coverage.getGridGeometry.gridToWorld(coordinate2D_source)
    val result_target = coverage.getGridGeometry.gridToWorld(coordinates2D_target)
    val factory = new GeometryFactory
    val point1 = factory.createPoint((new Coordinate(result_source.getOrdinate(0), result_source.getOrdinate(1))))
    val point2 = factory.createPoint((new Coordinate(result_target.getOrdinate(0), result_target.getOrdinate(1))))

    point1.distance(point2)

  }

  override def dataType: DataType = DoubleType

  override def children: Seq[Expression] = inputExpressions
}

// fetch a particular region from a raster image
case class rs_FetchRegion(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length > 1 && inputExpressions.length <=5)
    val band = inputExpressions(0).eval(inputRow).asInstanceOf[GenericArrayData].toDoubleArray()
    val lowX = inputExpressions(1).eval(inputRow).asInstanceOf[Int]
    val lowY = inputExpressions(2).eval(inputRow).asInstanceOf[Int]
    val highX = inputExpressions(3).eval(inputRow).asInstanceOf[Int]
    val highY = inputExpressions(4).eval(inputRow).asInstanceOf[Int]

    regionEnclosed(band, lowX, lowY, highX, highY)

  }

  private def regionEnclosed(Band: Array[Double], lowX: Int, lowY: Int, highX: Int, highY: Int):Array[Double] = {

    val rows, cols = Math.sqrt(Band.length).toInt
    val result2D = Array.ofDim[Int](rows, cols)
    val result1D = new Array[Double]((highX - lowX + 1) * (highY - lowY + 1))

    var i = 0
    while ( {
      i < rows
    }) {
      System.arraycopy(Band, i*cols, result2D(i), 0, cols)
      i += 1
    }

    val k = 0
    for(i<-lowX until highX +1) {
      for(j<-lowY until highY + 1) {
        result1D(k) = result2D(i)(j)
      }
    }

    result1D

  }




  override def dataType: DataType = DoubleType

  override def children: Seq[Expression] = inputExpressions
}