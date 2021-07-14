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

package org.apache.sedona.sql

import org.apache.sedona.core.formatMapper.GeoJsonReader
import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader
import org.apache.sedona.sql.utils.Adapter

class constructorTestScala extends TestBaseScala {

  describe("Sedona-SQL Constructor Test") {
    it("Passed ST_Point") {

      var pointCsvDF = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPointInputLocation)

      pointCsvDF.createOrReplaceTempView("pointtable")
      var pointDf = sparkSession.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)), cast(pointtable._c1 as Decimal(24,20))) as arealandmark from pointtable")
      assert(pointDf.count() == 1000)

    }

    it("Passed ST_Point by double") {
      val pointDf = sparkSession.sql("SELECT ST_Point(double(1.2345), 2.3456)")
      assert(pointDf.count() == 1)
    }

    it("Passed ST_PolygonFromEnvelope") {
      val polygonDF = sparkSession.sql("select ST_PolygonFromEnvelope(double(1.234),double(2.234),double(3.345),double(3.345))")
      assert(polygonDF.count() == 1)
    }

    it("Passed ST_PointFromText") {
      var pointCsvDF = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(arealmPointInputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")

      var pointDf = sparkSession.sql("select ST_PointFromText(concat(_c0,',',_c1),',') as arealandmark from pointtable")
      assert(pointDf.count() == 121960)
    }

    it("Passed ST_GeomFromWKT") {
      var polygonWktDf = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      var polygonDf = sparkSession.sql("select ST_GeomFromWkt(polygontable._c0) as countyshape from polygontable")
      assert(polygonDf.count() == 100)
    }

    it("Passed ST_GeomFromText") {
      var polygonWktDf = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      var polygonDf = sparkSession.sql("select ST_GeomFromText(polygontable._c0) as countyshape from polygontable")
      assert(polygonDf.count() == 100)
    }

    it("Passed ST_GeomFromWKB") {
      var polygonWkbDf = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(mixedWkbGeometryInputLocation)
      polygonWkbDf.createOrReplaceTempView("polygontable")
      var polygonDf = sparkSession.sql("select ST_GeomFromWKB(polygontable._c0) as countyshape from polygontable")
      assert(polygonDf.count() == 100)
    }

    it("Passed ST_GeomFromGeoJSON") {
      val polygonJsonDf = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(geojsonInputLocation)
      polygonJsonDf.createOrReplaceTempView("polygontable")
      val polygonDf = sparkSession.sql("select ST_GeomFromGeoJSON(polygontable._c0) as countyshape from polygontable")
      assert(polygonDf.count() == 1000)
    }

    it("Passed GeoJsonReader to DataFrame") {
      var spatialRDD = GeoJsonReader.readToGeometryRDD(sparkSession.sparkContext, geojsonInputLocation)
      var spatialDf = Adapter.toDf(spatialRDD, sparkSession)
      assert(spatialDf.count() > 0)
    }

    it("Read shapefile -> DataFrame > RDD -> DataFrame") {
      var spatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, shapefileInputLocation)
      spatialRDD.analyze()
      var df = Adapter.toDf(spatialRDD, sparkSession)
      assert(df.columns(1) == "STATEFP")
      var spatialRDD2 = Adapter.toSpatialRdd(df, "geometry")
      Adapter.toDf(spatialRDD2, sparkSession).show(1)
    }
  }
}
