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

package org.apache.sedona.sql;

import org.apache.sedona.core.enums.GridType;
import org.apache.sedona.core.enums.IndexType;
import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader;
import org.apache.sedona.core.spatialOperator.JoinQuery;
import org.apache.sedona.core.spatialRDD.CircleRDD;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.sedona.sql.utils.Adapter;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;

public class adapterTestJava extends TestBaseWKBSerdeJava {

    @Test
    public void testReadCsv()
    {
        Dataset<Row> df = sparkSession.read().format("csv").option("delimiter", "\t").option("header", "false").load(csvPointInputLocation);
        df.createOrReplaceTempView("inputtable");
        Dataset<Row> spatialDf = sparkSession.sql("select ST_PointFromText(inputtable._c0,\",\") as arealandmark from inputtable");
        SpatialRDD spatialRDD = Adapter.toSpatialRdd(spatialDf, "arealandmark");
        spatialRDD.analyze();
        Adapter.toDf(spatialRDD, sparkSession).show(1);
    }

    @Test
    public void testReadCsvUsingCoordinates()
    {
        Dataset<Row> df = sparkSession.read().format("csv").option("delimiter", ",").option("header", "false").load(csvPointInputLocation);
        df.createOrReplaceTempView("inputtable");
        Dataset<Row> spatialDf = sparkSession.sql("select ST_Point(cast(inputtable._c0 as Decimal(24,20)),cast(inputtable._c1 as Decimal(24,20))) as arealandmark from inputtable");
        SpatialRDD spatialRDD = Adapter.toSpatialRdd(spatialDf, "arealandmark");
        spatialRDD.analyze();
        Adapter.toDf(spatialRDD, sparkSession).show(1);
    }

    @Test
    public void testReadCsvWithIdUsingCoordinates()
    {
        Dataset<Row> df = sparkSession.read().format("csv").option("delimiter", ",").option("header", "false").load(csvPointInputLocation);
        df.createOrReplaceTempView("inputtable");
        Dataset<Row> spatialDf = sparkSession.sql("select ST_Point(cast(inputtable._c0 as Decimal(24,20)),cast(inputtable._c1 as Decimal(24,20))) as arealandmark from inputtable");
        SpatialRDD spatialRDD = Adapter.toSpatialRdd(spatialDf, "arealandmark");
        spatialRDD.analyze();
        Adapter.toDf(spatialRDD, sparkSession).show(1);
    }

    @Test
    public void testReadWkt()
    {
        Dataset<Row> df = sparkSession.read().format("csv").option("delimiter", "\t").option("header", "false").load(mixedWktGeometryInputLocation);
        df.createOrReplaceTempView("inputtable");
        Dataset<Row> spatialDf = sparkSession.sql("select ST_GeomFromWKT(inputtable._c0) as usacounty from inputtable");
        SpatialRDD spatialRDD = Adapter.toSpatialRdd(spatialDf, "usacounty");
        spatialRDD.analyze();
        Adapter.toDf(spatialRDD, sparkSession).show(1);
    }

    @Test
    public void testReadWktWithId()
    {
        Dataset<Row> df = sparkSession.read().format("csv").option("delimiter", "\t").option("header", "false").load(mixedWktGeometryInputLocation);
        df.createOrReplaceTempView("inputtable");
        Dataset<Row> spatialDf = sparkSession.sql("select ST_GeomFromWKT(inputtable._c0) as usacounty from inputtable");
        SpatialRDD spatialRDD = Adapter.toSpatialRdd(spatialDf, "usacounty");
        spatialRDD.analyze();
        Adapter.toDf(spatialRDD, sparkSession).show(1);
    }

    @Test
    public void testReadWkb()
    {
        Dataset<Row> df = sparkSession.read().format("csv").option("delimiter", "\t").option("header", "false").load(mixedWkbGeometryInputLocation);
        df.createOrReplaceTempView("inputtable");
        Dataset<Row> spatialDf = sparkSession.sql("select ST_GeomFromWKB(inputtable._c0) as usacounty from inputtable");
        SpatialRDD spatialRDD = Adapter.toSpatialRdd(spatialDf, "usacounty");
        spatialRDD.analyze();
        Adapter.toDf(spatialRDD, sparkSession).show(1);
    }

    @Test
    public void testReadWkbWithId()
    {
        Dataset<Row> df = sparkSession.read().format("csv").option("delimiter", "\t").option("header", "false").load(mixedWkbGeometryInputLocation);
        df.createOrReplaceTempView("inputtable");
        Dataset<Row> spatialDf = sparkSession.sql("select ST_GeomFromWKB(inputtable._c0) as usacounty from inputtable");
        SpatialRDD spatialRDD = Adapter.toSpatialRdd(spatialDf, "usacounty");
        spatialRDD.analyze();
        Adapter.toDf(spatialRDD, sparkSession).show(1);
    }

    @Test
    public void testReadShapefileToDF()
    {
        SpatialRDD spatialRDD = ShapefileReader.readToGeometryRDD(JavaSparkContext.fromSparkContext(sparkSession.sparkContext()), shapefileInputLocation);
        spatialRDD.analyze();
        Adapter.toDf(spatialRDD, sparkSession).show(1);
    }

    @Test
    public void testSpatialJoinToDataFrame()
            throws Exception
    {
        Dataset<Row> pointCsvDf = sparkSession.read().format("csv").option("delimiter", ",").option("header", "false").load(csvPointInputLocation);
        pointCsvDf.createOrReplaceTempView("pointtable");
        Dataset<Row> pointDf = sparkSession.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as arealandmark from pointtable");
        SpatialRDD pointRDD = Adapter.toSpatialRdd(pointDf, "arealandmark");
        pointRDD.analyze();

        Dataset<Row> polygonWktDf = sparkSession.read().format("csv").option("delimiter", "\t").option("header", "false").load(mixedWktGeometryInputLocation);
        polygonWktDf.createOrReplaceTempView("polygontable");
        Dataset<Row> polygonDf = sparkSession.sql("select ST_GeomFromWKT(polygontable._c0) as usacounty from polygontable");
        SpatialRDD polygonRDD = Adapter.toSpatialRdd(polygonDf, "usacounty");
        polygonRDD.analyze();

        pointRDD.spatialPartitioning(GridType.QUADTREE);
        polygonRDD.spatialPartitioning(pointRDD.getPartitioner());

        pointRDD.buildIndex(IndexType.QUADTREE, true);

        JavaPairRDD joinResultPairRDD = JoinQuery.SpatialJoinQueryFlat(pointRDD, polygonRDD, true, true);

        Dataset joinResultDf = Adapter.toDf(joinResultPairRDD, sparkSession);

        joinResultDf.show(1);
    }

    @Test
    public void testDistanceJoinToDataFrame()
            throws Exception
    {
        Dataset<Row> pointCsvDf = sparkSession.read().format("csv").option("delimiter", ",").option("header", "false").load(csvPointInputLocation);
        pointCsvDf.createOrReplaceTempView("pointtable");
        Dataset<Row> pointDf = sparkSession.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as arealandmark from pointtable");
        SpatialRDD pointRDD = Adapter.toSpatialRdd(pointDf, "arealandmark");
        pointRDD.analyze();

        Dataset<Row> polygonWktDf = sparkSession.read().format("csv").option("delimiter", "\t").option("header", "false").load(mixedWktGeometryInputLocation);
        polygonWktDf.createOrReplaceTempView("polygontable");
        Dataset<Row> polygonDf = sparkSession.sql("select ST_GeomFromWKT(polygontable._c0) as usacounty from polygontable");
        SpatialRDD polygonRDD = Adapter.toSpatialRdd(polygonDf, "usacounty");
        polygonRDD.analyze();

        CircleRDD circleRDD = new CircleRDD(polygonRDD, 0.2);

        pointRDD.spatialPartitioning(GridType.QUADTREE);
        circleRDD.spatialPartitioning(pointRDD.getPartitioner());

        pointRDD.buildIndex(IndexType.QUADTREE, true);

        JavaPairRDD joinResultPairRDD = JoinQuery.DistanceJoinQueryFlat(pointRDD, circleRDD, true, true);

        Dataset joinResultDf = Adapter.toDf(joinResultPairRDD, sparkSession);

        joinResultDf.show(1);
    }
}
