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

package org.apache.sedona.core.serde;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.sedona.core.geometryObjects.Circle;
import org.apache.sedona.core.serde.WKB.WKBGeometrySerde;
import org.apache.sedona.core.serde.shape.ShapeGeometrySerde;
import org.apache.sedona.core.utils.GeomUtils;
import org.junit.Ignore;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import java.io.ByteArrayOutputStream;

import static org.junit.Assert.assertTrue;

/**
 * To add serde test case, you need to add the following functions: (WKB for instance)
 * 1. @Test testUsingWKBSerde
 * 2. private testUsingWKBSerde
 * 3. wkbSerde
 */
public class GeometrySerdeTest
{
    private final Kryo kryo = new Kryo();
    private final WKTReader wktReader = new WKTReader();

    private final String POINT = "POINT (1.3 4.5)";
    private final String MULTIPOINT = "MULTIPOINT ((1 1), (1.3 4.5), (5.2 999))";
    private final String LINESTRING = "LINESTRING (1 1, 1.3 4.5, 5.2 999)";
    private final String MULTILINESTRING = "MULTILINESTRING ((1 1, 1.3 4.5, 5.2 999), (0 0, 0 1))";
    private final String POLYGON = "POLYGON ((0 0, 0 1, 1 1, 1 0.4, 0 0))";
    private final String POLYGON_WITH_HOLE = "POLYGON ((0 0, 0 1, 1 1, 1 0.4, 0 0), " +
            "(0.2 0.2, 0.5 0.2, 0.5 0.5, 0.2 0.5, 0.2 0.2))";
    private final String MULTIPOLYGON = "MULTIPOLYGON (((0 0, 0 1, 1 1, 1 0.4, 0 0)), " +
            "((0 0, 0 1, 1 1, 1 0.4, 0 0), (0.2 0.2, 0.5 0.2, 0.5 0.5, 0.2 0.5, 0.2 0.2)))";
    private final String GEOMETRYCOLLECTION = "GEOMETRYCOLLECTION (POINT(4 6), LINESTRING(4 6,7 10))";

    @Test
    public void testUsingShapeSerde()
            throws Exception
    {
        testUsingShapeSerde(POINT);
        testUsingShapeSerde(MULTIPOINT);
        testUsingShapeSerde(LINESTRING);
        testUsingShapeSerde(MULTILINESTRING);
        testUsingShapeSerde(POLYGON);
        testUsingShapeSerde(POLYGON_WITH_HOLE);
        testUsingShapeSerde(MULTIPOLYGON);
        testUsingShapeSerde(GEOMETRYCOLLECTION);
    }

    @Test
    public void testUsingWKBSerde()
            throws Exception
    {
        testUsingWKBSerde(POINT);
        testUsingWKBSerde(MULTIPOINT);
        testUsingWKBSerde(LINESTRING);
        testUsingWKBSerde(MULTILINESTRING);
        testUsingWKBSerde(POLYGON);
        testUsingWKBSerde(POLYGON_WITH_HOLE);
        testUsingWKBSerde(MULTIPOLYGON);
        testUsingWKBSerde(GEOMETRYCOLLECTION);
    }

    private void testUsingShapeSerde(String wkt)
            throws Exception
    {
        Geometry geometry = parseWkt(wkt);
        assertTrue(GeomUtils.equalsExactGeom(geometry, shapeSerde(geometry)));

        geometry.setUserData("This is a test");
        assertTrue(GeomUtils.equalsExactGeom(geometry, shapeSerde(geometry)));

        if (geometry instanceof GeometryCollection) {
            return;
        }

        Circle circle = new Circle(geometry, 1.2);
        assertTrue(GeomUtils.equalsExactGeom(circle, shapeSerde(circle)));
    }

    private void testUsingWKBSerde(String wkt)
            throws Exception
    {
        Geometry geometry = parseWkt(wkt);
        assertTrue(GeomUtils.equalsExactGeom(geometry, wkbSerde(geometry)));

        geometry.setUserData("This is a test");
        assertTrue(GeomUtils.equalsExactGeom(geometry, wkbSerde(geometry)));

        if (geometry instanceof GeometryCollection) {
            return;
        }

        Circle circle = new Circle(geometry, 1.2);
        assertTrue(GeomUtils.equalsExactGeom(circle, wkbSerde(circle)));
    }

    private Geometry parseWkt(String wkt)
            throws ParseException
    {
        return wktReader.read(wkt);
    }

    private Geometry shapeSerde(Geometry input)
    {
        ShapeGeometrySerde serde = new ShapeGeometrySerde();
        byte[] ser = serialize(input, serde);
        return kryo.readObject(new Input(ser), input.getClass());
    }

    private Geometry wkbSerde(Geometry input)
    {
        WKBGeometrySerde serde = new WKBGeometrySerde();
        byte[] ser = serialize(input, serde);
        return kryo.readObject(new Input(ser), input.getClass());
    }

    private byte[] serialize(Geometry input, GeometrySerde serde)
    {
        kryo.register(input.getClass(), serde);

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        Output output = new Output(bos);
        kryo.writeObject(output, input);
        output.close();
        return bos.toByteArray();
    }
}
