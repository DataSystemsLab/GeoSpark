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
import com.esotericsoftware.kryo.Serializer;
import org.apache.log4j.Logger;
import org.apache.sedona.core.geometryObjects.Circle;
import org.apache.sedona.core.serde.shape.ShapeGeometrySerde;
import org.apache.sedona.core.serde.WKB.WKBGeometrySerde;
import org.apache.sedona.core.serde.spatialindex.SpatialIndexSerde;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.index.quadtree.Quadtree;
import org.locationtech.jts.index.strtree.STRtree;

/**
 * Register Kryo classes by the type of geometry serde mechanism
 */
public class SedonaKryoRegistratorHelper {

    final static Logger log = Logger.getLogger(SedonaWKBKryoRegistrator.class);

    public static void registerClasses(Kryo kryo, Serializer geometrySerdeType, SpatialIndexSerde indexSerializer) {
        log.info("Registering custom serializers for geometry types");
        
        kryo.register(Point.class, geometrySerdeType);
        kryo.register(LineString.class, geometrySerdeType);
        kryo.register(Polygon.class, geometrySerdeType);
        kryo.register(MultiPoint.class, geometrySerdeType);
        kryo.register(MultiLineString.class, geometrySerdeType);
        kryo.register(MultiPolygon.class, geometrySerdeType);
        kryo.register(GeometryCollection.class, geometrySerdeType);
        kryo.register(Circle.class, geometrySerdeType);
        kryo.register(Envelope.class, geometrySerdeType);

        kryo.register(Quadtree.class, indexSerializer);
        kryo.register(STRtree.class, indexSerializer);
    }

}
