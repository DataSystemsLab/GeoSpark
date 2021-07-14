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

package org.apache.sedona.core.serde.shape;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.log4j.Logger;
import org.apache.sedona.core.enums.SerializerType;
import org.apache.sedona.core.formatMapper.shapefileParser.parseUtils.shp.ShapeSerde;
import org.apache.sedona.core.serde.GeometrySerde;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

/**
 * Specific implementation of GeometrySerde using the Shape format
 */
public class ShapeGeometrySerde
        extends GeometrySerde
{
    @Override
    protected void writeGeometry(Kryo kryo, Output out, Geometry geometry)
    {
        // Mainly used by the python binding to know in which serde to use
        writeSerializedType(out, SerializerType.SHAPE);

        byte[] data = ShapeSerde.serialize(geometry);
        out.write(data, 0, data.length);
        writeUserData(kryo, out, geometry);
    }

    @Override
    protected Geometry readGeometry(Kryo kryo, Input input)
    {
        // Skip the unneeded Serialized Type
        input.readInt(true);

        Geometry geometry = ShapeSerde.deserialize(input, geometryFactory);
        geometry.setUserData(readUserData(kryo, input));
        return geometry;
    }
}
