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
package org.apache.sedona.core.enums;

/**
 * Spark Kryo Serializer type
 */
public enum SerializerType {
    
    SHAPE(0),
    WKB(1);

    final int id;

    public int getId() {
        return id;
    }

    SerializerType(int id)
    {
        this.id = id;
    }

    /**
     * Gets the serializer type.
     *
     * @param str the str
     * @return the index type
     */
    public static SerializerType getSerializerType(String str)
    {
        for (SerializerType me : SerializerType.values()) {
            if (me.name().equalsIgnoreCase(str)) { return me; }
        }
        return null;
    }

    public static SerializerType fromId(int id)
    {
        for (SerializerType type : values()) {
            if (type.id == id) {
                return type;
            }
        }
        return null;
    }
}
