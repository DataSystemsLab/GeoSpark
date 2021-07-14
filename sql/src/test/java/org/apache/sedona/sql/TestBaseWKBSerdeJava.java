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

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.sedona.sql.utils.SedonaSQLRegistrator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * This TestBase class using the WKB Serializer
 */
public class TestBaseWKBSerdeJava {

    public static String resourceFolder = System.getProperty("user.dir") + "/../core/src/test/resources/";
    public static String mixedWktGeometryInputLocation = resourceFolder + "county_small.tsv";
    public static String mixedWkbGeometryInputLocation = resourceFolder + "county_small_wkb.tsv";
    public static String csvPointInputLocation = resourceFolder + "arealm.csv";
    public static String shapefileInputLocation = resourceFolder + "shapefiles/polygon";
    protected static SparkConf conf;
    protected static JavaSparkContext sc;
    protected static SparkSession sparkSession;

    /**
     * Once executed before all using the WKB serde
     */
    @BeforeClass
    public static void onceExecutedBeforeAll()
    {
        conf = new SparkConf().setAppName("adapterTestJava").setMaster("local[2]");
        conf.set("spark.serializer", org.apache.spark.serializer.KryoSerializer.class.getName());
        conf.set("sedona.serializer.type", "wkb");

        sc = new JavaSparkContext(conf);
        sparkSession = new SparkSession(sc.sc());
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
        SedonaSQLRegistrator.registerAll(sparkSession.sqlContext());
    }

    /**
     * Tear down.
     */
    @AfterClass
    public static void TearDown()
    {
        SedonaSQLRegistrator.dropAll(sparkSession);
        sparkSession.stop();
    }
}
