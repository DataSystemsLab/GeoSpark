package org.apache.sedona.core.serde;

import com.esotericsoftware.kryo.Kryo;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.sedona.core.TestBase;
import org.apache.sedona.core.serde.WKB.WKBGeometrySerde;
import org.apache.sedona.core.serde.spatialindex.SpatialIndexSerde;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.junit.*;
import org.locationtech.jts.geom.Point;

import static org.junit.Assert.assertEquals;

public class SedonaWKBKryoRegistratorTest extends TestBase {

    public static JavaSparkContext sc;
    public static SparkConf conf;

    @BeforeClass
    public static void onceExecutedBeforeAll()
    {
        conf = new SparkConf()
                .setAppName(SedonaWKBKryoRegistratorTest.class.getName())
                .setMaster("local[2]")
                .set("spark.serializer", KryoSerializer.class.getName());

        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
    }

    @After
    public void tearDown()
            throws Exception
    {
        sc.stop();
    }

    @Test
    public void testRegistration()
    {
        conf.set("spark.kryo.registrator", SedonaWKBKryoRegistrator.class.getName());
        sc = new JavaSparkContext(conf);

        Kryo kryo = new Kryo();
        WKBGeometrySerde wkbGeometrySerde = new WKBGeometrySerde();
        SpatialIndexSerde indexSerializer = new SpatialIndexSerde();

        SedonaKryoRegistratorHelper.registerClasses(kryo, wkbGeometrySerde, indexSerializer);

        assertEquals(WKBGeometrySerde.class.getName(), kryo.getSerializer(Point.class).getClass().getName());
    }
}
