package org.apache.sedona.core.serde;

import com.esotericsoftware.kryo.Kryo;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.sedona.core.TestBase;
import org.apache.sedona.core.serde.shape.ShapeGeometrySerde;
import org.apache.sedona.core.serde.spatialindex.SpatialIndexSerde;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.junit.*;
import org.locationtech.jts.geom.Point;

import static org.junit.Assert.assertEquals;

public class SedonaKryoRegistratorTest extends TestBase {

    public static JavaSparkContext sc;
    public static SparkConf conf;

    @BeforeClass
    public static void onceExecutedBeforeAll()
    {
        conf = new SparkConf()
                .setAppName(SedonaKryoRegistratorTest.class.getName())
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
    public void testRegistration() {
        conf.set("spark.kryo.registrator", SedonaKryoRegistrator.class.getName());
        sc = new JavaSparkContext(conf);

        Kryo kryo = new Kryo();
        ShapeGeometrySerde shapeGeometrySerde = new ShapeGeometrySerde();
        SpatialIndexSerde indexSerializer = new SpatialIndexSerde();

        SedonaKryoRegistratorHelper.registerClasses(kryo, shapeGeometrySerde, indexSerializer);

        assertEquals(ShapeGeometrySerde.class.getName(), kryo.getSerializer(Point.class).getClass().getName());
    }
}
