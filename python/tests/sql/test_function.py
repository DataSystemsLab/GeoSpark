import pyspark
from shapely.wkt import loads

from tests.data import mixed_wkt_geometry_input_location
from tests.test_base import TestBase


class TestPredicateJoin(TestBase):

    def test_st_convex_hull(self):
        polygon_wkt_df = self.spark.read.format("csv").\
            option("delimiter", "\t").\
            option("header", "false").\
            load(mixed_wkt_geometry_input_location)

        polygon_wkt_df.createOrReplaceTempView("polygontable")
        polygon_wkt_df.show()

        polygon_df = self.spark.sql("select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
        polygon_df.createOrReplaceTempView("polygondf")
        polygon_df.show()

        function_df = self.spark.sql("select ST_ConvexHull(polygondf.countyshape) from polygondf")
        function_df.show()

    def test_st_buffer(self):
        polygon_from_wkt = self.spark.read.format("csv").\
            option("delimiter", "\t").\
            option("header", "false").\
            load(mixed_wkt_geometry_input_location)

        polygon_from_wkt.createOrReplaceTempView("polygontable")
        polygon_from_wkt.show()

        polygon_df = self.spark.sql("select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
        polygon_df.createOrReplaceTempView("polygondf")
        polygon_df.show()
        function_df = self.spark.sql("select ST_Buffer(polygondf.countyshape, 1) from polygondf")
        function_df.show()

    def test_st_envelope(self):
        polygon_from_wkt = self.spark.read.format("csv").\
            option("delimiter", "\t").\
            option("header", "false").\
            load(mixed_wkt_geometry_input_location)

        polygon_from_wkt.createOrReplaceTempView("polygontable")
        polygon_from_wkt.show()
        polygon_df = self.spark.sql("select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
        polygon_df.createOrReplaceTempView("polygondf")
        polygon_df.show()
        function_df = self.spark.sql("select ST_Envelope(polygondf.countyshape) from polygondf")
        function_df.show()

    def test_st_centroid(self):
        polygon_wkt_df = self.spark.read.format("csv").\
            option("delimiter", "\t").\
            option("header", "false").\
            load(mixed_wkt_geometry_input_location)

        polygon_wkt_df.createOrReplaceTempView("polygontable")
        polygon_wkt_df.show()
        polygon_df = self.spark.sql("select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
        polygon_df.createOrReplaceTempView("polygondf")
        polygon_df.show()
        function_df = self.spark.sql("select ST_Centroid(polygondf.countyshape) from polygondf")
        function_df.show()

    def test_st_length(self):
        polygon_wkt_df = self.spark.read.format("csv").\
            option("delimiter", "\t").\
            option("header", "false").load(mixed_wkt_geometry_input_location)

        polygon_wkt_df.createOrReplaceTempView("polygontable")
        polygon_wkt_df.show()

        polygon_df = self.spark.sql("select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
        polygon_df.createOrReplaceTempView("polygondf")
        polygon_df.show()

        function_df = self.spark.sql("select ST_Length(polygondf.countyshape) from polygondf")
        function_df.show()

    def test_st_area(self):
        polygon_wkt_df = self.spark.read.format("csv").\
            option("delimiter", "\t").\
            option("header", "false").\
            load(mixed_wkt_geometry_input_location)

        polygon_wkt_df.createOrReplaceTempView("polygontable")
        polygon_wkt_df.show()
        polygon_df = self.spark.sql("select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
        polygon_df.createOrReplaceTempView("polygondf")
        polygon_df.show()
        function_df = self.spark.sql("select ST_Area(polygondf.countyshape) from polygondf")
        function_df.show()

    def test_st_distance(self):
        polygon_wkt_df = self.spark.read.format("csv").\
            option("delimiter", "\t").\
            option("header", "false").\
            load(mixed_wkt_geometry_input_location)

        polygon_wkt_df.createOrReplaceTempView("polygontable")
        polygon_wkt_df.show()

        polygon_df = self.spark.sql("select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
        polygon_df.createOrReplaceTempView("polygondf")
        polygon_df.show()
        function_df = self.spark.sql("select ST_Distance(polygondf.countyshape, polygondf.countyshape) from polygondf")
        function_df.show()

    def test_st_transform(self):
        polygon_wkt_df = self.spark.read.format("csv").\
            option("delimiter", "\t").\
            option("header", "false").\
            load(mixed_wkt_geometry_input_location)

        polygon_wkt_df.createOrReplaceTempView("polygontable")
        polygon_wkt_df.show()
        polygon_df = self.spark.sql("select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
        polygon_df.createOrReplaceTempView("polygondf")
        polygon_df.show()
        try:
            function_df = self.spark.sql("select ST_Transform(polygondf.countyshape, 'epsg:4326','epsg:3857',true, false) from polygondf")
            function_df.show()
        except Exception:
            function_df = self.spark.sql("select ST_Transform(polygondf.countyshape, 'epsg:4326','epsg:3857',true, false) from polygondf")
            function_df.show()

    def test_st_intersection_intersects_but_not_contains(self):
        test_table = self.spark.sql("select ST_GeomFromWKT('POLYGON((1 1, 8 1, 8 8, 1 8, 1 1))') as a,ST_GeomFromWKT('POLYGON((2 2, 9 2, 9 9, 2 9, 2 2))') as b")
        test_table.createOrReplaceTempView("testtable")
        intersect = self.spark.sql("select ST_Intersection(a,b) from testtable")
        assert intersect.take(1)[0][0].wkt == "POLYGON ((2 8, 8 8, 8 2, 2 2, 2 8))"

    def test_st_intersection_intersects_but_left_contains_right(self):
        test_table = self.spark.sql("select ST_GeomFromWKT('POLYGON((1 1, 1 5, 5 5, 1 1))') as a,ST_GeomFromWKT('POLYGON((2 2, 2 3, 3 3, 2 2))') as b")
        test_table.createOrReplaceTempView("testtable")
        intersects = self.spark.sql("select ST_Intersection(a,b) from testtable")
        assert intersects.take(1)[0][0].wkt == "POLYGON ((2 2, 2 3, 3 3, 2 2))"

    def test_st_intersection_intersects_but_right_contains_left(self):
        test_table = self.spark.sql("select ST_GeomFromWKT('POLYGON((2 2, 2 3, 3 3, 2 2))') as a,ST_GeomFromWKT('POLYGON((1 1, 1 5, 5 5, 1 1))') as b")
        test_table.createOrReplaceTempView("testtable")
        intersects = self.spark.sql("select ST_Intersection(a,b) from testtable")
        assert intersects.take(1)[0][0].wkt == "POLYGON ((2 2, 2 3, 3 3, 2 2))"

    def test_st_intersection_not_intersects(self):
        test_table = self.spark.sql("select ST_GeomFromWKT('POLYGON((40 21, 40 22, 40 23, 40 21))') as a,ST_GeomFromWKT('POLYGON((2 2, 9 2, 9 9, 2 9, 2 2))') as b")
        test_table.createOrReplaceTempView("testtable")
        intersects = self.spark.sql("select ST_Intersection(a,b) from testtable")
        assert intersects.take(1)[0][0].wkt == "GEOMETRYCOLLECTION EMPTY"

    def test_st_is_valid(self):
        test_table = self.spark.sql(
            "SELECT ST_IsValid(ST_GeomFromWKT('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (15 15, 15 20, 20 20, 20 15, 15 15))')) AS a, " +
            "ST_IsValid(ST_GeomFromWKT('POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))')) as b"
        )

        assert not test_table.take(1)[0][0]
        assert test_table.take(1)[0][1]

    def test_fixed_null_pointer_exception_in_st_valid(self):
        test_table = self.spark.sql("SELECT ST_IsValid(null)")
        assert test_table.take(1)[0][0] is None

    def test_st_precision_reduce(self):
        test_table = self.spark.sql(
        """SELECT ST_PrecisionReduce(ST_GeomFromWKT('Point(0.1234567890123456789 0.1234567890123456789)'), 8)""")
        test_table.show(truncate=False)
        assert test_table.take(1)[0][0].x == 0.12345679
        test_table = self.spark.sql(
        """SELECT ST_PrecisionReduce(ST_GeomFromWKT('Point(0.1234567890123456789 0.1234567890123456789)'), 11)""")
        test_table.show(truncate=False)
        assert test_table.take(1)[0][0].x == 0.12345678901

    def test_st_is_simple(self):

        test_table = self.spark.sql(
        "SELECT ST_IsSimple(ST_GeomFromText('POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))')) AS a, " +
                "ST_IsSimple(ST_GeomFromText('POLYGON((1 1,3 1,3 3,2 0,1 1))')) as b"
        )
        assert test_table.take(1)[0][0]
        assert not test_table.take(1)[0][1]

    def test_st_as_text(self):
        polygon_wkt_df = self.spark.read.format("csv").\
            option("delimiter", "\t").\
            option("header", "false").\
            load(mixed_wkt_geometry_input_location)

        polygon_wkt_df.createOrReplaceTempView("polygontable")
        polygon_df = self.spark.sql("select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
        polygon_df.createOrReplaceTempView("polygondf")
        wkt_df = self.spark.sql("select ST_AsText(countyshape) as wkt from polygondf")
        assert polygon_df.take(1)[0]["countyshape"].wkt == loads(wkt_df.take(1)[0]["wkt"]).wkt

    def test_st_n_points(self):
        if pyspark.version.__version__[:3] == "2.2":
            pass
        else:
            test = self.spark.sql("SELECT ST_NPoints(ST_GeomFromText('LINESTRING(77.29 29.07,77.42 29.26,77.27 29.31,77.29 29.07)'))")
            assert test.take(1)[0][0] == 4

    def test_st_geometry_type(self):
        if pyspark.version.__version__[:3] == "2.2":
            pass
        else:
            test = self.spark.sql("SELECT ST_GeometryType(ST_GeomFromText('LINESTRING(77.29 29.07,77.42 29.26,77.27 29.31,77.29 29.07)'))")
            assert test.take(1)[0][0].upper() == "ST_LINESTRING"
