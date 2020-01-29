import os

from geo_pyspark.core.enums import FileDataSplitter, GridType, IndexType
from geo_pyspark.core.enums.join_build_side import JoinBuildSide
from geo_pyspark.core.geom.envelope import Envelope
from geo_pyspark.core.spatialOperator import JoinQuery
from geo_pyspark.core.spatialOperator.join_params import JoinParams
from tests.spatial_operator.test_join_base import TestJoinBase
from tests.tools import tests_path

input_location = os.path.join(tests_path, "resources/zcta510-small.csv")
query_window_set = os.path.join(tests_path, "resources/zcta510-small.csv")
offset = 0
splitter = FileDataSplitter.CSV
distance = 0.001
queryPolygonSet = os.path.join(tests_path, "resources/primaryroads-polygon.csv")
inputCount = 3000
inputBoundary = Envelope(-171.090042, 145.830505, -14.373765, 49.00127)
match_count = 17599
match_with_original_duplicates_count = 17738


def pytest_generate_tests(metafunc):
    funcarglist = metafunc.cls.params[metafunc.function.__name__]
    argnames = sorted(funcarglist[0])
    metafunc.parametrize(
        argnames, [[funcargs[name] for name in argnames] for funcargs in funcarglist]
    )


parameters = [
    dict(num_partitions=11, use_legacy_apis=True, grid_type=GridType.RTREE),
    dict(num_partitions=11, use_legacy_apis=False, grid_type=GridType.RTREE),
    dict(num_partitions=11, use_legacy_apis=True, grid_type=GridType.QUADTREE),
    dict(num_partitions=11, use_legacy_apis=False, grid_type=GridType.QUADTREE),
    dict(num_partitions=11, use_legacy_apis=False, grid_type=GridType.KDBTREE),
]
params_dyn = [{**param, **{"index_type": IndexType.QUADTREE}} for param in parameters]
params_dyn.extend([{**param, **{"index_type": IndexType.RTREE}} for param in parameters])


class TestRectangleJoin(TestJoinBase):
    params = {
        "test_nested_loop": parameters,
        "test_dynamic_index_int": params_dyn,
        "test_index_int": params_dyn
    }

    def test_nested_loop(self, num_partitions, use_legacy_apis, grid_type):
        query_rdd = self.create_rectangle_rdd(input_location, splitter, num_partitions)
        spatial_rdd = self.create_rectangle_rdd(input_location, splitter, num_partitions)

        self.partition_rdds(query_rdd, spatial_rdd, grid_type, use_legacy_apis)

        result = JoinQuery.SpatialJoinQuery(
            spatial_rdd, query_rdd, False, True).collect()

        count = 0
        for el in result:
            count += el[1].__len__()
        self.sanity_check_join_results(result)
        assert match_count == self.count_join_results(result)

    def test_dynamic_index_int(self, num_partitions, use_legacy_apis, grid_type, index_type):
        query_rdd = self.create_rectangle_rdd(input_location, splitter, num_partitions)
        spatial_rdd = self.create_rectangle_rdd(input_location, splitter, num_partitions)

        self.partition_rdds(query_rdd, spatial_rdd, grid_type, use_legacy_apis)

        join_params = JoinParams(True, index_type, JoinBuildSide.LEFT)
        result = JoinQuery.spatialJoin(query_rdd, spatial_rdd, join_params).collect()

        self.sanity_check_flat_join_results(result)

        expected_count = match_with_original_duplicates_count \
            if self.expect_to_preserve_original_duplicates(grid_type) else match_count

        assert expected_count == result.__len__()

    def test_index_int(self, num_partitions, use_legacy_apis, grid_type, index_type):
        query_rdd = self.create_rectangle_rdd(input_location, splitter, num_partitions)
        spatial_rdd = self.create_rectangle_rdd(input_location, splitter, num_partitions)

        self.partition_rdds(query_rdd, spatial_rdd, grid_type, use_legacy_apis)
        spatial_rdd.buildIndex(index_type, True)

        result = JoinQuery.SpatialJoinQuery(
            spatial_rdd, query_rdd, False, True).collect()

        self.sanity_check_join_results(result)
        assert match_count == self.count_join_results(result)
