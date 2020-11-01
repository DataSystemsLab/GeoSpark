#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

from typing import List

from pyspark import RDD
from pyspark.sql import DataFrame, SparkSession

from sedona.core.SpatialRDD.spatial_rdd import SpatialRDD
from sedona.core.enums.spatial import SpatialType
from sedona.utils.meta import MultipleMeta


class Adapter(metaclass=MultipleMeta):
    """
    Class which allow to convert between Spark DataFrame and SpatialRDD and reverse.
    """

    @classmethod
    def toRdd(cls, dataFrame: DataFrame) -> 'JvmSpatialRDD':
        from sedona.core.SpatialRDD.spatial_rdd import JvmSpatialRDD
        sc = dataFrame._sc
        jvm = sc._jvm

        srdd = jvm.Adapter.toRdd(dataFrame._jdf)

        return JvmSpatialRDD(srdd.toJavaRDD(), sc, SpatialType.SPATIAL)

    @classmethod
    def toSpatialRdd(cls, dataFrame: DataFrame, geometryFieldName: str) -> SpatialRDD:
        """

        :param dataFrame:
        :param geometryFieldName:
        :return:
        """
        sc = dataFrame._sc
        jvm = sc._jvm

        srdd = jvm.Adapter.toSpatialRdd(dataFrame._jdf, geometryFieldName)

        spatial_rdd = SpatialRDD(sc)
        spatial_rdd.set_srdd(srdd)

        return spatial_rdd

    @classmethod
    def toSpatialRdd(cls, dataFrame: DataFrame):
        """

        :param dataFrame:
        :return:
        """
        sc = dataFrame._sc
        jvm = sc._jvm

        srdd = jvm.Adapter.toSpatialRdd(dataFrame._jdf)

        spatial_rdd = SpatialRDD(sc)
        spatial_rdd.set_srdd(srdd)

        return spatial_rdd

    @classmethod
    def toSpatialRdd(cls, dataFrame: DataFrame, fieldNames: List) -> SpatialRDD:
        """

        :param dataFrame:
        :param geometryFieldName:
        :param fieldNames:
        :return:
        """
        sc = dataFrame._sc
        jvm = sc._jvm

        srdd = jvm.PythonAdapterWrapper.toSpatialRdd(dataFrame._jdf, fieldNames)

        spatial_rdd = SpatialRDD(sc)
        spatial_rdd.set_srdd(srdd)

        return spatial_rdd

    @classmethod
    def toDf(cls, spatialRDD: SpatialRDD, fieldNames: List, sparkSession: SparkSession) -> DataFrame:
        """

        :param spatialRDD:
        :param fieldNames:
        :param sparkSession:
        :return:
        """
        sc = spatialRDD._sc
        jvm = sc._jvm

        jdf = jvm.PythonAdapterWrapper.toDf(spatialRDD._srdd, fieldNames, sparkSession._jsparkSession)

        df = DataFrame(jdf, sparkSession._wrapped)

        return df

    @classmethod
    def toDf(cls, spatialRDD: SpatialRDD, sparkSession: SparkSession) -> DataFrame:
        """

        :param spatialRDD:
        :param sparkSession:
        :return:
        """
        sc = spatialRDD._sc
        jvm = sc._jvm

        jdf = jvm.Adapter.toDf(spatialRDD._srdd, sparkSession._jsparkSession)

        df = DataFrame(jdf, sparkSession._wrapped)

        return df

    @classmethod
    def toDf(cls, spatialPairRDD: RDD, sparkSession: SparkSession):
        """

        :param spatialPairRDD:
        :param sparkSession:
        :return:
        """
        spatial_pair_rdd_mapped = spatialPairRDD.map(
            lambda x: [x[0].geom, *x[0].getUserData().split("\t"), x[1].geom, *x[1].getUserData().split("\t")]
        )
        df = sparkSession.createDataFrame(spatial_pair_rdd_mapped)
        return df

    @classmethod
    def toDf(cls, spatialPairRDD: RDD, leftFieldnames: List, rightFieldNames: List, sparkSession: SparkSession):
        """

        :param spatialPairRDD:
        :param leftFieldnames:
        :param rightFieldNames:
        :param sparkSession:
        :return:
        """

        df = Adapter.toDf(spatialPairRDD, sparkSession)
        columns_length = df.columns.__len__()
        combined_columns = ["geom_1", *leftFieldnames, "geom_2", *rightFieldNames]
        if columns_length == combined_columns.__len__():
            return df.toDF(*combined_columns)
        else:
            raise TypeError("Column length does not match")
