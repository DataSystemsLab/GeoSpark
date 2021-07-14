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
package org.apache.spark.sql

import org.apache.sedona.core.utils.SedonaConf
import org.apache.sedona.sql.serde.SedonaSerializer
import org.apache.spark.{SparkContext, SparkException}

package object sedona_sql {

  private val sparkContext = SparkContext.getActive.getOrElse(None)
  private val sedonaConf = sparkContext match {
    case sc: SparkContext => new SedonaConf(sc.conf)
    case None => throw new SparkException("There is no active SparkContext. Hence, cannot create SedonaSerializer")
  }

  private val userSerializerType = sedonaConf.getSerializerType
  val sedonaSerializer: SedonaSerializer = SedonaSerializer.apply(userSerializerType)

}
