/*
 *
 *  Copyright(c) 2020 VerstSiu
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package com.ijoic.slickstage.statement

import com.ijoic.slickstage.entity.Field
import com.ijoic.slickstage.entity.FieldSearch
import java.lang.StringBuilder

/**
 * Insert builder
 *
 * @author verstsiu created at 2020-08-27 11:40
 */
class InsertBuilder(
  private val tableName: String
) {

  private val params = mutableListOf<FieldSearch<*>>()

  fun <T> set(field: Field<T>, value: T): InsertBuilder {
    params.add(FieldSearch(field, value))
    return this
  }

  @Throws(IllegalArgumentException::class)
  internal fun build(): String {
    if (params.isEmpty()) {
      throw IllegalArgumentException("Insert fields must not empty")
    }
    return StringBuilder()
      .append("INSERT INTO `")
      .append(tableName)
      .append("`(")
      .append(
        params.joinToString(",") { "`${it.field.name}`" }
      )
      .append(") VALUES (")
      .append(
        params.joinToString(",") { it.write() }
      )
      .append(')')
      .toString()
  }

  @Throws(IllegalArgumentException::class)
  internal fun toIdentityQuery(field: Field<*>): String {
    if (params.isEmpty()) {
      throw IllegalArgumentException("Insert fields must not empty")
    }
    return StringBuilder()
      .append("SELECT `")
      .append(field.name)
      .append("` FROM `")
      .append(tableName)
      .append("` WHERE ")
      .appendQueryParams(params)
      .append(" ORDER BY `")
      .append(field.name)
      .append("` DESC LIMIT 1")
      .toString()
  }

}