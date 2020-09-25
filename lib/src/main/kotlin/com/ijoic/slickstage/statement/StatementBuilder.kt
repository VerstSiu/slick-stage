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
 */
package com.ijoic.slickstage.statement

import com.ijoic.slickstage.entity.Field
import java.lang.StringBuilder

/**
 * Statement builder
 *
 * @author verstsiu created at 2020-08-27 14:39
 */
open class StatementBuilder internal constructor(
  private val contentBuilder: StringBuilder
) {

  internal fun build(): String {
    onPrepareBuild()
    return contentBuilder.toString()
  }

  protected open fun onPrepareBuild() {}

  companion object {
    /**
     * Select fields("SELECT column1, column2 ..")
     */
    @Throws(IllegalArgumentException::class)
    fun select(vararg field: Field<*>): FromBuilder {
      if (field.isEmpty()) {
        throw IllegalArgumentException("Select fields must not empty")
      }
      return FromBuilder(
        StringBuilder()
          .append("SELECT ")
          .append(field.joinToString(",") { it.selectFieldName })
      )
    }

    /**
     * Update table("UPDATE table_name ..")
     */
    fun update(tableName: String): UpdateTopBuilder {
      return UpdateTopBuilder(
        StringBuilder()
          .append("UPDATE `")
          .append(tableName)
          .append('`')
      )
    }

    /**
     * Update table("DELETE FROM table_name ..")
     */
    fun delete(tableName: String): WhereBuilder {
      return WhereBuilder(
        StringBuilder()
          .append("DELETE FROM `")
          .append(tableName)
          .append('`')
      )
    }
  }
}