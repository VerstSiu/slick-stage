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
import java.lang.StringBuilder

/**
 * Condition builder
 *
 * @author verstsiu created at 2020-08-28 11:00
 */
class ConditionBuilder internal constructor(
  private val contentBuilder: StringBuilder
) {

  fun <T> and(field: Field<T>, value: T): ConditionBuilder {
    contentBuilder
      .append(" AND ")
      .appendQueryField(field, value)
    return this
  }

  fun and(builder: ConditionBuilder): ConditionBuilder {
    contentBuilder.append(" AND (")
    builder.fill(contentBuilder)
    contentBuilder.append(')')
    return this
  }

  fun <T> or(field: Field<T>, value: T): ConditionBuilder {
    contentBuilder
      .append(" OR ")
      .appendQueryField(field, value)
    return this
  }

  fun or(builder: ConditionBuilder): ConditionBuilder {
    contentBuilder.append(" OR (")
    builder.fill(contentBuilder)
    contentBuilder.append(')')
    return this
  }

  /**
   * Fill query conditions to [contentBuilder]
   */
  internal fun fill(contentBuilder: StringBuilder) {
    contentBuilder.append(this.contentBuilder.toString())
  }

  companion object {
    fun <T> field(field: Field<T>, value: T): ConditionBuilder {
      return ConditionBuilder(
        StringBuilder()
          .appendQueryField(field, value)
      )
    }
  }
}