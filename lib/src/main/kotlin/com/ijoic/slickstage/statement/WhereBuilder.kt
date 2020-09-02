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
 * Where builder
 *
 * "WHERE condition .."
 *
 * @author verstsiu created at 2020-08-28 10:51
 */
open class WhereBuilder internal constructor(
  private val contentBuilder: StringBuilder
) : StatementBuilder(contentBuilder) {

  fun <T> where(field: Field<T>, value: T): StatementBuilder {
    contentBuilder
      .append(" WHERE ")
      .appendQueryField(field, value)
    return StatementBuilder(contentBuilder)
  }

  fun where(builder: ConditionBuilder): StatementBuilder {
    contentBuilder.append(" WHERE ")
    builder.fill(contentBuilder)
    return StatementBuilder(contentBuilder)
  }
}