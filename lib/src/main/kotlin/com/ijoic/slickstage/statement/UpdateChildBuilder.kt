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

/**
 * Update child builder
 *
 * ",column2=value2.. WHERE condition .."
 *
 * @author verstsiu created at 2020-08-24 11:02
 */
class UpdateChildBuilder internal constructor(
  private val contentBuilder: StringBuilder
) : WhereBuilder(contentBuilder) {

  fun <T> set(field: Field<T>, value: T): UpdateChildBuilder {
    contentBuilder
      .append(',')
      .appendUpdateField(field, value)
    return this
  }

}