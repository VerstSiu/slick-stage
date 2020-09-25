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
package com.ijoic.slickstage.entity

import com.ijoic.slickstage.constants.QueryConstants
import org.apache.commons.lang.StringEscapeUtils

/**
 * Field accessors
 *
 * @author verstsiu created at 2020-08-27 11:14
 */
object FieldAccessors {
  val LONG = FieldAccessor<Long>(
    read = { it.nextLong() },
    write = { it.toString() }
  )
  val OPTIONAL_LONG = FieldAccessor(
    read = { it.nextString()?.toLongOrNull() },
    write = { writeAsNumberOrNull(it) }
  )

  val BOOLEAN = FieldAccessor<Boolean>(
    read = { it.nextBoolean() },
    write = { writeAsBoolean(it) }
  )
  val OPTIONAL_BOOLEAN = FieldAccessor(
    read = { readAsBooleanOrNull(it.nextString()) },
    write = { writeAsBooleanOrNull(it) }
  )

  val INT = FieldAccessor<Int>(
    read = { it.nextInt() },
    write = { it.toString() }
  )
  val OPTIONAL_INT = FieldAccessor(
    read = { it.nextString()?.toIntOrNull() },
    write = { writeAsNumberOrNull(it) }
  )

  val STRING = FieldAccessor<String>(
    read = { it.nextString() },
    write = { writeAsText(it) }
  )
  val OPTIONAL_STRING = FieldAccessor<String?>(
    read = { it.nextString() },
    write = { writeAsTextOrNull(it) }
  )

  val DOUBLE = FieldAccessor<Double>(
    read = { it.nextDouble() },
    write = { it.toString() }
  )
  val OPTIONAL_DOUBLE = FieldAccessor(
    read = { it.nextString()?.toDoubleOrNull() },
    write = { writeAsNumberOrNull(it) }
  )

  val DOUBLE_AS_DECIMAL = FieldAccessor(
    read = { it.nextString().toDouble() },
    write = { it.toBigDecimal().toPlainString() }
  )
  val OPTIONAL_DOUBLE_AS_DECIMAL = FieldAccessor(
    read = { it.nextString()?.toDoubleOrNull() },
    write = { writeAsBigDecimalOrNull(it) }
  )

  val FLOAT = FieldAccessor<Float>(
    read = { it.nextFloat() },
    write = { it.toString() }
  )
  val OPTIONAL_FLOAT = FieldAccessor(
    read = { it.nextString()?.toFloatOrNull() },
    write = { writeAsNumberOrNull(it) }
  )

  val DATETIME_SECONDS = FieldAccessor(
    read = { it.nextLong() },
    write = { writeUnixTimestamp(it) },
    wrapSelect = { "UNIX_TIMESTAMP(`$it`)" }
  )
  val OPTIONAL_DATETIME_SECONDS = FieldAccessor(
    read = { it.nextString()?.toLongOrNull() },
    write = { writeUnixTimestampOrNull(it) },
    wrapSelect = { "UNIX_TIMESTAMP(`$it`)" }
  )

  private const val NULL = QueryConstants.NULL

  /* Read */

  private fun readAsBooleanOrNull(value: String?): Boolean? {
    return when(value?.toIntOrNull()) {
      null -> null
      0 -> false
      1 -> true
      else -> null
    }
  }

  /* Read :END */

  /* Write */

  private fun writeAsText(value: String): String {
    val escape = when {
      value.isEmpty() -> value
      else -> StringEscapeUtils.escapeSql(value)
    }
    return "'$escape'"
  }

  private fun writeAsTextOrNull(value: String?): String {
    if (value == null) {
      return NULL
    }
    return writeAsText(value)
  }

  private fun writeAsNumberOrNull(value: Number?): String {
    if (value == null) {
      return NULL
    }
    return value.toString()
  }

  private fun writeAsBigDecimalOrNull(value: Double?): String {
    if (value == null) {
      return NULL
    }
    return value.toBigDecimal().toPlainString()
  }

  private fun writeAsBoolean(value: Boolean): String {
    return when(value) {
      true -> QueryConstants.TRUE
      else -> QueryConstants.FALSE
    }
  }

  private fun writeAsBooleanOrNull(value: Boolean?): String {
    if (value == null) {
      return NULL
    }
    return writeAsBoolean(value)
  }

  private fun writeUnixTimestamp(value: Long): String {
    return "FROM_UNIXTIME($value)"
  }

  private fun writeUnixTimestampOrNull(value: Long?): String {
    if (value == null) {
      return NULL
    }
    return writeUnixTimestamp(value)
  }

  /* Write :END */

}