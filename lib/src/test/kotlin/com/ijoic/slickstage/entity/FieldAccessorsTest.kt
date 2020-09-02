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

import akka.stream.alpakka.slick.javadsl.SlickRow
import com.ijoic.slickstage.constants.QueryConstants
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import org.junit.Test
import slick.jdbc.PositionedResult
import java.sql.ResultSet

/**
 * Field accessors test
 *
 * @author verstsiu created at 2020-08-28 09:35
 */
internal class FieldAccessorsTest {

  /* Long */

  @Test
  fun testLong() {
    val accessor = FieldAccessors.LONG
    val testValue = 123L

    val rs = mock<ResultSet> {
      on { next() } doReturn true
      on { getLong(FIRST_READ_INDEX) } doReturn testValue
    }
    val row = prepareRow(rs)
    assert(accessor.read(row) == testValue)
    assert(accessor.write(testValue) == testValue.toString())
  }

  @Test
  fun testOptionalLongExist() {
    val accessor = FieldAccessors.OPTIONAL_LONG
    val testValue = 123L

    val rs = mock<ResultSet> {
      on { next() } doReturn true
      on { getString(FIRST_READ_INDEX) } doReturn testValue.toString()
    }
    val row = prepareRow(rs)
    assert(accessor.read(row) == testValue)
    assert(accessor.write(testValue) == testValue.toString())
  }

  @Test
  fun testOptionalLongEmpty() {
    val accessor = FieldAccessors.OPTIONAL_LONG
    testOptionalFieldEmpty(accessor)
  }

  /* Long :END */

  /* Boolean */

  @Test
  fun testBooleanTrue() {
    val accessor = FieldAccessors.BOOLEAN
    val testValue = true

    val rs = mock<ResultSet> {
      on { next() } doReturn true
      on { getBoolean(FIRST_READ_INDEX) } doReturn testValue
    }
    val row = prepareRow(rs)
    assert(accessor.read(row) == testValue)
    assert(accessor.write(testValue) == QueryConstants.TRUE)
  }

  @Test
  fun testBooleanFALSE() {
    val accessor = FieldAccessors.BOOLEAN
    val testValue = false

    val rs = mock<ResultSet> {
      on { next() } doReturn true
      on { getBoolean(FIRST_READ_INDEX) } doReturn testValue
    }
    val row = prepareRow(rs)
    assert(accessor.read(row) == testValue)
    assert(accessor.write(testValue) == QueryConstants.FALSE)
  }

  @Test
  fun testOptionalBooleanTrue() {
    val accessor = FieldAccessors.OPTIONAL_BOOLEAN
    val testValue = true

    val rs = mock<ResultSet> {
      on { next() } doReturn true
      on { getString(FIRST_READ_INDEX) } doReturn "1"
    }
    val row = prepareRow(rs)
    assert(accessor.read(row) == testValue)
    assert(accessor.write(testValue) == QueryConstants.TRUE)
  }

  @Test
  fun testOptionalBooleanFalse() {
    val accessor = FieldAccessors.OPTIONAL_BOOLEAN
    val testValue = false

    val rs = mock<ResultSet> {
      on { next() } doReturn true
      on { getString(FIRST_READ_INDEX) } doReturn "0"
    }
    val row = prepareRow(rs)
    assert(accessor.read(row) == testValue)
    assert(accessor.write(testValue) == QueryConstants.FALSE)
  }

  @Test
  fun testOptionalBooleanEmpty() {
    val accessor = FieldAccessors.OPTIONAL_BOOLEAN
    testOptionalFieldEmpty(accessor)
  }

  /* Boolean :END */

  /* Int */

  @Test
  fun testInt() {
    val accessor = FieldAccessors.INT
    val testValue = 123

    val rs = mock<ResultSet> {
      on { next() } doReturn true
      on { getInt(FIRST_READ_INDEX) } doReturn testValue
    }
    val row = prepareRow(rs)
    assert(accessor.read(row) == testValue)
    assert(accessor.write(testValue) == testValue.toString())
  }

  @Test
  fun testOptionalIntExist() {
    val accessor = FieldAccessors.OPTIONAL_INT
    val testValue = 123

    val rs = mock<ResultSet> {
      on { next() } doReturn true
      on { getString(FIRST_READ_INDEX) } doReturn testValue.toString()
    }
    val row = prepareRow(rs)
    assert(accessor.read(row) == testValue)
    assert(accessor.write(testValue) == testValue.toString())
  }

  @Test
  fun testOptionalIntEmpty() {
    val accessor = FieldAccessors.OPTIONAL_INT
    testOptionalFieldEmpty(accessor)
  }

  /* Int :END */

  /* String */

  @Test
  fun testString() {
    val accessor = FieldAccessors.STRING
    val testValue = "ABC"

    val rs = mock<ResultSet> {
      on { next() } doReturn true
      on { getString(FIRST_READ_INDEX) } doReturn testValue
    }
    val row = prepareRow(rs)
    assert(accessor.read(row) == testValue)
    assert(accessor.write(testValue) == "'$testValue'")
  }

  @Test
  fun testOptionalStringExist() {
    val accessor = FieldAccessors.OPTIONAL_STRING
    val testValue = "ABC"

    val rs = mock<ResultSet> {
      on { next() } doReturn true
      on { getString(FIRST_READ_INDEX) } doReturn testValue
    }
    val row = prepareRow(rs)
    assert(accessor.read(row) == testValue)
    assert(accessor.write(testValue) == "'$testValue'")
  }

  @Test
  fun testOptionalStringEmpty() {
    val accessor = FieldAccessors.OPTIONAL_STRING
    testOptionalFieldEmpty(accessor)
  }

  /* String :END */

  /* Double */

  @Test
  fun testDouble() {
    val accessor = FieldAccessors.DOUBLE
    val testValue = 1.23

    val rs = mock<ResultSet> {
      on { next() } doReturn true
      on { getDouble(FIRST_READ_INDEX) } doReturn testValue
    }
    val row = prepareRow(rs)
    assert(accessor.read(row) == testValue)
    assert(accessor.write(testValue) == testValue.toString())
  }

  @Test
  fun testOptionalDoubleExist() {
    val accessor = FieldAccessors.OPTIONAL_DOUBLE
    val testValue = 1.23

    val rs = mock<ResultSet> {
      on { next() } doReturn true
      on { getString(FIRST_READ_INDEX) } doReturn testValue.toString()
    }
    val row = prepareRow(rs)
    assert(accessor.read(row) == testValue)
    assert(accessor.write(testValue) == testValue.toString())
  }

  @Test
  fun testOptionalDoubleEmpty() {
    val accessor = FieldAccessors.OPTIONAL_DOUBLE
    testOptionalFieldEmpty(accessor)
  }

  /* Double :END */

  /* Double as Decimal */

  @Test
  fun testDoubleAsDecimal() {
    val accessor = FieldAccessors.DOUBLE_AS_DECIMAL
    val testValue = 0.000012345678

    val rs = mock<ResultSet> {
      on { next() } doReturn true
      on { getString(FIRST_READ_INDEX) } doReturn testValue.toBigDecimal().toString()
    }
    val row = prepareRow(rs)
    assert(accessor.read(row) == testValue)
    assert(accessor.write(testValue) == testValue.toBigDecimal().toString())
  }

  @Test
  fun testOptionalDoubleAsDecimalExist() {
    val accessor = FieldAccessors.OPTIONAL_DOUBLE_AS_DECIMAL
    val testValue = 0.000012345678

    val rs = mock<ResultSet> {
      on { next() } doReturn true
      on { getString(FIRST_READ_INDEX) } doReturn testValue.toBigDecimal().toString()
    }
    val row = prepareRow(rs)
    assert(accessor.read(row) == testValue)
    assert(accessor.write(testValue) == testValue.toBigDecimal().toString())
  }

  @Test
  fun testOptionalDoubleAsDecimalEmpty() {
    val accessor = FieldAccessors.OPTIONAL_DOUBLE_AS_DECIMAL
    testOptionalFieldEmpty(accessor)
  }

  /* Double as Decimal :END */

  /* Float */

  @Test
  fun testFloat() {
    val accessor = FieldAccessors.FLOAT
    val testValue = 1.23F

    val rs = mock<ResultSet> {
      on { next() } doReturn true
      on { getFloat(FIRST_READ_INDEX) } doReturn testValue
    }
    val row = prepareRow(rs)
    assert(accessor.read(row) == testValue)
    assert(accessor.write(testValue) == testValue.toString())
  }

  @Test
  fun testOptionalFloatExist() {
    val accessor = FieldAccessors.OPTIONAL_FLOAT
    val testValue = 1.23F

    val rs = mock<ResultSet> {
      on { next() } doReturn true
      on { getString(FIRST_READ_INDEX) } doReturn testValue.toString()
    }
    val row = prepareRow(rs)
    assert(accessor.read(row) == testValue)
    assert(accessor.write(testValue) == testValue.toString())
  }

  @Test
  fun testOptionalFloatEmpty() {
    val accessor = FieldAccessors.OPTIONAL_FLOAT
    testOptionalFieldEmpty(accessor)
  }

  /* Float :END */

  /* Datetime Seconds */

  @Test
  fun testDatetimeSecondsFloat() {
    val accessor = FieldAccessors.DATETIME_SECONDS
    val testValue = 1598582208L

    val rs = mock<ResultSet> {
      on { next() } doReturn true
      on { getString(FIRST_READ_INDEX) } doReturn "2020-08-28 10:36:48.0"
    }
    val row = prepareRow(rs)
    assert(accessor.read(row) == testValue)
    assert(accessor.write(testValue) == "'2020-08-28 10:36:48'")
  }

  @Test
  fun testOptionalDatetimeSecondsFloatExist() {
    val accessor = FieldAccessors.OPTIONAL_DATETIME_SECONDS
    val testValue = 1598582208L

    val rs = mock<ResultSet> {
      on { next() } doReturn true
      on { getString(FIRST_READ_INDEX) } doReturn "2020-08-28 10:36:48.0"
    }
    val row = prepareRow(rs)
    assert(accessor.read(row) == testValue)
    assert(accessor.write(testValue) == "'2020-08-28 10:36:48'")
  }

  @Test
  fun testOptionalDatetimeSecondsFloatEmpty() {
    val accessor = FieldAccessors.OPTIONAL_DATETIME_SECONDS
    testOptionalFieldEmpty(accessor)
  }

  /* Datetime Seconds :END */

  private fun <T> testOptionalFieldEmpty(accessor: FieldAccessor<T?>) {
    val testValue = null

    val rs = mock<ResultSet> {
      on { next() } doReturn true
      on { getString(FIRST_READ_INDEX) } doReturn testValue
    }
    val row = prepareRow(rs)
    assert(accessor.read(row) == testValue)
    assert(accessor.write(testValue) == QueryConstants.NULL)
  }

  private fun prepareRow(rs: ResultSet): SlickRow {
    val pr = ChildPositionedResult(rs)
    pr.nextRow()
    return SlickRow(pr)
  }

  private class ChildPositionedResult(rs: ResultSet) : PositionedResult(rs) {
    override fun close() {
      // do nothing
    }
  }

  companion object {
    private const val FIRST_READ_INDEX = 1
  }
}