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
import com.ijoic.slickstage.entity.FieldAccessors
import org.junit.Test

/**
 * Statement builder test
 *
 * @author verstsiu created at 2020-08-28 11:22
 */
internal class StatementBuilderTest {

  @Test
  fun testSelectFields() {
    val builder = StatementBuilder
      .select(Fields.name, Fields.age)
      .from(TEST_TABLE)

    assert(builder.build() == "SELECT `$NAME`,`$AGE` FROM `$TEST_TABLE`")
  }

  @Test
  fun testSelectAll() {
    val builder = StatementBuilder
      .select(Fields.id, Fields.name)
      .from(TEST_TABLE)

    assert(builder.build() == "SELECT `$ID`,`$NAME` FROM `$TEST_TABLE`")
  }

  @Test
  fun testWhereSimple() {
    val builder = StatementBuilder
      .select(Fields.id, Fields.name)
      .from(TEST_TABLE)
      .where(
        ConditionBuilder
          .field(Fields.id, TEST_ID)
          .and(
            ConditionBuilder
              .field(Fields.name, TEST_NAME)
              .or(Fields.age, TEST_AGE)
          )
      )

    assert(builder.build() == "SELECT `$ID`,`$NAME` FROM `$TEST_TABLE` WHERE `$ID`=$TEST_ID AND (`$NAME`='$TEST_NAME' OR `$AGE`=$TEST_AGE)")
  }

  @Test
  fun testUpdateSimple() {
    val builder = StatementBuilder
      .update(TEST_TABLE)
      .set(Fields.name, TEST_NAME)
      .set(Fields.age, TEST_AGE)
      .where(Fields.id, TEST_ID)

    assert(builder.build() == "UPDATE `$TEST_TABLE` SET `$NAME`='$TEST_NAME',`$AGE`=$TEST_AGE WHERE `$ID`=$TEST_ID")
  }

  @Test
  fun testDeleteSimple() {
    val builder = StatementBuilder
      .delete(TEST_TABLE)
      .where(Fields.id, TEST_ID)

    assert(builder.build() == "DELETE FROM `$TEST_TABLE` WHERE `$ID`=$TEST_ID")
  }

  private object Fields {
    val id = Field(name = ID, accessor = FieldAccessors.LONG)
    val name = Field(name = NAME, accessor = FieldAccessors.STRING)
    val age = Field(name = AGE, accessor = FieldAccessors.INT)
  }

  companion object {
    private const val TEST_TABLE = "person"

    private const val TEST_ID = 1001L
    private const val TEST_NAME = "Tom"
    private const val TEST_AGE = 10

    private const val ID = "id"
    private const val NAME = "name"
    private const val AGE = "age"
  }
}