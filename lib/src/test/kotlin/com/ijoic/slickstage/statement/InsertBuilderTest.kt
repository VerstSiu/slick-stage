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
 * Insert builder test
 *
 * @author verstsiu created at 2020-08-27 16:40
 */
internal class InsertBuilderTest {
  @Test
  fun testBuild() {
    val builder = InsertBuilder(TEST_TABLE)
      .set(Fields.name, "Tom")
      .set(Fields.age, 10)

    assert(builder.build() == "INSERT INTO `$TEST_TABLE`(`$NAME`,`$AGE`) VALUES ('Tom',10)")
  }

  @Test
  fun testToIdentityQuery() {
    val builder = InsertBuilder(TEST_TABLE)
      .set(Fields.name, "Tom")
      .set(Fields.age, 10)

    assert(builder.toIdentityQuery(Fields.id) == "SELECT `$ID` FROM `$TEST_TABLE` WHERE `$NAME`='Tom' AND `$AGE`=10 ORDER BY `id` DESC LIMIT 1")
  }

  @Test
  fun testToIdentityQueryWithOptionalField() {
    val builder = InsertBuilder(TEST_TABLE)
      .set(Fields.name, "Tom")
      .set(Fields.age, 10)
      .set(Fields.tag, null)

    assert(builder.toIdentityQuery(Fields.id) == "SELECT `$ID` FROM `$TEST_TABLE` WHERE `$NAME`='Tom' AND `$AGE`=10 AND `$TAG` IS NULL ORDER BY `id` DESC LIMIT 1")
  }

  private object Fields {
    val id = Field(name = ID, accessor = FieldAccessors.LONG)
    val name = Field(name = NAME, accessor = FieldAccessors.STRING)
    val age = Field(name = AGE, accessor = FieldAccessors.INT)
    val tag = Field(name = TAG, accessor = FieldAccessors.OPTIONAL_STRING)
  }

  companion object {
    private const val TEST_TABLE = "person"

    private const val ID = "id"
    private const val NAME = "name"
    private const val AGE = "age"
    private const val TAG = "tag"
  }
}