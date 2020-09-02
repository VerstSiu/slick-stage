
# Statement Builder

[![Commitizen friendly](https://img.shields.io/badge/commitizen-friendly-brightgreen.svg)](http://commitizen.github.io/cz-cli/)

Assist library to create sql statement and by field types for [slick](https://doc.akka.io/docs/alpakka/current/slick.html).

## Get Start

1. Add the `JitPack` repository to your build file:

    ```gradle
    allprojects {
        repositories {
            maven { url 'https://jitpack.io' }
        }
    }
    ```

2. Add library dependency:

    ```gradle
    dependencies {
        implementation ''
    }
    ```

## Config

* Add database config to `application.conf`:

    ```conf
    # Load using SlickSession.forConfig("slick-mysql")
    slick-mysql {
      profile = "slick.jdbc.MySQLProfile$"
      db {
        dataSourceClass = "slick.jdbc.DriverDataSource"
        properties = {
          driver = "com.mysql.jdbc.Driver"
          url = "jdbc:mysql://localhost:3306/"
          user = slick
          password = ""
        }
      }
    }
    ```

* Start actor system and perform queries:

    ```kotlin
    val system = ActorSystem.create()
    val session = SlickSession.forConfig("slick-mysql")
    val stage = SlickStage(session) { system }
    system.registerOnTermination(session::close)

    stage
      .insert(
        builder = InsertBuilder("user")
          .set(Fields.name, "Tom")
          .set(Fields.age, 10),
        identityField = Fields.id
      )
      .whenComplete { insertedId, failure ->
        println("insertedId: $insertedId")
        failure?.printStackTrace()
        system.terminate()
      }
    ```

## Usages

* Define fields:

    ```kotlin
    private object Fields {
      val id = Field(name = "id", accessor = FieldAccessors.LONG)
      val name = Field(name = "name", accessor = FieldAccessors.STRING)
      val age = Field(name = "age", accessor = FieldAccessors.INT)  
    }
    ```

* Insert record:

    ```kotlin
    val builder = StatmentBuilder
      .insert(TABLE_NAME)
      .set(Fields.name, "Tom")
      .set(Fields.age, 10)
    ```

* Query:

    ```kotlin
    val builder = StatmentBuilder
      .select(Fields.name, Fields.age)
      .from(TABLE_NAME)
      .where(Fields.id, 1001)
    ```

* Query with condition builder:

    ```kotlin
    val builder = StatmentBuilder
      .select(Fields.name, Fields.age)
      .from(TABLE_NAME)
      .where(
        ConditionBuilder
          .field(Fields.id, 1001)
          .and(Fields.age, 10)
      )
    ```

* Update:

    ```kotlin
    val builder = StatmentBuilder
      .update(TABLE_NAME)
      .set(Fields.age, 11)
      .where(Fields.id, 1001)
    ```

* Delete:

    ```kotlin
    val builder = StatmentBuilder
      .delete(TABLE_NAME)
      .where(Fields.id, 1001)
    ```

## License

```

   Copyright(c) 2020 VerstSiu

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

```
