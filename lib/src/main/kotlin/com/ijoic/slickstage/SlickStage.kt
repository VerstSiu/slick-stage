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
package com.ijoic.slickstage

import akka.actor.ActorSystem
import akka.stream.alpakka.slick.javadsl.Slick
import akka.stream.alpakka.slick.javadsl.SlickRow
import akka.stream.alpakka.slick.javadsl.SlickSession
import akka.stream.javadsl.Source
import com.ijoic.slickstage.entity.Field
import com.ijoic.slickstage.statement.InsertBuilder
import com.ijoic.slickstage.statement.StatementBuilder
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage

/**
 * Slick stage
 *
 * @author verstsiu created at 2020-08-27 11:40
 */
class SlickStage(
  private val session: SlickSession,
  private val provider: () -> ActorSystem
) {

  /**
   * Returns completion stage of insert with [builder]
   */
  @Throws(IllegalArgumentException::class)
  fun insert(builder: InsertBuilder): CompletionStage<Int?> {
    return Source.single(Unit)
      .via(Slick.flow(session) { builder.build() })
      .runFoldAsSingleFirst(provider)
  }

  /**
   * Returns completion stage of insert with [builder] and [identityField]
   */
  @Throws(IllegalArgumentException::class)
  fun <T> insert(builder: InsertBuilder, identityField: Field<T>): CompletionStage<T?> {
    return Source.single(Unit)
      .via(Slick.flow(session) { builder.build() })
      .runFoldAsList(provider)
      .thenCompose { insertCount ->
        if (insertCount.isNullOrEmpty() || insertCount.first() <= 0) {
          CompletableFuture.completedFuture<T?>(null)
        } else {
          Slick
            .source(
              session,
              builder.toIdentityQuery(identityField)
            ) { identityField.accessor.read(it) }
            .runFoldAsSingleFirst(provider)
        }
      }
  }

  /**
   * Returns completion stage of query with [sql] and [mapper]
   */
  fun <T> query(sql: String, mapper: (SlickRow) -> T): CompletionStage<List<T>> {
    return Slick
      .source(session, sql, mapper)
      .runFoldAsList(provider)
  }

  /**
   * Returns completion stage of query rows with [builder] and [mapValue]
   */
  fun <T> query(builder: StatementBuilder, mapValue: (SlickRow) -> T?): CompletionStage<List<T?>> {
    return Slick
      .source(
        session,
        builder.build(),
        mapValue
      )
      .runFoldAsList(provider)
  }

  /**
   * Returns completion stage of update with [toStatement] and [parallelism]
   */
  fun update(toStatement: () -> String, parallelism: Int? = null): CompletionStage<Int?> {
    val flow = when(parallelism) {
      null -> Slick.flow<Unit>(session) { toStatement() }
      else -> Slick.flow<Unit>(session, parallelism) { toStatement() }
    }
    return Source.single(Unit)
      .via(flow)
      .runFoldAsSingleFirst(provider)
  }

  /**
   * Returns completion stage of update with [value], [toStatement] and [parallelism]
   */
  fun <T> update(value: T, toStatement: (T) -> String, parallelism: Int? = null): CompletionStage<Int?> {
    val flow = when(parallelism) {
      null -> Slick.flow<T>(session, toStatement)
      else -> Slick.flow<T>(session, parallelism, toStatement)
    }
    return Source.single(value)
      .via(flow)
      .runFoldAsSingleFirst(provider)
  }

  /**
   * Returns completion stage of update with [items], [toStatement] and [parallelism]
   */
  fun <T> update(items: Iterable<T>, toStatement: (T) -> String, parallelism: Int? = null): CompletionStage<List<Int>> {
    val flow = when(parallelism) {
      null -> Slick.flow<T>(session, toStatement)
      else -> Slick.flow<T>(session, parallelism, toStatement)
    }
    return Source.from(items)
      .via(flow)
      .runFoldAsList(provider)
  }

  /**
   * Returns completion stage of update with [builder]
   */
  fun update(builder: StatementBuilder): CompletionStage<Int?> {
    return Source.single(Unit)
      .via(Slick.flow(session) { builder.build() })
      .runFoldAsSingleFirst(provider)
  }

  companion object {

    private fun <T> T?.wrapAsOptional(): Optional<T> {
      return when(this) {
        null -> Optional.empty()
        else -> Optional.of(this)
      }
    }

    private fun <T, R> Source<T, R>.runFoldAsSingleFirst(provider: () -> ActorSystem): CompletionStage<T?> {
      val done = runFold(
        mutableListOf<T>(),
        { out, value -> out.add(value);out },
        provider
      )
      return done.thenApply { it.firstOrNull() }
    }

    private fun <T, R> Source<T, R>.runFoldAsList(provider: () -> ActorSystem): CompletionStage<List<T>> {
      val done = runFold(
        mutableListOf<T>(),
        { out, value -> out.add(value);out },
        provider
      )
      return done.thenApply { it }
    }
  }
}