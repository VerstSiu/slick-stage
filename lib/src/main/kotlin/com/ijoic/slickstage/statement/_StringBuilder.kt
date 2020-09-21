package com.ijoic.slickstage.statement

import com.ijoic.slickstage.constants.QueryConstants
import com.ijoic.slickstage.entity.Field
import com.ijoic.slickstage.entity.FieldSearch
import java.lang.StringBuilder

internal fun <T> StringBuilder.appendQueryField(field: Field<T>, value: T): StringBuilder {
  val writeValue = field.accessor.write(value)
  append('`')
  append(field.name)
  append('`')

  if (writeValue != QueryConstants.NULL) {
    append('=')
  } else {
    append(" IS ")
  }
  append(writeValue)
  return this
}

internal fun StringBuilder.appendQueryParams(params: Collection<FieldSearch<*>>): StringBuilder {
  for ((fieldIndex, param) in params.withIndex()) {
    val writeValue = param.write()

    if (fieldIndex > 0) {
      append(" AND ")
    }
    append('`')
    append(param.field.name)
    append('`')

    if (writeValue != QueryConstants.NULL) {
      append('=')
    } else {
      append(" IS ")
    }
    append(writeValue)
  }
  return this
}

internal fun <T> StringBuilder.appendUpdateField(field: Field<T>, value: T): StringBuilder {
  val writeValue = field.accessor.write(value)
  append('`')
  append(field.name)
  append('`')

  append('=')
  append(writeValue)
  return this
}