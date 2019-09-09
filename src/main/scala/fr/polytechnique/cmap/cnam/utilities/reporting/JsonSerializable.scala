package fr.polytechnique.cmap.cnam.utilities.reporting

import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.{compact, parse, pretty, render}
import org.json4s.jackson.Serialization

trait JsonSerializable {
  self: Product =>
  def toJsonString(pretify: Boolean = true): String = {
    val json = render(parse(Serialization.write(this)(DefaultFormats)).snakizeKeys)
    if (pretify) {
      pretty(json)
    } else {
      compact(json)
    }
  }
}
