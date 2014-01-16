///////////////////////////////////////////////////////////////////////////////
//  json.scala
//
//  Copyright (C) 2013-2014 Ben Wing, The University of Texas at Austin
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
///////////////////////////////////////////////////////////////////////////////

package opennlp.textgrounder
package util

import net.liftweb.json.{Serializer=>LiftSerializer,_}

package json {
  /**
   * A Liftweb serializer that special-cases the situation where we have a
   * Traversable over ("string" -> x) pairs, by converting this to a JObject
   * (similar to a map). If we didn't do this, we'd instead end up with an
   * array of separate JObjects, each containing a single mapping. Note that
   * this will automatically handle Maps, which are a subclass of Traversable.
   * FIXME: This will lead to errors if the Traversable contains a
   * ("string" -> x) pair as its first element but other things as other
   * elements.
   */
  class MapIterableSerializer extends LiftSerializer[Traversable[(String, Any)]] {
    def deserialize(implicit format: Formats): PartialFunction[(TypeInfo, JValue), Traversable[(String, Any)]] = {
     case null => throw new MappingException("Should never happen")
    }

    def serialize(implicit format: Formats): PartialFunction[Any, JValue] = {
      case x: Traversable[_] if is_map_as_sequence(x) =>
        Extraction.decompose(x.asInstanceOf[Traversable[(String, Any)]].toMap)(format)
    }
  }
}

package object json {
  def is_map_as_sequence(x: Traversable[Any]) = {
    x match {
      case _: scala.collection.Map[_,_] => false
      case _ =>
        x.size > 0 && {
          val head = x.head
          head match {
            case (_:String, _) => true
            case _ => false
          }
        }
    }
  }

  val no_type_hints_formats = Serialization.formats(NoTypeHints)
  implicit val default_formats = no_type_hints_formats + new MapIterableSerializer

  /**
   * Convert an object of any type into a JSON expression.
   */
  def value_to_json(value: Any)(implicit formats: Formats): JValue =
    Extraction.decompose(value)(formats)

//  /**
//   * Convert an object of any type into a JSON expression. Types
//   * that are unknown are converted to strings.
//   *
//   * FIXME: Add a mechanism to allow extensibility of conversions.
//   * (On the other hand, this might not be possible, at least using
//   * the implicit mechanism. Probably just easier to require the
//   * caller to convert such types manually into a JValue.)
//   */
//  def value_to_json_old(value: Any): JValue = {
//    import net.liftweb.json.JsonDSL._
//    value match {
//      case null => JNull
//      // These declarations work because there are implicit converters from
//      // these various types to JValue, which are invoked because the return
//      // type of value_to_json is declared as a JValue.
//      //
//      // FIXME: This looks weird. Is there a better way?
//      case x: Boolean => x
//      case x: Byte => x
//      case x: Char => x
//      case x: Short => x
//      case x: Int => x
//      case x: Long => x
//      case x: Float => x
//      case x: Double => x
//      case x: BigInt => x
//      case x: BigDecimal => x
//      case x: String => x
//      case x: Symbol => x
//      case x: JValue => x
//      case x: Option[_] => x map value_to_json
//      case (x:String, y) => (x, value_to_json(y))
//      // We special-case the situation where we have a Traversable over
//      // ("string" -> x) pairs, by converting this to a JObject (similar to
//      // a map). If we didn't do this, we'd instead end up with an array of
//      // separate JObjects, each containing a single mapping. Note that this
//      // will automatically handle Maps, which are a subclass of Traversable.
//      // FIXME: This will lead to errors if the Traversable contains a
//      // ("string" -> x) pair as its first element but other things as
//      // other elements.
//      case x: Traversable[_] => {
//        assert(x.size >= 0)
//        val head = x.head
//        head match {
//          case (_:String, _) =>
//            row_to_json_old(x.asInstanceOf[Traversable[(String, Any)]])
//          case _ => x map value_to_json
//        }
//      }
//      case x@_ => x.toString
//    }
//  }
//
//  protected def row_to_json_old(row: Traversable[(String, Any)]) = {
//    import net.liftweb.json.JsonDSL._
//    row.map { case (key, value) => (key, value_to_json_old(value)) }.
//      // This is necessary to invoke the implicit conversion to JObject
//      map { x => x: JObject }.
//      reduce(_ ~ _)
//  }

  /**
   * Convert an object of any type into a JSON expression and render
   * as a string in a "pretty" format (human-readable, with extra
   * whitespace).
   */
  def pretty_json(value: Any) =
    pretty(render(value_to_json(value)))

  /**
   * Convert an object of any type into a JSON expression and render
   * as a string in a compact format (no extra whitespace).
   */
  def compact_json(value: Any) =
    compact(render(value_to_json(value)))
}
