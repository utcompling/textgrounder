///////////////////////////////////////////////////////////////////////////////
//  Serializer.scala
//
//  Copyright (C) 2011 Ben Wing, The University of Texas at Austin
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

package opennlp.textgrounder.util

import printutil.warning

/** A type class for converting to and from values in serialized form. */
@annotation.implicitNotFound(msg = "No implicit Serializer defined for ${T}.")
trait Serializer[T] {
  def deserialize(foo: String): T
  def serialize(foo: T): String
  /**
   * Validate the serialized form of the string.  Return true if valid,
   * false otherwise.  Can be overridden for efficiency.  By default,
   * simply tries to deserialize, and checks whether an error was thrown.
   */
  def validate_serialized_form(foo: String): Boolean = {
    try {
      deserialize(foo)
    } catch {
      case _ => return false
    }
    return true
  }
}

/////////////////////////////////////////////////////////////////////////////
//                           Conversion functions                          //
/////////////////////////////////////////////////////////////////////////////

object Serializer {
  def get_int_or_none(foo: String) =
    if (foo == "") None else Option[Int](foo.toInt)
  def put_int_or_none(foo: Option[Int]) = {
    foo match {
      case None => ""
      case Some(x) => x.toString
    }
  }

  implicit val string_serializer = new Serializer[String] {
    def serialize(x: String) = x
    def deserialize(x: String) = x
  }

  implicit val int_serializer = new Serializer[Int] {
    def serialize(x: Int) = x.toString
    def deserialize(x: String) = x.toInt
  }

  implicit val long_serializer = new Serializer[Long] {
    def serialize(x: Long) = x.toString
    def deserialize(x: String) = x.toLong
  }

  implicit val boolean_serializer = new Serializer[Boolean] {
    def serialize(x: Boolean) = if (x) "yes" else "no"
    def deserialize(x: String) = x match {
      case "yes" => true
      case "no" => false
      case _ => {
        warning("Expected yes or no, saw '%s'", x)
        false
      }
    }
  }

  /**
   * Convert an object of type `T` into a serialized (string) form, for
   * storage purposes in a text file.  Note that the construction
   * `T : Serializer` means essentially "T must have a Serializer".
   * More technically, it adds an extra implicit parameter list with a
   * single parameter of type Serializer[T].  When the compiler sees a
   * call to put_x[X] for some type X, it looks in the lexical environment
   * to see if there is an object in scope of type Serializer[X] that is
   * marked `implicit`, and if so, it gives the implicit parameter
   * the value of that object; otherwise, you get a compile error.  The
   * function can then retrieve the implicit parameter's value using the
   * construction `implicitly[Serializer[T]]`.  The `T : Serializer`
   * construction is technically known as a *context bound*.
   */
  def put_x[T : Serializer](foo: T) =
    implicitly[Serializer[T]].serialize(foo)
  /**
   * Convert the serialized form of the value of an object of type `T`
   * back into that type.  Throw an error if an invalid string was seen.
   * See `put_x` for a description of the `Serializer` type and the *context
   * bound* (denoted by a colon) that ties it to `T`.
   *
   * @see put_x
   */
  def get_x[T : Serializer](foo: String) =
    implicitly[Serializer[T]].deserialize(foo)

  /**
   * Convert an object of type `Option[T]` into a serialized (string) form.
   * See `put_x` for more information.  The only difference between that
   * function is that if the value is None, a blank string is written out;
   * else, for a value Some(x), where `x` is a value of type `T`, `x` is
   * written out using `put_x`.
   *
   * @see put_x
   */
  def put_x_or_none[T : Serializer](foo: Option[T]) = {
    foo match {
      case None => ""
      case Some(x) => put_x[T](x)
    }
  }
  /**
   * Convert a blank string into None, or a valid string that converts into
   * type T into Some(value), where value is of type T.  Throw an error if
   * a non-blank, invalid string was seen.
   *
   * @see get_x
   * @see put_x
   */
  def get_x_or_none[T : Serializer](foo: String) =
    if (foo == "") None
    else Option[T](get_x[T](foo))

  /**
   * Convert an object of type `T` into a serialized (string) form.
   * If the object has the value `null`, write out a blank string.
   * Note that T must be a reference type (i.e. not a primitive
   * type such as Int or Double), so that `null` is a valid value.
   * 
   * @see put_x
   * @see put_x
   */
  def put_x_or_null[T >: Null : Serializer](foo: T) = {
    if (foo == null) ""
    else put_x[T](foo)
  }
  /**
   * Convert a blank string into null, or a valid string that converts into
   * type T into that value.  Throw an error if a non-blank, invalid string
   * was seen.  Note that T must be a reference type (i.e. not a primitive
   * type such as Int or Double), so that `null` is a valid value.
   *
   * @see get_x
   * @see put_x
   */
  def get_x_or_null[T >: Null : Serializer](foo: String) =
    if (foo == "") null
    else get_x[T](foo)
}
