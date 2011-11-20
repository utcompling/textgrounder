package opennlp.textgrounder.util

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
