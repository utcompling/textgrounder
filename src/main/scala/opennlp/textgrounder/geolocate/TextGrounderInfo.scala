package opennlp.textgrounder.geolocate

import NlpUtil._

/**
 TextGrounder-specific information (e.g. env vars).
 */

object TextGrounderInfo {
  val textgrounder_dir = System.getenv("TEXTGROUNDER_DIR")
  if (textgrounder_dir == null) {
    errprint("""TEXTGROUNDER_DIR must be set to the top-level directory where
Textgrounder is installed.""")
    require(textgrounder_dir != null)
  }
}
