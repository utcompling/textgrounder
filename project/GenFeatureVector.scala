import sbt._

/**
 * This contains code to auto-generate *CompressedFeatureVector classes
 * for differing implementation types (Double, Float, etc.).
 * Run from build.sbt.
 */
object GenFeatureVector {
  def gen(dir: File) = {
    // UGLY HACK! How to do this properly?
    val toplevel = dir.getParentFile.getParentFile.getParentFile
    val infile = (toplevel / "src" / "main" / "scala" / "opennlp" /
      "textgrounder" / "learning" / "FeatureVector.scala.template")
    Seq("Double", "Float", "Int", "Short").map { ty =>
      val outfile = ty  + "FeatureVector.scala"
      val outdir = dir / "textgrounder" / "codegen"
      IO.createDirectory(outdir)
      val outplace = outdir / outfile
      val command = "gcc -E -C -P -x c -DVECTY=%s %s" format (ty, infile)
      println("%s > %s" format (command, outplace))
      // Surround with parens to avoid attempts to parse following
      // var as arg to !
      (Process(command) #> outplace !)
      outplace
    }
  }
}
