import sbt._

// UGLY HACK! How to do this properly?

class GenCode {
  def srcdir(dir: File) = {
    // FIXME! Total hack. The directory passed in is
    // 'TOPLEVEL/target/src_managed/main'.
    val toplevel = dir.getParentFile.getParentFile.getParentFile
    (toplevel / "src" / "main" / "scala" / "opennlp" /
      "textgrounder")
  }

  def create_file(dir: File, infile: File, outfile: String,
      defcmd: String) = {
    val outdir = dir / "textgrounder" / "codegen"
    IO.createDirectory(outdir)
    val outplace = outdir / outfile
    val command = "gcc -E -C -P -x c %s %s" format (defcmd, infile)
    println("%s > %s" format (command, outplace))
    // Surround with parens to avoid attempts to parse following
    // var as arg to !
    (Process(command) #> outplace !)
    outplace
  }
}

/**
 * This contains code to auto-generate *CompressedFeatureVector classes
 * for differing implementation types (Double, Float, etc.).
 * Run from build.sbt.
 */
object GenFeatureVector extends GenCode {
  def gen(dir: File) = {
    val infile = srcdir(dir) / "learning" / "FeatureVector.scala.template"
    Seq("Double", "Float", "Int", "Short").map { ty =>
      val outfile = ty + "FeatureVector.scala"
      create_file(dir, infile, outfile, "-DVECTY=%s" format ty)
    }
  }
}

/**
 * This contains code to auto-generate *ToIntMemoizer classes
 * for differing implementation types (Double, Float, etc.).
 * Run from build.sbt.
 */
object GenMemoizer extends GenCode {
  def gen(dir: File) = {
    val infile = srcdir(dir) / "util" / "Memoizer.scala.template"
    Seq("Object", "Long", "Int").map { ty =>
      val outfile = ty + "Memoizer.scala"
      val defconst = ty.toUpperCase + "_MEMOIZE"
      create_file(dir, infile, outfile, "-D%s" format defconst)
    }
  }
}
