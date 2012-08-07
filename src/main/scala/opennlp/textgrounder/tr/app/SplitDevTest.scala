package opennlp.textgrounder.tr.app

import java.io._

object SplitDevTest extends App {
  val dir = new File(args(0))

  val devDir = new File(dir.getCanonicalPath+"dev")
  val testDir = new File(dir.getCanonicalPath+"test")
  devDir.mkdir
  testDir.mkdir

  val files = dir.listFiles

  var i = 1
  for(file <- files) {
    if(i % 3 == 0)
      file.renameTo(new File(testDir, file.getName))
    else
      file.renameTo(new File(devDir, file.getName))
    i += 1
  }
}
