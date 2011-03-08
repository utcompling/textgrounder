import sbt._

class TextGrounderProject (info: ProjectInfo) extends DefaultProject(info) {
  override def disableCrossPaths = true 

  val mavenLocal = "Local Maven Repository" at "file://" + Path.userHome + "/.m2/repository"
  val opennlpRepo = "OpenNLP Maven Repository" at "http://opennlp.sourceforge.net/maven2"
  

}

