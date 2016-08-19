version := "0.0.1"
scalaVersion := "2.10.4"
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.5.1" % "provided"
libraryDependencies += "com.github.fommil.netlib" % "all" % "1.1.2"
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"

lazy val submit = taskKey[Unit]("compute NMF on large dataset")
submit <<= (assembly in Compile) map {
  (jarFile: File) => s"src/computeNMF.sh ${jarFile}" !
} 

parallelExecution in Test := false
test in assembly := {}
