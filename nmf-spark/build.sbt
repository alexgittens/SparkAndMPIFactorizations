version := "0.0.1"
scalaVersion := "2.10.4"
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.5.1" % "provided"
libraryDependencies += "com.github.fommil.netlib" % "all" % "1.1.2"
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"

lazy val runTest = taskKey[Unit]("test NMF")
runTest <<= (assembly in Compile) map {
  (jarFile: File) => s"src/testNMF.sh ${jarFile}" !
}

lazy val submit = taskKey[Unit]("compute NMF on large dataset")
submit <<= (assembly in Compile) map {
  (jarFile: File) => s"src/computeNMF.sh ${jarFile}" !
} 

lazy val nodes51 = taskKey[Unit]("compute NMF on large dataset w/ 50 executors")
nodes51 <<= (assembly in Compile) map {
    (jarFile: File) => s"src/computeNMF.sh ${jarFile} 50" !
}

lazy val nodes101 = taskKey[Unit]("compute NMF on large dataset w/ 100 executors")
nodes101 <<= (assembly in Compile) map {
    (jarFile: File) => s"src/computeNMF.sh ${jarFile} 100" !
}

lazy val nodes301 = taskKey[Unit]("compute NMF on large dataset w/ 300 executors")
nodes301 <<= (assembly in Compile) map {
    (jarFile: File) => s"src/computeNMF.sh ${jarFile} 300" !
}

parallelExecution in Test := false
test in assembly := {}
