name := "heromsi"

version := "0.0.1"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.3.1" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.3.1" % "provided"

libraryDependencies += "com.github.fommil.netlib" % "all" % "1.1.2"

libraryDependencies += "io.spray" %%  "spray-json" % "1.3.2"

lazy val submit = taskKey[Unit]("Submit CX job")
submit <<= (assembly in Compile) map {
  (jarFile: File) => s"src/runcx.sh ${jarFile}" !
}

lazy val runTest = taskKey[Unit]("Submit test job for CX")
runTest <<= (assembly in Compile) map {
  (jarFile: File) => s"spark-submit --driver-memory 4G --class org.apache.spark.mllib.linalg.distributed.CX ${jarFile} test A_16K_1K.mtx B_1K_32.mtx" !
}

lazy val testPCAvariants = taskKey[Unit]("Test the PCA variants")
testPCAvariants <<= (assembly in Compile) map {
  (jarFile: File) => s"src/run_pca_variants_test.sh ${jarFile}" !
}

lazy val submitPCAvariants = taskKey[Unit]("Submit the PCA variants job")
submitPCAvariants <<= (assembly in Compile) map {
  (jarFile: File) => s"src/runpcavariants.sh ${jarFile}" !
}

lazy val testSVDVariants = taskKey[Unit]("Test the centered SVD variants")
testSVDVariants <<= (assembly in Compile) map {
  (jarFile: File) => s"src/run_svd_variants_test.sh ${jarFile}" !
}

lazy val submitSVDvariants = taskKey[Unit]("Submit the SVD variants job")
submitSVDvariants <<= (assembly in Compile) map {
  (jarFile: File) => s"src/runsvdvariants.sh ${jarFile}" !
}
