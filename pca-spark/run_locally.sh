#!/bin/bash -l

src/local-submit.sh ./target/scala-2.10/pca_with_h5_and_timings-assembly-0.0.1.jar  ~/projects/spark-test/h5spark/file_list.txt 150 192 centerOverAllObservations 20 . exact
