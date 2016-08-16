DIR="$(cd "`dirname "$0"`"/..; pwd)"
NAME="sm-cx-16-0-5-1024"
spark-submit --verbose \
  --driver-memory 64G \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=$DIR/../eventlogs \
  --conf spark.driver.maxResultSize=64g \
  --jars $1 \
  --class org.apache.spark.mllib.linalg.distributed.CX \
  $1 \
  df hdfs:///Lewis_Dalisay_Peltatum_20131115_hexandrum_1_1-masked.mat.df \
  0 0 \
  /mnt/out/"$NAME".out \
  16 0 5 1024 \
  2>&1 | tee "$NAME".log

#  df hdfs:///Lewis_Dalisay_Peltatum_20131115_hexandrum_1_1-smoothed-mz=437.11407-sd=0.05.mat.df \
#  df hdfs:///sc-2015/Lewis_Dalisay_Peltatum_20131115_hexandrum_1_1-masked.mat.df \

#  genmat \
#  hdfs:///Lewis_Dalisay_Peltatum_20131115_hexandrum_1_1-masked.mat/Lewis_Dalisay_Peltatum_20131115_hexandrum_1_1-masked.mat.csv \
#  hdfs:///test.out

#  df hdfs:///test.out \
#  8258911 131048 \
#  dump.out \
#  16 2 1


#  genmat hdfs:///Lewis_Dalisay_Peltatum_20131115_hexandrum_1_1-smoothed-mz=437.11407-sd=0.05.rawmat.csv.gz \
#  hdfs:///smoothed.out

#  genmat s3n://amp-jey/sc-2015/Lewis_Dalisay_Peltatum_20131115_hexandrum_1_1-masked.mat/Lewis_Dalisay_Peltatum_20131115_hexandrum_1_1-masked.mat.csv \
