export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/global/cscratch1/sd/gittens/nmf-spark/lib

#sbt assembly
#cp target/scala-2.10/nmf-spark-assembly-0.0.1.jar ..
#chmod ugo+rx ../nmf-spark-assembly-0.0.1.jar

#mkdir -p /global/cscratch1/sd/gittens/nmf-spark/temp_fs_dir

#sbatch runnmf_51.slurm 
#sleep 30
#sbatch runnmf_101.slurm
#sleep 30
sbatch runnmf_301.slurm
