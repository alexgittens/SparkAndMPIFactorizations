NB: sometimes we say EOFs and sometimes we say PCs. Climate scientists call PCs of climate fields EOFs, so they're the same thing.

Before running ensure that there are directories named "data" and "eventLogs" in the base repo directory

Next edit these files:
- src/computeEOFs.sh : here you should define the PLATFORM variable ("EC2", "CORI") and invoke src/runOneJob.sh several times to 
  run experiments varying the type of PCA ("exact", "randomized") and the number of PCs 
- src/runOneJob.sh : here you should define the inputs that vary according to the platform (location of the source data, the spark master URL, the executor memory and core settings, etc)

Now you can run experiments (assuming you have salloc-ed and started Spark as needed on the HPC platforms) with 
"sbt submit"
See runeofs.slrm for an example of using sbatch to run experiments on Cori

The console logs are saved to the base repo directory, and the event logs are stored to the eventLogs subdirectory

Miscellany:

We should take into consideration that EC2 has more memory per physical node than Cori (244Gb vs 128Gb), a situation exacerbated by the need on Cori to have some space left over for ramdisks. It seems Spark can safely ask for about 105 Gb/node on Cori, while it can ask for about 210 Gb on EC2, so if we want to keep our 2.2Tb dataset resident in memory on both plaforms and run our experiments using 960 cores, we can either use the setting 

EC2: 30 nodes, 1 executor/node using all 32 cores, 210Gb/executor
Cori: 60 nodes, 1 executor/node using 16 cores, 105Gb/executor 
which has the same memory/core ratio and provides the same amount of space for storing the RDD, or

EC2: 30 nodes, 2 executors/node using 16 cores, 105Gb/executor
Cori: 60 nodes, 1 executor/node using 16 cores, 105Gb/executor
This seems more reasonable

Quick overview of the algorithms:

We want to compute the rank-k truncated PCA of a tall-skinny matrix A. We first process A so that it has mean zero rows. 

The first algorithm, "exact", begins by computing the top-k right singular vectors of A, V_k. We obtain V_k by calling MLLib's ARPACK binding to get the top k eigenvectors of A^TA using a distributed matrix-vector multiply against A^TA (this matrix is never explicitly computed, instead we compute the matrix-vector product). Once V_k is known, we take an SVD on AV_k, which fits locally in one machine. These SVD factors are then combined with V_k to give us our final truncated PCA of A.

The second algorithm, "randomized", follows the same idea but instead of explicitly using a Krylov method to find V_k, it uses a basis for (A^TA)^q Omega, obtained via QR decomposition, as an approximation to V_k. Here, q is a fixed integer, and Omega is a random Gaussian matrix that has (k + p) columns for p a small fixed integer. The values of q and p are hard-coded for now (q = 10, p = 4 as of the time this was written). The advantage is it doesn't have a variable number of iterations as ARPACK or another exact method would, but the disadvantage is decreased accuracy. 
