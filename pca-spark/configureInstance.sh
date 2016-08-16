# to be run on master as ubuntu
# formats and mounts the two SSD instances for r3.8xlarge
# and reconfigures yarn/hdfs to use them

HOSTFILE='/home/mpich2.hosts'
PARDO="parallel-ssh -h $HOSTFILE"
#PARDO="echo $PARDO"

/usr/local/hadoop/sbin/stop-dfs.sh
/usr/local/hadoop/sbin/stop-yarn.sh

$PARDO sudo mkdir /mnt2
$PARDO sudo mkdir /mnt3
$PARDO sudo mkfs -t ext4 /dev/xvdaa
$PARDO sudo mkfs -t ext4 /dev/xvdab
$PARDO sudo mount -t ext4 /dev/xvdaa /mnt2
$PARDO sudo mount -t ext4 /dev/xvdab /mnt3

$PARDO sed -i.bak 's/\\/mnt/\\/mnt2/g' /usr/local/hadoop/etc/hadoop/core-site.xml
$PARDO sed -i.bak 's/\\/mnt/\\/mnt2/g' /usr/local/hadoop/etc/hadoop/hdfs-site.xml

$PARDO sudo mkdir -p /mnt2/hadoop
$PARDO sudo mkdir -p /mnt2/hdfs/ubuntu/namenode
$PARDO sudo mkdir -p /mnt2/hdfs/ubuntu/datanode
$PARDO sudo chown -R ubuntu:hadoop /mnt2/hadoop
$PARDO sudo chown -R ubuntu:hadoop /mnt2/hdfs/ubuntu/namenode
$PARDO sudo chown -R ubuntu:hadoop /mnt2/hdfs/ubuntu/datanode
$PARDO /usr/local/hadoop/bin/hdfs namenode -format -force

/usr/local/hadoop/sbin/start-dfs.sh
/usr/local/hadoop/sbin/start-yarn.sh
