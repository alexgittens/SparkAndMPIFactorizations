# This sets up YARN so you can pull from S3 to HDFS on a Hadoop 2.0.0 cluster launched by spark-ec2. It might be irrelevant soon, or already is on some instance types. Check for the yarn-site.xml in ephemeral-hdfs/conf directory: if it exists, then don't run this file

# before running, ensure that you've exported AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in the environment
import os
import boto.utils
import boto.ec2
import re

_yarn_site_template = """
<configuration>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce.shuffle</value>
  </property>
  <property>
    <name>yarn.nodemanager.aux-services.mapreduce.shuffle.class</name>
    <value>org.apache.hadoop.mapred.ShuffleHandler</value>
  </property>
  <property>
    <name>yarn.resourcemanager.resource-tracker.address</name>
    <value>%(master_public_dns)s:8025</value>
  </property>
  <property>
    <name>yarn.resourcemanager.scheduler.address</name>
    <value>%(master_public_dns)s:8030</value>
  </property>
  <property>
    <name>yarn.resourcemanager.address</name>
    <value>%(master_public_dns)s:8040</value>
  </property>
</configuration>
"""

_mapred_site_template="""
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
</configuration>
"""

_configuration_sync_commands="""
export HADOOP_HOME=/root/ephemeral-hdfs;
for HOST in `cat $HADOOP_HOME/conf/slaves`
  do
    scp -rBv $HADOOP_HOME/conf $HOST:$HADOOP_HOME
  done
"""

def _shutdown_yarn():
  os.system("ephemeral-hdfs/sbin/stop-yarn.sh")

def _start_yarn():
  os.system("ephemeral-hdfs/sbin/start-yarn.sh")

def _shutdown_ephemeral_hdfs():
  os.system("ephemeral-hdfs/sbin/stop-dfs.sh")
  
def _start_ephemeral_hdfs():
  os.system("ephemeral-hdfs/sbin/start-dfs.sh")

def _shutdown_tachyon():
  os.system("tachyon/bin/tachyon-stop.sh")

def _start_tachyon():
  os.system("tachyon/bin/tachyon-start.sh all Mount")

def _shutdown_spark():
  os.system("spark/sbin/stop-all.sh")

def _start_spark():
  os.system("spark/sbin/start-all.sh")

def _configure_yarn():
  with open("ephemeral-hdfs/conf/yarn-site.xml", "w") as fout:
    fout.write(_yarn_site_template % {'master_public_dns': boto.utils.get_instance_metadata()['public-hostname']}) 

def _configure_mapreduce():
  with open("ephemeral-hdfs/conf/mapred-site.xml", "w") as fout:
    fout.write(_mapred_site_template)

def _sync_ephemeral_configuration():
  os.system(_configuration_sync_commands)

def _open_ports():
  region = boto.utils.get_instance_identity()['document']['region']
  conn = boto.ec2.connect_to_region(region)

  master_group_name = boto.utils.get_instance_metadata()['security-groups']
  slave_group_name = re.sub('-master$', '-slaves', master_group_name)
  master_group = [g for g in conn.get_all_security_groups() if g.name==master_group_name][0]
  slave_group = [g for g in conn.get_all_security_groups() if g.name==slave_group_name][0]

  for portnum in [9000,8025,8030,8040]:
    master_group.authorize(src_group=slave_group, ip_protocol='tcp', from_port=portnum, to_port=portnum, cidr_ip='0.0.0.0/0')

def _pulls3data():
  os.system("ephemeral-hdfs/bin/hadoop distcp s3n://agittens/Lewis_Dalisay_Peltatum hdfs:///Lewis_Dalisay_Peltatum_100G")

def main_setup():
    _shutdown_yarn()
    _shutdown_ephemeral_hdfs()
    _shutdown_tachyon()
    _shutdown_spark()

    _configure_yarn()
    _configure_mapreduce()
    _sync_ephemeral_configuration()

    _open_ports()
    
    _start_ephemeral_hdfs()
    _start_yarn()
    _start_tachyon()
    _start_spark()

if __name__=="__main__":
  main_setup()

  # CLUSTER SPECIFIC
  _pulls3data()
