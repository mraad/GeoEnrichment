echo -e "ID\tLON\tLAT\tPOP2005" > /tmp/output.txt
hadoop fs -cat /user/cloudera/output/part-* >> /tmp/output.txt
