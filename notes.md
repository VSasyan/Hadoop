Notes
=====

http://pingax.com/install-apache-hadoop-ubuntu-cluster-setup/


Ajout du dêpot oracle : `sudo vim /etc/apt/sources.list.d/oracle-java.list`

    deb http://ppa.launchpad.net/webupd8team/java/ubuntu trusty main
    deb-src http://ppa.launchpad.net/webupd8team/java/ubuntu trusty main

Modifications du fichier hosts `/etc/hosts` :

    # Add following hostname and their ip in host table
    172.31.57.77    HadoopMaster lnv1509w029U
    172.31.57.85    HadoopSlave1 lnv1509w037U
    172.31.57.86    HadoopSlave2 lnv1509w038U
    172.31.57.80    HadoopSlave3 lnv1509w032U
    172.31.57.79    HadoopSlave4 lnv1509w031U
    172.31.57.81    HadoopSlave5 lnv1509w033U
    172.31.57.82    HadoopSlave6 lnv1509w034U
    172.31.57.76    HadoopSlave7 lnv1509w028U
    172.31.57.75    HadoopSlave8 lnv1509w027U
