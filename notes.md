Notes
=====

http://pingax.com/install-apache-hadoop-ubuntu-cluster-setup/


Ajout du dêpot oracle : `sudo vim /etc/apt/sources.list.d/oracle-java.list`

    deb http://ppa.launchpad.net/webupd8team/java/ubuntu trusty main
    deb-src http://ppa.launchpad.net/webupd8team/java/ubuntu trusty main

Modifications du fichier hosts `/etc/hosts` :

    # Add following hostname and their ip in host table
    172.31.57.77    HadoopMaster
    172.31.57.85    HadoopSlave1
    172.31.57.86    HadoopSlave2
