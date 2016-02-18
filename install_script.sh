
# Installation de Java

# Ajout depot
sudo echo "deb http://ppa.launchpad.net/webupd8team/java/ubuntu trusty main" >> /etc/apt/sources.list.d/oracle-java.list
sudo echo "deb-src http://ppa.launchpad.net/webupd8team/java/ubuntu trusty main" >> /etc/apt/sources.list.d/oracle-java.list

# Installation
sudo apt-get update
sudo apt-get install oracle-java8-installer -y

# Fichier hosts (à remodifier a la main)
sudo cp /etc/hosts /etc/hosts_old
sudo scp -r hduser@172.31.57.85:/etc/hosts /etc/hosts

# Configuration de l'utilisateur :
sudo addgroup hadoop
sudo adduser --ingroup hadoop hduser
sudo adduser hduser sudo
sudo su - hduser
ssh-keygen -t rsa -P ""
cat $HOME/.ssh/id_rsa.pub >> $HOME/.ssh/authorized_keys


# Copie des fichiers hadoop :
sudo scp -r hduser@172.31.57.85:/usr/local/hadoop /usr/local/hadoop
sudo chown hduser:hadoop -R /usr/local/hadoop 

# sudo rsync -avxP /usr/local/hadoop/ hduser@HadoopSlave3:/usr/local/hadoop/
