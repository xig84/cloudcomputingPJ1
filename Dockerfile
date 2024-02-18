FROM ubuntu:18.04

LABEL maintainer="xig"

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
ENV HADOOP_HOME /usr/local/hadoop
ENV PATH $PATH:$HADOOP_HOME/bin:$JAVA_HOME/bin

# Add non-root user
RUN useradd -ms /bin/bash hadoopuser && \
    usermod -aG sudo hadoopuser

RUN apt-get update

RUN apt-get install -y sudo && \
    apt-get install -y rsync && \
    apt-get install -y ssh && \
    apt-get install -y vim && \
    apt-get install -y openjdk-8-jdk && \
    apt-get install -y wget


RUN echo 'root:123' | chpasswd

RUN echo 'hadoopuser:123' | chpasswd

RUN echo 'hadoopuser ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers


# Add user definitions
ENV HDFS_NAMENODE_USER hadoopuser
ENV HDFS_DATANODE_USER hadoopuser
ENV HDFS_SECONDARYNAMENODE_USER hadoopuser
ENV YARN_RESOURCEMANAGER_USER hadoopuser
ENV YARN_NODEMANAGER_USER hadoopuser


RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-3.2.1/hadoop-3.2.1.tar.gz && \
    tar -xzf hadoop-3.2.1.tar.gz && \
    mv hadoop-3.2.1 $HADOOP_HOME && \
    echo "export JAVA_HOME=$JAVA_HOME" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh
  

ADD core-site.xml $HADOOP_HOME/etc/hadoop/
ADD hdfs-site.xml $HADOOP_HOME/etc/hadoop/
ADD mapred-site.xml $HADOOP_HOME/etc/hadoop/
ADD yarn-site.xml $HADOOP_HOME/etc/hadoop/
ADD bootstrap.sh /bootstrap.sh

RUN chmod +x /bootstrap.sh

# Set permissions
RUN chown -R hadoopuser:hadoopuser $HADOOP_HOME

EXPOSE 8088 50070 50075 9000 22 

RUN sudo -u hadoopuser mkdir /home/hadoopuser/.ssh && \
    sudo -u hadoopuser ssh-keygen -t rsa -b 4096 -C "xig84@@pitt.edu" -f /home/hadoopuser/.ssh/id_rsa -q -N "" && \
    cat /home/hadoopuser/.ssh/id_rsa.pub >> /home/hadoopuser/.ssh/authorized_keys && \
    chmod 700 /home/hadoopuser/.ssh && \
    chmod 600 /home/hadoopuser/.ssh/id_rsa && \
    chmod 644 /home/hadoopuser/.ssh/id_rsa.pub && \
    chmod 644 /home/hadoopuser/.ssh/authorized_keys

CMD ["/usr/sbin/sshd", "-D"]

USER hadoopuser

CMD ["bash", "/bootstrap.sh"]

