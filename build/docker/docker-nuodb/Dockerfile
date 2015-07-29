# Get the clean Ubuntu
FROM ubuntu:14.04
MAINTAINER Mahesha Godekere <mgodekere@nuodb.com>
RUN apt-get update --fix-missing
RUN apt-get install -y openssh-server openssl vim

# Install JRE and other tools
RUN /usr/bin/apt-get -y install default-jre-headless wget tar

# Run SSH to directly login into nuodb docker container if needed
RUN mkdir /var/run/sshd
RUN echo 'root:root' | chpasswd
RUN sed -i 's/PermitRootLogin without-password/PermitRootLogin yes/' /etc/ssh/sshd_config

# SSH login fix. Otherwise user is kicked off after login
RUN sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd
#ENV NOTVISIBLE "in users profile"
RUN echo "export VISIBLE=now" >> /etc/profile

# Identify the NuoDB version
ENV NUODB_TAR_VERSION nuodb-2.2.1.2.linux.x86_64.tar.gz
ENV NUODB_VER nuodb-2.2.1.2.linux.x86_64

# Pull the NuoDB tar.gz and install
RUN wget http://download.nuohub.org/$NUODB_TAR_VERSION -O /tmp/$NUODB_TAR_VERSION
RUN tar xzvf /tmp/$NUODB_TAR_VERSION -C /opt/
RUN chown -R root:root /opt/$NUODB_VER
RUN ln -s /opt/$NUODB_VER /opt/nuodb
ENV NUODB_HOME /opt/nuodb
ENV PATH ${NUODB_HOME}/bin:${PATH}

# Set the NuoDB default.properties file
RUN mv $NUODB_HOME/etc/default.properties.sample $NUODB_HOME/etc/default.properties
RUN /bin/sed -ie 's/#domainPassword =/domainPassword = bird/' $NUODB_HOME/etc/default.properties
RUN /bin/sed -ie "s/^[# ]*port =.*/port = 48004/" $NUODB_HOME/etc/default.properties
RUN /bin/sed -ie "s/^[# ]*portRange =.*/portRange = 48005/" $NUODB_HOME/etc/default.properties
RUN /bin/sed -ie "s/^[# ]*broker =.*/broker = true/" $NUODB_HOME/etc/default.properties
RUN /bin/sed -ie "s/^[# ]*region =.*/region = DEFAULT_REGION/" $NUODB_HOME/etc/default.properties

# Expose all the necessary ports
EXPOSE 25 8080 8888 9001 48004 48005 48006 48007 48008

# Start NuoDB
ADD start-nuodb.sh /start-nuodb.sh
CMD sh start-nuodb.sh

