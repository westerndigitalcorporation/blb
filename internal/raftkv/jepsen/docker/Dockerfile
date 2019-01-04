# Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
# SPDX-License-Identifier: MIT

#
# Use start.sh to build and run this container.
#

FROM jpetazzo/dind

# Stop lein complaining about running as root.
ENV LEIN_ROOT true

#
# Jepsen dependencies
#
RUN apt-get -y -q update && \
    apt-get -y -q install software-properties-common && \
    add-apt-repository ppa:openjdk-r/ppa && \
    apt-get -y -q update && \
    apt-get install -qqy \
        openjdk-8-jdk \
        libjna-java \
        git \
        gnuplot \
        wget

#
# For building blb repo
#
RUN apt-get update && apt-get install -y \
      net-tools \
      gcc \
      vim \
      git \
      make \
      wget && \
      wget -q https://storage.googleapis.com/golang/go1.5.1.linux-amd64.tar.gz && \
      tar -xvzf go1.5.1.linux-amd64.tar.gz && \
      rm go1.5.1.linux-amd64.tar.gz

#
# Get go in path
#
ENV PATH /usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/go/bin
ENV GOROOT /go

#
# Install Leiningen
#
RUN cd / && \
      wget -q https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein && \
      mv /lein /usr/bin && \
      chmod +x /usr/bin/lein && \
      /usr/bin/lein

#
# Start with this script to finish setup (with volumes mounted)
#
ENTRYPOINT ["/usr/local/bin/wrapdocker"]
CMD ["/blb/raftkv/jepsen/docker/internal-run.sh"]
