ARG UBUNTU_VERSION=latest
FROM --platform=linux/amd64 ubuntu:${UBUNTU_VERSION}

# Update apt-get and install various needed utilities
RUN apt-get update && \
    apt-get install -y curl && \
    apt-get install -y wget && \
    apt-get install -y xz-utils && \
    apt-get install -y make && \
    apt-get install -y gcc && \
    apt-get install -y git && \
    apt-get install -y sysstat && \
    apt-get install -y lsof && \
    apt-get install -y attr

# Install bridged provider prerequisites
# See README.md

# Install go 
ARG GO_VERSION=1.20.4
RUN rm -rf /usr/local/go && \
    wget -O ${GO_VERSION}.tar.gz https://golang.org/dl/go${GO_VERSION}.linux-amd64.tar.gz && \
    tar -C /usr/local -xzf ${GO_VERSION}.tar.gz && \
    rm ${GO_VERSION}.tar.gz 

ENV GOPATH=/root/go
ENV PATH=$PATH:/usr/local/go/bin

# Install go linter
RUN mkdir -p $GOPATH/bin && \
    curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $GOPATH/bin v1.46.2

ENV PATH=$PATH:$GOPATH/bin