FROM ubuntu:18.04
ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update \
 && apt-get install --assume-yes --no-install-recommends --quiet \
    ca-certificates \
    cmake \
    git \
    g++ \
    make \
    libzip-dev \
    clang-format \
    gdb \
    && apt-get clean all

RUN apt-get install --assume-yes --no-install-recommends --quiet vim

WORKDIR /home/a1

