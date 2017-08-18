# Base docker node image
FROM node:7

## PART 1: Core components
## =======================

# Install utilities
RUN apt-get update --fix-missing && apt-get -y upgrade &&\
apt-get install -y sudo curl wget unzip git

## PART 2: TrackinOps Requeue Concurrency
## ==================================

# Download TrackinOps from git source.
RUN git clone https://github.com/darvydas/trackinops-requeue-concurrency /usr/src/app/trackinops-requeue-concurrency &&\
cd /usr/src/app/trackinops-requeue-concurrency &&\
# git checkout tags/v0.1 &&\
npm install

# # Build TrackinOps from source locally.
# COPY . /usr/src/app/trackinops-requeue-concurrency
# RUN npm install

# Copy configuration file from local source
COPY ./configuration.js /usr/src/app/trackinops-requeue-concurrency/configuration.js

# Create app directory
RUN mkdir -p /usr/src/app/trackinops-requeue-concurrency
RUN mkdir -p /usr/src/app/trackinops-requeue-concurrency/DB
WORKDIR /usr/src/app/trackinops-requeue-concurrency

# EXPOSE 3000
CMD NODE_ENV=production node --max_old_space_size=4096 index.js