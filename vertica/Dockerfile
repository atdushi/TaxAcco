FROM ubuntu:20.04

# Set the locale
RUN apt-get clean && apt-get update && apt-get install -y locales
RUN locale-gen en_US.UTF-8

ADD ./opt /opt/
COPY ./init.sql  /
CMD sleep 20 && /opt/vertica/bin/vsql -f /init.sql -h vertica -Udbadmin

RUN echo "/opt/vertica/bin/vsql -f /init.sql -h vertica -Udbadmin"
