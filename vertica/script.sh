#!/bin/bash

sleep 20 && /opt/vertica/bin/vsql -f "/tmp/initdb/init.sql" -Udbadmin
