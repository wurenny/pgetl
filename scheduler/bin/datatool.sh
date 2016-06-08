#!/bin/bash
#******************************************************************************************
#*
#* This Source Code Form is subject to the terms of the Mozilla Public
#* License, v. 2.0. If a copy of the MPL was not distributed with this
#* file, You can obtain one at http://mozilla.org/MPL/2.0/.
#*
#* Copyright (c) 2015-2016, wurenny@gmail.com, All rights reserved
#*
#* IDENTIFICATION
#*     datatool.sh
#* AUTHOR:renny
#* PARAMS:
#*     $1: hostname or ip address
#*     $2: port number
#*     $3: database name
#*     $4: schema name
#*     $5: username
#*     $6: password
#*     $7: table array (separate with comma)
#*     $8: how many rows will insert?
#*     $9: for timestamp column
#* HISTORY:
#*     2016/05/15:created
#*
#* This file is part of PGETL(PostgreSQL ETL) project
#* datatool.sh is a database data generator tools
#*
#******************************************************************************************
BINPATH=`cd $(dirname $0);pwd`
SCH_PATH="$BINPATH/.."
SCH_CFG_FILE="$SCH_PATH/cfg/app/sch"
GLOBAL_INI="$SCH_PATH/cfg/sch/global.ini"
PG_PATH=`grep "^PG_PATH" $GLOBAL_INI | cut -d"=" -f2`
PATH=$PG_PATH:$SCH_PATH/bin:$PATH:.

SCH_DB_IP=`grep "^SCH_DB_IP" $GLOBAL_INI | cut -d"=" -f2`
SCH_DB_PORT=`grep "^SCH_DB_PORT" $GLOBAL_INI | cut -d"=" -f2`
SCH_DB_NAME=`grep "^SCH_DB_NAME" $GLOBAL_INI | cut -d"=" -f2`
SCH_DB_SCHEMA=`grep "^SCH_DB_SCHEMA" $GLOBAL_INI | cut -d"=" -f2`
SCH_DB_USERNAME=`grep "^SCH_DB_USERNAME" $GLOBAL_INI | cut -d"=" -f2`
SCH_DB_PASSWORD=`grep "^SCH_DB_PASSWORD" $GLOBAL_INI | cut -d"=" -f2`

ip=$1
port=$2
dbname=$3
schema=$4
dbuser=$5
password=$6
tables=$7
rows=$8
timez=$9

[ -n $timez ] && (tmz=",'$timez'::timestamptz")

echo *****start `date +'%Y-%m-%d %H:%M:%S'`*****

psql -h "$ip" -p "$port" -U "$dbuser" "$dbname" -c "select $schema.filltabs('$tables',$rows$tmz)"

echo *****end   `date +'%Y-%m-%d %H:%M:%S'`*****
