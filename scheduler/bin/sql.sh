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
#*     sql.sh
#* AUTHOR:renny
#* PARAMS:
#*     $1: schedule date (8Bit eg:20160101)
#*     $2: hostname or ip address
#*     $3: port number
#*     $4: database name
#*     $5: username
#*     $6: password
#*     $7: sql statement
#* HISTORY:
#*     2016/05/15:created
#*
#* This file is part of PGETL(PostgreSQL ETL) project
#* sql.sh use to run a sql statement for PGETL
#*
#******************************************************************************************
BINPATH=`cd $(dirname $0);pwd`
SCH_PATH="$BINPATH/.."
SCH_CFG_FILE="$SCH_PATH/cfg/app/sch"
GLOBAL_INI="$SCH_PATH/cfg/sch/global.ini"
PG_PATH=`grep "^PG_PATH" $GLOBAL_INI | cut -d"=" -f2`
PATH=$PG_PATH:$SCH_PATH/bin:$PATH:.

date=$1
host=$2
port=$3
dbname=$4
username=$5
password=$6
sql=$7

time="date +'%Y-%m-%d %H:%M:%S'"

[ -n $password ] && export PGPASSWORD="$password"

psql -h "$host" -p "$port" -d "$dbname" -U "$username" -Ptuples_only -c "$sql"

