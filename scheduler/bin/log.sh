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
#*     log.sh
#* AUTHOR:renny
#* PARAMS:
#*     $1: schedule date (8Bit eg:20160101)
#*     $2: FROM-db name
#*     $3: FROM-table name
#*     $4: TO-db name
#*     $5: TO-table name
#*     $6: step name for query log handily
#*     $7: shell & command returned
#*     $8: sql text content
#* HISTORY:
#*     2016/05/15:created
#*
#* This file is part of PGETL(PostgreSQL ETL) project
#* log.sh record loginfo in table prm_sch_log for each schedule step
#*
#******************************************************************************************
BINPATH=`cd $(dirname $0);pwd`
SCH_PATH="$BINPATH/.."
SCH_CFG_FILE="$SCH_PATH/cfg/app/sch"
GLOBAL_INI="$SCH_PATH/cfg/sch/global.ini"
PG_PATH=`grep "^PG_PATH" $GLOBAL_INI | cut -d"=" -f2`
PATH=$PG_PATH:$SCH_PATH/bin:$PATH:.

DATA_DATE=$1
src_db=$2
src_tab=$3
des_db=$4
des_tab=$5
shell_info=$6
log_info=$7
sql_text=$8

shell_info=${shell_info//\'/\'\'}
sql_text=${sql_text//\'/\'\'}
log_info=${log_info//\'/\'\'}

SCH_DB_IP=`grep "^SCH_DB_IP" $GLOBAL_INI | cut -d"=" -f2`
SCH_DB_PORT=`grep "^SCH_DB_PORT" $GLOBAL_INI | cut -d"=" -f2`
SCH_DB_NAME=`grep "^SCH_DB_NAME" $GLOBAL_INI | cut -d"=" -f2`
SCH_DB_SCHEMA=`grep "^SCH_DB_SCHEMA" $GLOBAL_INI | cut -d"=" -f2`
SCH_DB_USERNAME=`grep "^SCH_DB_USERNAME" $GLOBAL_INI | cut -d"=" -f2`
SCH_DB_PASSWORD=`grep "^SCH_DB_PASSWORD" $GLOBAL_INI | cut -d"=" -f2`
export PGPASSWORD="$SCH_DB_PASSWORD"

LOG_TAB=`grep "SCH_LOG_TABLE" $GLOBAL_INI | cut -d"=" -f2`
LOG_TAB=$SCH_DB_SCHEMA.$LOG_TAB

write_log_sql="insert into $LOG_TAB(sch_date,run_time,src_db,src_tab,des_db,des_tab,shell_info,log_info,sql_text) values('$DATA_DATE','$(date +'%Y-%m-%d %H:%M:%S:%N')','$src_db','$src_tab','$des_db','$des_tab','$shell_info','$log_info','$sql_text')"
psql -h $SCH_DB_IP -d $SCH_DB_NAME -p $SCH_DB_PORT -U $SCH_DB_USERNAME -c "$write_log_sql" 1>/dev/null
