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
#*     logfile.sh
#* AUTHOR:renny
#* PARAMS:
#*     $1: schedule date (8Bit eg:20160101)
#* HISTORY:
#*     2016/05/15:created
#*
#* This file is part of PGETL(PostgreSQL ETL) project
#* logfile.sh check log work directory and return a valid logfile name
#*
#******************************************************************************************
BINPATH=`cd $(dirname $0);pwd`
SCH_PATH="$BINPATH/.."
SCH_CFG_FILE="$SCH_PATH/cfg/app/sch"
GLOBAL_INI="$SCH_PATH/cfg/sch/global.ini"
PG_PATH=`grep "^PG_PATH" $GLOBAL_INI | cut -d"=" -f2`
PATH=$PG_PATH:$SCH_PATH/bin:$PATH:.

MAX_PARALLEL=`grep "^MAX_PARALLEL" $GLOBAL_INI | cut -d"=" -f2`
LOGFILE=$SCH_PATH/log/$1

#check log directory
if [ ! -w $LOGFILE ];then
	mkdir -p $LOGFILE
	if [ $? -eq 0 ];then
		LOGFILE=$LOGFILE/schedule_p${MAX_PARALLEL}_0.log
	else
		LOGFILE=-1
	fi
else
	LOGFILE=$LOGFILE/schedule_p${MAX_PARALLEL}_$(ls $LOGFILE |grep "schedule_p${MAX_PARALLEL}_" |wc -l).log
fi
echo $LOGFILE
