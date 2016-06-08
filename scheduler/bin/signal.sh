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
#*     phrase.sh
#* AUTHOR:renny
#* PARAMS:
#*     $1: schedule log file
#*     $2: schedule date (8Bit eg:20160101)
#*     $3: schedule script file
#*     $4: job status:[0,success;-1,running;Other:error]
#*     $5: job id
#*     $6: source schema of job
#*     $7: destination schema of job
#* HISTORY:
#*     2016/05/15:created
#*
#* This file is part of PGETL(PostgreSQL ETL) project
#* phrase.sh notify scheduler when a job finished
#*
#******************************************************************************************
BINPATH=`cd $(dirname $0);pwd`
SCH_PATH="$BINPATH/.."
SCH_CFG_FILE="$SCH_PATH/cfg/app/sch"
GLOBAL_INI="$SCH_PATH/cfg/sch/global.ini"
PG_PATH=`grep "^PG_PATH" $GLOBAL_INI | cut -d"=" -f2`
PATH=$PG_PATH:$SCH_PATH/bin:$PATH:.
LOCK_FILE="$SCH_PATH/lck/pgetl.lock"

logfile=$1
date=$2
file=$3
flag=$4
sno=$5
src_schema=$6
des_schema=$7

time="date +'%Y-%m-%d %H:%M:%S'"

#get logfile
[ -w "$logfile" ] || (logfile=`$SCH_PATH/bin/logfile.sh $date`)


if [ $flag -eq -1 ];then
	flag=""
	flag1="ING"
	flag2=""
	tip="start"
elif [ $flag -eq 0 ];then
	flag="ING"
	flag1="FIN"
	flag2=""
	tip="finish"
else
	flag="ING"
	flag1="ERR"
	flag2="|$sno-ERR"
	tip="error"
fi

#write back
#echo [$(eval $time)]:: "job signal :$sno-$src_schema-$des_schema > $tip" |tee -a $logfile
cmd="sed -i -e 's/^\"${sno}\",\"${flag}\"/\"${sno}\",\"${flag1}\"/' -e 's/|${sno}-${src_schema}-${des_schema}/${flag2}/g' $file 2>>$logfile"
flock "$LOCK_FILE" -c "$cmd"

rtncode=$?
if [ $rtncode -ne 0 ];then
	echo [$(eval $time)]:: "write back failed" |tee -a $logfile
	echo -e "detial log: $logfile\n"
fi
