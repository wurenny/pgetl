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
#*     $3: source file
#*     $4: phrase word
#*     $5: instead value
#*     $6: target file (out file)
#* HISTORY:
#*     2016/05/15:created
#*
#* This file is part of PGETL(PostgreSQL ETL) project
#* phrase.sh work for recognizing phrase of schedule script
#*
#******************************************************************************************
BINPATH=`cd $(dirname $0);pwd`
SCH_PATH="$BINPATH/.."
SCH_CFG_FILE="$SCH_PATH/cfg/app/sch"
GLOBAL_INI="$SCH_PATH/cfg/sch/global.ini"
PG_PATH=`grep "^PG_PATH" $GLOBAL_INI | cut -d"=" -f2`
PATH=$PG_PATH:$SCH_PATH/bin:$PATH:.

logfile=$1
date=$2
file=$3
bskw=$4
askw=$5
file2=$6

time="date +'%Y-%m-%d %H:%M:%S'"

#get logfile
[ -w "$logfile" ] || (logfile=`$SCH_PATH/bin/logfile.sh $date`)


#ident
#echo "[$(eval $time)]:: (3).ident phrase {"$bskw"}" |tee -a $logfile
if [ $# -eq 5 ];then
	sed -i -e 's/'"$bskw"'/'"$askw"'/g' "$file" 2>>$logfile
elif [ $# -eq 6 ];then
	sed -e 's/'"$bskw"'/'"$askw"'/g' "$file" >"$file2" 2>>$logfile
fi

rtncode=$?
#echo "${bskw}--->${askw}!"
if [ $rtncode -eq 0 ];then
	echo -e "[$(eval $time)]:: (3).ident phrase $bskw success" | tee -a $logfile
	exit $rtncode
else
	echo "ERR-00003[$rtncode]: ident phrase $bskw failed $askw" | tee -a $logfile
	echo -e "log file for detial: $logfile\n"
	exit $rtncode
fi
	