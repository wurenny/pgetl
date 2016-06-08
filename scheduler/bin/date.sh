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
#*     date.sh
#* AUTHOR:renny
#* PARAMS:
#*     $1: date (8Bit eg:20160101)
#* HISTORY:
#*     2016/05/15:created
#*
#* This file is part of PGETL(PostgreSQL ETL) project
#* date.sh can check a string is a valid date or not
#*
#******************************************************************************************
BINPATH=`cd $(dirname $0);pwd`
SCH_PATH="$BINPATH/.."
SCH_CFG_FILE="$SCH_PATH/cfg/app/sch"
GLOBAL_INI="$SCH_PATH/cfg/sch/global.ini"
PG_PATH=`grep "^PG_PATH" $GLOBAL_INI | cut -d"=" -f2`
PATH=$PG_PATH:$SCH_PATH/bin:$PATH:.

#Check input params :date
[ -z $1 ] && echo 1 && exit

if [ `expr length $1` -eq 8 ];then
	year=`expr ${1:0:4} + 0 2>/dev/null` && month=`expr ${1:4:2} + 0 2>/dev/null` && date=`expr ${1:6:2} + 0 2>/dev/null`
	if [ $? -eq 0 ];then
		cal $month $year 2>/dev/null |grep -q $date
		if [ $? -eq 0 ];then
			rtn=0
		else
			rtn=3
		fi
	else
		rtn=2
	fi
else
	rtn=1
fi
echo $rtn
