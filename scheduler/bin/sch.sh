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
#*     sch.sh
#* AUTHOR:renny
#* PARAMS:
#*     $1: schedule log file
#*     $2: schedule date (8Bit eg:20160101)
#* HISTORY:
#*     2016/05/15:created
#*
#* This file is part of PGETL(PostgreSQL ETL) project
#* sch.sh is the core scheduler of PGETL
#*
#******************************************************************************************
BINPATH=`cd $(dirname $0);pwd`
SCH_PATH="$BINPATH/.."
SCH_CFG_FILE="$SCH_PATH/cfg/app/sch"
GLOBAL_INI="$SCH_PATH/cfg/sch/global.ini"
PG_PATH=`grep "^PG_PATH" $GLOBAL_INI | cut -d"=" -f2`
PATH=$PG_PATH:$SCH_PATH/bin:$PATH:.

MAX_PARALLEL=`grep "^MAX_PARALLEL" $GLOBAL_INI | cut -d"=" -f2`
TLNS=`cat $SCH_CFG_FILE |wc -l`

logfile=$1
date=$2

time="date +'%Y-%m-%d %H:%M:%S.%N'"

#check parallel configure
paral=`expr $MAX_PARALLEL + 0 2>/dev/null`
if [ $? -ne 0 -o 1 -gt ${paral:-0} ];then
	echo -e "ERR-10001[$MAX_PARALLEL]: invalid Configure parameter for MAX_PARALLEL!\n" 
	exit
fi

#get logfile
[ -w "$logfile" ] || (logfile=`$SCH_PATH/bin/logfile.sh $date`)

#wash
wash()
{
	date=$1
	DES_DB_IP=$2
	DES_DB_PORT=$3
	DES_DB_USER=$4
	DES_DB_PASS=$5
	DES_DB_NAME=$6
	DES_TABNAME=$7
	DES_CLR_SQL=$8
	SNO=$9
	
	[[ -z "$DES_CLR_SQL" ]] && return 0
	export PGPASSWORD="$DES_DB_PASS"
	
	echo | tee -a $logfile
	echo -e [$(eval $time)]:: "ETL[$SNO] wash: "$DES_DB_NAME"."$DES_TABNAME" " |tee -a $logfile
	loginfo=`psql -h $DES_DB_IP -p $DES_DB_PORT -d $DES_DB_NAME -U $DES_DB_USER -Ptuples_only -c "$DES_CLR_SQL" 2>&1`
	rtncode=$?
	loginfo=${loginfo:-"success"}
	$SCH_PATH/bin/log.sh "$date" "-" "-" "$DES_DB_NAME" "$DES_TABNAME" "WASH" "$loginfo" "$DES_CLR_SQL"
	
	#log
	if [ $rtncode -ne 0 ];then
		echo [$(eval $time)]:: "ERR-00101[$rtncode]: wash[$SNO] failed: "$DES_DB_NAME"["$DES_TABNAME": $loginfo" |tee -a $logfile
		echo $loginfo >>$logfile
	else
		echo [$(eval $time)]:: "wash[$SNO] success: "$DES_DB_NAME"["$DES_TABNAME"]" |tee -a $logfile
	fi
	
	return $rtncode
}

#etl core function
etl()
{
	date=${1}
	SRC_DB_IP=${2}
	SRC_DB_PORT=${3}
	SRC_DB_USER=${4}
	SRC_DB_PASS=${5}
	SRC_DB_NAME=${6}
	SRC_TABNAME=${7}
	DES_DB_IP=${8}
	DES_DB_PORT=${9}
	DES_DB_USER=${10}
	DES_DB_PASS=${11}
	DES_DB_NAME=${12}
	DES_TABNAME=${13}
	SRC_QRY_SQL=${14}
	SNO=${15}
	
	SRC_SCHEMA=`echo $SRC_TABNAME | cut -d'.' -f1`
	DES_SCHEMA=`echo $DES_TABNAME | cut -d'.' -f1`
	
	[[ -z "$SRC_QRY_SQL" ]] && return 0

	#etl
	echo | tee -a $logfile
	echo [$(eval $time)]:: "ETL[$SNO]: "$SRC_DB_NAME"."$SRC_TABNAME" " |tee -a $logfile
	sql1="copy ("$SRC_QRY_SQL") to stdout"
	sql2="copy $DES_TABNAME from stdin"
	sh1='nohup $SCH_PATH/bin/sql.sh "$date" "$SRC_DB_IP" "$SRC_DB_PORT" "$SRC_DB_NAME" "$SRC_DB_USER" "$SRC_DB_PASS" "$sql1"'
	sh2='nohup $SCH_PATH/bin/sql.sh "$date" "$DES_DB_IP" "$DES_DB_PORT" "$DES_DB_NAME"  "$DES_DB_USER" "$DES_DB_PASS" "$sql2"'
	loginfo=$(eval nohup $sh1 2>&1| eval $sh2 2>&1)
	rtncode=$?
	loginfo=${loginfo:-"success"}
	$SCH_PATH/bin/log.sh "$date" "$SRC_DB_NAME" "$SRC_TABNAME" "$DES_DB_NAME" "$DES_TABNAME" "ETL" "$loginfo" "$SRC_QRY_SQL ===>[$DES_TABNAME]"
	
	#log
	if [ $rtncode -ne 0 ];then
		rtncode=1
		echo [$(eval $time)]:: "ERR-00100[$rtncode]: ETL[$SNO] Failed: "$SRC_DB_NAME"["$SRC_TABNAME" ===>"$DES_DB_NAME"["$DES_TABNAME": $loginfo" |tee -a $logfile
		echo $loginfo >>$logfile
	else
		echo [$(eval $time)]:: "ETL[$SNO] success: "$SRC_DB_NAME"["$SRC_TABNAME"] ===>"$DES_DB_NAME"["$DES_TABNAME"]" |tee -a $logfile
	fi
	echo | tee -a $logfile
	
	#write back
	$SCH_PATH/bin/signal.sh "$logfile" "$date" "$SCH_CFG_FILE" "$rtncode" "$SNO" "$SRC_SCHEMA" "$DES_SCHEMA"
	echo | tee -a $logfile
	
	return $rtncode
}

#cmd core function
cmd()
{
	date=$1
	DB_IP=$2
	DB_PORT=$3
	DB_USER=$4
	DB_PASS=$5
	DB_NAME=$6
	SCHEMA_NAME=$7
	SQL_TEXT=$8
	SNO=$9
	
	[[ -z "$SQL_TEXT" ]] && return 0
	export PGPASSWORD="$DB_PASS"
	
	echo | tee -a $logfile
	echo -e [$(eval $time)]:: "ETL[$SNO] sql-command: "$DB_NAME"=>"$SQL_TEXT" " |tee -a $logfile
	loginfo=`psql -h $DB_IP -p $DB_PORT -d $DB_NAME -U $DB_USER -Ptuples_only -c "$SQL_TEXT" 2>&1`
	rtncode=$?
	$SCH_PATH/bin/log.sh "$date" "-" "-" "$DB_NAME" "$SCHEMA_NAME" "CMD" "$loginfo" "$SQL_TEXT"
	
	#log
	if [ $rtncode -ne 0 ];then
		rtncode=1
		echo [$(eval $time)]:: "ERR-00102[$rtncode]: ETL[$SNO] sql-command failed=>$loginfo" |tee -a $logfile
		echo $loginfo >>$logfile
	else
		echo [$(eval $time)]:: "ETL[$SNO] sql-command success=>$loginfo" |tee -a $logfile
	fi
	echo | tee -a $logfile
	
	#write back
	$SCH_PATH/bin/signal.sh "$logfile" "$date" "$SCH_CFG_FILE" "$rtncode" "$SNO" "" "$SCHEMA_NAME" >/dev/null 2>&1
	echo | tee -a $logfile
	
	return $rtncode
}


#start process
[[ $TLNS = 0 ]] && exit;
curloop=0
curproc=0

while true
do
	exec 9<&0 < $SCH_CFG_FILE
	while read line
	do
		while [ $curproc -ge $MAX_PARALLEL ]
		do
			curproc=$(jobs -p | wc -w)
			sleep 0.5
		done
		
		SNO=`echo "$line" | awk -F '","' '{print substr($1,2)}'`
		PSNO=`echo "$line" | awk -F '","' '{print $2}'`
		{
		if [ -z $PSNO ];then
			$SCH_PATH/bin/signal.sh "$logfile" "$date" "$SCH_CFG_FILE" "-1" "$SNO" "-" "-"
			
			#etl
			if [ 18 -eq `echo "$line" | awk -F '","' '{print NF}'` ];then
				recd=$line
				SRC_DB_NAME=`echo "$recd" | awk -F '","' '{print substr($3,1)}'`
				SRC_DB_IP=`echo "$recd" | awk -F '","' '{print $4}'`
				SRC_DB_PORT=`echo "$recd" | awk -F '","' '{print $5}'`
				SRC_DB_USER=`echo "$recd" | awk -F '","' '{print $6}'`
				SRC_DB_PASS=`echo "$recd" | awk -F '","' '{print $7}'`
				SRC_SCHEMA=`echo "$recd" | awk -F '","' '{print $8}'`
				SRC_TABNAME=`echo "$recd" | awk -F '","' '{print $9}'`
				SRC_QRY_SQL=`echo "$recd" | awk -F '","' '{print $10}'`
				DES_DB_NAME=`echo "$recd" | awk -F '","' '{print $11}'`
				DES_DB_IP=`echo "$recd" | awk -F '","' '{print $12}'`
				DES_DB_PORT=`echo "$recd" | awk -F '","' '{print $13}'`
				DES_DB_USER=`echo "$recd" | awk -F '","' '{print $14}'`
				DES_DB_PASS=`echo "$recd" | awk -F '","' '{print $15}'`
				DES_SCHEMA=`echo "$recd" | awk -F '","' '{print $16}'`
				DES_TABNAME=`echo "$recd" | awk -F '","' '{print $17}'`
				DES_CLR_SQL=`echo "$recd" | awk -F '","' '{print substr($18,1,length($18)-1)}'`
				SRC_TABNAME=$SRC_SCHEMA.$SRC_TABNAME
				DES_TABNAME=$DES_SCHEMA.$DES_TABNAME
				
				wash "$date" "$DES_DB_IP" "$DES_DB_PORT" "$DES_DB_USER" "$DES_DB_PASS" "$DES_DB_NAME" "$DES_TABNAME" "$DES_CLR_SQL" "$SNO"
				if [ $? -eq 0 ];then
					etl "$date" "$SRC_DB_IP" "$SRC_DB_PORT" "$SRC_DB_USER" "$SRC_DB_PASS" "$SRC_DB_NAME" "$SRC_TABNAME" "$DES_DB_IP" "$DES_DB_PORT" "$DES_DB_USER" "$DES_DB_PASS" "$DES_DB_NAME" "$DES_TABNAME" "$SRC_QRY_SQL" "$SNO" &
				else
					$SCH_PATH/bin/signal.sh "$logfile" "$date" "$SCH_CFG_FILE" "1" "$SNO" "$SRC_SCHEMA" "$DES_SCHEMA"
				fi
			
			#cmd
			elif [ 9 -eq `echo "$line" | awk -F '","' '{print NF}'` ];then
				recd=$line
				DB_NAME=`echo "$recd" | awk -F '","' '{print $3}'`
				SCHEMA_NAME=`echo "$recd" | awk -F '","' '{print $4}'`
				IP=`echo "$recd" | awk -F '","' '{print $5}'`
				PORT=`echo "$recd" | awk -F '","' '{print $6}'`
				USERNAME=`echo "$recd" | awk -F '","' '{print $7}'`
				PASSWORD=`echo "$recd" | awk -F '","' '{print $8}'`
				SQL_TEXT=`echo "$recd" | awk -F '","' '{print substr($9,1,length($9)-1)}'`
				
				cmd "$date" "$IP" "$PORT" "$USERNAME" "$PASSWORD" "$DB_NAME" "$SCHEMA_NAME" "$SQL_TEXT" "$SNO" &
			fi
			
			curproc=$(jobs -p | wc -w)
		fi
		}
		
		if [ $MAX_PARALLEL -eq 1 ];then
			wait
		fi
		curloop=`expr $curloop + 1`
		curproc=$(jobs -p | wc -w)
	done 
	exec 0<&9 9<&-
	
	#finished?
	FINLNS=`grep '","FIN","' $SCH_CFG_FILE |wc -l`
	ERRLNS=`grep -E '","(ERR)+|(\|[0-9]+-ERR)+","' $SCH_CFG_FILE |wc -l`
	
	if [ $(expr $FINLNS + $ERRLNS) -eq $TLNS ] || [ $curloop -gt 2016 -a $curproc = 0 ];then
		break
	fi
	
	wait
	
done
wait
