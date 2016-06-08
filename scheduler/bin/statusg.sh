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
#*     statusg.sh
#* AUTHOR:renny
#* PARAMS:
#* HISTORY:
#*     2016/05/15:created
#*
#* This file is part of PGETL(PostgreSQL ETL) project
#* statusg.sh show help tips for pgetlstat command
#*
#******************************************************************************************
BINPATH=`cd $(dirname $0);pwd`
SCH_PATH="$BINPATH/.."
GLOBAL_INI="$SCH_PATH/cfg/sch/global.ini"
PG_PATH=`grep "^PG_PATH" $GLOBAL_INI | cut -d"=" -f2`
PATH=$PG_PATH:$SCH_PATH/bin:$PATH:.

SCH_DB_SCHEMA=`grep "^SCH_DB_SCHEMA" $GLOBAL_INI | cut -d"=" -f2`

echo  "\

                   PostgreSQL ETL Status Tool
------------------------------------------------------------------------
usage: pgetl [options]

options:
	\$1: \"-f\" means flush display from time to time, end while all jobs are finished
	    \"-F\" means no end until to stop manually
	\$2: flush interval seconds(only work when \$1 defined), default 2s
example: pgetlstat -F 3

Any help? welcome mail to <`tput bold && tput smul`wurenny@gmail.com`tput sgr0`>
"