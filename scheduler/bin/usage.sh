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
#*     usage.sh
#* AUTHOR:renny
#* PARAMS:
#* HISTORY:
#*     2016/05/15:created
#*
#* This file is part of PGETL(PostgreSQL ETL) project
#* usage.sh show help tips for PGETL
#*
#******************************************************************************************
BINPATH=`cd $(dirname $0);pwd`
SCH_PATH="$BINPATH/.."
GLOBAL_INI="$SCH_PATH/cfg/sch/global.ini"
PG_PATH=`grep "^PG_PATH" $GLOBAL_INI | cut -d"=" -f2`
PATH=$PG_PATH:$SCH_PATH/bin:$PATH:.

SCH_DB_SCHEMA=`grep "^SCH_DB_SCHEMA" $GLOBAL_INI | cut -d"=" -f2`

echo  "\

                   PostgreSQL ETL <version:1.3>
------------------------------------------------------------------------
usage: pgetl date=\"\" batch=\"\" [options]=\"[args...]\"

options:
	etl		query condition for the view $SCH_DB_SCHEMA.v_sch_etl
	cmd		query condition for the view $SCH_DB_SCHEMA.v_sch_cmd
example: pgetl date=20160101 batch=1 etl=\"sno=1\"

Any help? welcome mail to <`tput bold && tput smul`wurenny@gmail.com`tput sgr0`>
"