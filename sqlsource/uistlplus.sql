/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2015-2016, wurenny@gmail.com, All rights reserved
 *
 * IDENTIFICATION
 *     uistlplus.sql
 * AUTHOR:renny
 * PARAMS:
 * HISTORY:
 *     2016/05/15:created
 *
 * This file is part of PGETL(PostgreSQL ETL) project
 * uistlplus.sql will be invoked by PGETL installer
 *
 ******************************************************************************************
*/
set search_path=:__PGETL_DB_SCHEMA;

delete from prm_sch_batch where bchno in(1,2);
delete from prm_sch_schema where db_name=:'__PGETL_DB_NAME' and schema_name=:'__PGETL_DB_SCHEMA';
delete from prm_sch_etl where sno between 1 and 3;
delete from prm_sch_cmd where sno=4;

drop table if exists pgetl_oltp_prod1 cascade;
drop table if exists pgetl_oltp_prod2 cascade;
drop table if exists pgetl_olap_ods cascade;
drop table if exists pgetl_olap_dw cascade;
drop table if exists pgetl_olap_dm cascade;
drop function if exists pgetl_olap_dmproc(date,varchar) cascade;
drop function if exists pgetl_tool_padtabs(text,int,timestamptz,bool) cascade;
drop function if exists pgetl_tool_padtab(varchar,int,timestamptz,bool) cascade;
