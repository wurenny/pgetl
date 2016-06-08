/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2015-2016, wurenny@gmail.com, All rights reserved
 *
 * IDENTIFICATION
 *     uistl.sql
 * AUTHOR:renny
 * PARAMS:
 * HISTORY:
 *     2016/05/15:created
 *
 * This file is part of PGETL(PostgreSQL ETL) project
 * uistl.sql will be invoked by PGETL installer
 *
 ******************************************************************************************
*/
set search_path=:__PGETL_DB_SCHEMA;

drop view if exists v_sch_etl cascade;
drop view if exists v_sch_cmd cascade;

drop trigger if exists trg_etl on prm_sch_etl cascade;
drop trigger if exists trg_cmd on prm_sch_cmd cascade;
drop function if exists func_trg_etl_cmd() cascade;

drop table if exists prm_sch_batch cascade;
drop table if exists prm_sch_dict cascade;
drop table if exists prm_sch_schema cascade;
drop table if exists prm_sch_etl cascade;
drop table if exists prm_sch_cmd cascade;
drop table if exists prm_sch_log cascade;
