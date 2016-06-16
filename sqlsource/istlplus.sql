/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2015-2016, wurenny@gmail.com, All rights reserved
 *
 * IDENTIFICATION
 *     istlplus.sql
 * AUTHOR:renny
 * PARAMS:
 * HISTORY:
 *     2016/05/15:created
 *
 * This file is part of PGETL(PostgreSQL ETL) project
 * istlplus.sql will be invoked by PGETL installer
 *
 ******************************************************************************************
*/
set search_path=:__PGETL_DB_SCHEMA;

drop table if exists pgetl_oltp_prod1;
create table pgetl_oltp_prod1 (
	id int4 not null,
	ctime timestamp(3) not null,
	field1 varchar(15),
	field2 varchar(15),
	field3 varchar(15),
	field4 varchar(15),
	field5 varchar(15),
	constraint pgetl_oltp_prod1_pk primary key (id)
);

drop table if exists pgetl_oltp_prod2;
create table pgetl_oltp_prod2 (
	id int4 not null,
	ctime timestamp(3) not null,
	field1 varchar(15),
	field2 varchar(15),
	field3 varchar(15),
	field4 varchar(15),
	field5 varchar(15),
	constraint pgetl_oltp_prod2_pk primary key (id)
);

drop table if exists pgetl_olap_ods;
create table pgetl_olap_ods (
	id int4 not null,
	source varchar(32),
	ctime timestamp(3) not null,
	field1 varchar(15),
	field2 varchar(15),
	field3 varchar(15),
	field4 varchar(15),
	field5 varchar(15)
);

drop table if exists pgetl_olap_dw cascade;
create table pgetl_olap_dw (
	id int4 not null,
	source varchar(32),
	ctime timestamp(3) not null,
	field1 varchar(15),
	field2 varchar(15),
	field3 varchar(15),
	field4 varchar(15),
	field5 varchar(15)
);
create index pgetl_olap_dw_idx1 on pgetl_olap_dw(ctime);

drop table if exists pgetl_olap_dm cascade;
create table pgetl_olap_dm (
	source varchar(32) not null,
	cdate date not null,
	total bigint not null
);
create index pgetl_olap_dm_idx1 on pgetl_olap_dm(cdate);

---------------------------------------drop for replace----------------------------------
drop function if exists pgetl_olap_dmproc(date) cascade;
drop function if exists pgetl_tool_filltabs(text,int,timestamptz) cascade;
drop function if exists pgetl_tool_filltab(varchar,int,timestamptz) cascade;

create or replace function pgetl_olap_dmproc(i_date date,schema_name varchar(128))
returns varchar as
$$
begin
	execute 'set search_path=' || schema_name;
	
	delete from pgetl_olap_dm where cdate =i_date;
	
	insert into pgetl_olap_dm
	select source,i_date,count(*) total from pgetl_olap_dw
	where ctime >=i_date and ctime <(i_date + interval'1 days')
	group by source;
	
	return 'oltp data model analyze success!';
--exception when others then raise;return 'the oltp data model analyze fail';
end;
$$ language plpgsql;

---------------------------------------function filltab---------------------------------------
create or replace function pgetl_tool_padtab(
	full_table_name varchar,
	rows int,
	times timestamptz default now(),
	append bool default false
) returns varchar as
$$
declare
	v_count int;
	schema_name varchar(128);
	table_name varchar(128);
	v_sql text;
begin
	schema_name =(regexp_split_to_array(full_table_name,'\.'))[1];
	table_name =(regexp_split_to_array(full_table_name,'\.'))[2];
	if(table_name is null) then
		schema_name ='public';
		table_name =full_table_name;
	end if;
	
	select count(*) into v_count from pg_tables where schemaname=schema_name and tablename=table_name;
	if(v_count =0) then
		return 'fail:table ['|| full_table_name ||'] not found!';
	end if;
	
	if(not append) then
		v_sql :='truncate table ' || full_table_name;
		execute v_sql;
	end if;
	
	select 'insert into '|| full_table_name || ' select'||
		chr(10) || string_agg(sqlpart,','||chr(10)) || chr(10) ||
		'from (select generate_series(1,'|| rows ||') id) t' sqltxt into v_sql
	from(
		select case when substr(data_type,1,18)='character varying(' then 
			'(select string_agg(a,'''')::text str from '||
			'(select generate_series(1,' || least(32,atttypmod-4) ||
			') id,chr(ascii(''A'') +floor(random()*((id*random()*31)::int%26))::int) a) x)'
		when substr(data_type,1,9)='timestamp' then 'current_timestamp'
		else 'id' end sqlpart 
		from(
			SELECT a.attname,format_type(a.atttypid, a.atttypmod) AS data_type,
				a.atttypid,a.atttypmod
			FROM pg_catalog.pg_attribute a,
				(
					SELECT c.oid 
					FROM pg_catalog.pg_class c  
					LEFT JOIN pg_catalog.pg_namespace n    
					ON n.oid = c.relnamespace  
					WHERE (c.relname) =lower(table_name) 
						AND (n.nspname) = lower(schema_name)
				) b 
				WHERE a.attrelid = b.oid
					AND a.attnum > 0 and attisdropped='f'
					AND NOT a.attisdropped ORDER BY a.attnum
		) t
	) t1;
	execute v_sql;
	return full_table_name ||' success!';
--exception when others then raise;return full_table_name||'failed!';
end;
$$ LANGUAGE 'plpgsql';

---------------------------------------function filltabs---------------------------------------
create or replace function pgetl_tool_padtabs(
	tables text,
	rows int,
	times timestamptz default now(),
	append bool default false
) returns varchar as
$$
declare
	tab varchar(128);
	rst text ='';
begin
set search_path=public;
	for tab in (select unnest(regexp_split_to_array(tables,','))) loop
		select x into tab from pgetl_tool_padtab(tab,rows,times,append) x;
		rst =rst || tab || chr(10);
	end loop;
	return rst;
--exception when others then raise;return 'failed!';
end;
$$
LANGUAGE 'plpgsql';

select pgetl_tool_padtab(:'__PGETL_DB_SCHEMA' || '.pgetl_oltp_prod1',2016);
select pgetl_tool_padtab(:'__PGETL_DB_SCHEMA' || '.pgetl_oltp_prod2',1333);

--clear
delete from prm_sch_batch where bchno in(1,2);
delete from prm_sch_schema where db_name=:'__PGETL_DB_NAME' and schema_name=:'__PGETL_DB_SCHEMA';
delete from prm_sch_etl where sno between 1 and 3;
delete from prm_sch_cmd where sno=4;

--init prm_sch_batch
INSERT INTO prm_sch_batch VALUES ('1', '00:00:00.000000', '00:00:00.000000', '1 day', 'batch No.1');
INSERT INTO prm_sch_batch VALUES ('2', '22:00:00.000000', '22:00:00.000000', '-1 days', 'batch No.2');
--init prm_sch_batch
INSERT INTO prm_sch_schema VALUES (:'__PGETL_DB_NAME', :'__PGETL_DB_SCHEMA', :'__PGETL_DB_IP', :'__PGETL_DB_PORT', :'__PGETL_DB_USERNAME', :'__PGETL_DB_PASSWORD', 'demo schema');
--init prm_sch_batch
INSERT INTO prm_sch_etl VALUES ('1', NULL, true, :'__PGETL_DB_NAME', 'pgetl_oltp_prod1', :'__PGETL_DB_NAME', 'pgetl_olap_ods', 'select id,''prod1'',ctime,field1,field2,field3,field4,field5 from ##src_tabname## where ctime>=''##start_time##''::timestamp and ctime<''##end_time##''::timestamp', '*', '*', :'__PGETL_DB_SCHEMA');
INSERT INTO prm_sch_etl VALUES ('2', '{1}', true, :'__PGETL_DB_NAME', 'pgetl_oltp_prod2', :'__PGETL_DB_NAME', 'pgetl_olap_ods', 'select id,''prod2'',ctime,field1,field2,field3,field4,field5 from ##src_tabname## where ctime>=''##start_time##''::timestamp and ctime<''##end_time##''::timestamp', null, '*', :'__PGETL_DB_SCHEMA');
INSERT INTO prm_sch_etl VALUES ('3', '{2}', true, :'__PGETL_DB_NAME', 'pgetl_olap_ods', :'__PGETL_DB_NAME', 'pgetl_olap_dw', 'select * from ##src_tabname## where ctime>=''##start_time##''::timestamp and ctime<''##end_time##''::timestamp', 'delete from ##des_tabname## where ctime>=''##start_time##''::timestamp and ctime<''##end_time##''::timestamp', :'__PGETL_DB_SCHEMA', :'__PGETL_DB_SCHEMA');
INSERT INTO prm_sch_cmd VALUES ('4', '{3}', true, :'__PGETL_DB_NAME', :'__PGETL_DB_SCHEMA', 'select '|| :'__PGETL_DB_SCHEMA' ||'.pgetl_olap_dmproc(''##start_time##''::date,'''||:'__PGETL_DB_SCHEMA'||''')');
