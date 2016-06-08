/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2015-2016, wurenny@gmail.com, All rights reserved
 *
 * IDENTIFICATION
 *     istl.sql
 * AUTHOR:renny
 * PARAMS:
 * HISTORY:
 *     2016/05/15:created
 *
 * This file is part of PGETL(PostgreSQL ETL) project
 * istl.sql will be invoked by PGETL installer
 *
 ******************************************************************************************
*/
-- CREATE USER PGETL;
-- CREATE DATABASE PGETL OWNER PGETL;
set search_path=:__PGETL_DB_SCHEMA;

-- ----------------------------
-- Table structure for prm_sch_batch
-- ----------------------------
CREATE TABLE prm_sch_batch (
	bchno int4 NOT NULL,
	starttime varchar(15) NOT NULL,
	endtime varchar(15) NOT NULL,
	apart interval,
	remarks varchar(255),
	constraint prm_sch_batch_pk PRIMARY KEY (bchno),
	constraint prm_sch_batch_check_starttime CHECK (starttime::time without time zone between '00:00:00'::time without time zone and '23:59:59.999999'::time without time zone ),
	constraint prm_sch_batch_check_endtime CHECK (endtime::time without time zone between '00:00:00'::time without time zone and '23:59:59.999999'::time without time zone )
);

-- ----------------------------
-- Table structure for prm_sch_dict
-- ----------------------------
CREATE TABLE prm_sch_dict (
	key_word varchar(128) NOT NULL,
	trans_value varchar(255),
	remarks varchar(500),
	constraint prm_sch_keydic_pk primary key(key_word)
);

-- ----------------------------
-- Table structure for prm_sch_schema
-- ----------------------------
CREATE TABLE prm_sch_schema (
	db_name varchar(128) NOT NULL,
	schema_name varchar(128) NOT NULL,
	ip varchar(15),
	port int4,
	username varchar(128),
	password varchar(128),
	remarks varchar(255),
	constraint prm_sch_schema_pk PRIMARY KEY (db_name, schema_name)
);

-- ----------------------------
-- Table structure for prm_sch_etl
-- ----------------------------
CREATE TABLE prm_sch_etl (
	sno int4 NOT NULL,
	psno int4[],
	enable bool NOT NULL default true,
	src_dbname varchar(128) NOT NULL,
	src_tabname varchar(128) NOT NULL,
	des_dbname varchar(128) NOT NULL,
	des_tabname varchar(128) NOT NULL,
	src_qry_sql varchar(4000) NOT NULL,
	des_clr_sql varchar(4000),
	src_schema varchar(128) NOT NULL,
	des_schema varchar(128) NOT NULL,
	constraint prm_sch_etl_pk PRIMARY KEY (sno),
	--constraint prm_sch_etl_uk unique (src_dbname, src_tabname, src_schema, des_tabname),
	constraint prm_sch_etl_psno_check check(not (psno@>array[sno]::int[]))
);

-- ----------------------------
-- Table structure for prm_sch_cmd
-- ----------------------------
CREATE TABLE prm_sch_cmd (
	sno int4 NOT NULL,
	psno int4[],
	enable bool NOT NULL default true,
	db_name varchar(128) NOT NULL,
	schema_name varchar(128) NOT NULL,
	sql_text varchar(4000) NOT NULL,
	constraint prm_sch_cmd_pk PRIMARY KEY (sno),
	constraint prm_sch_cmd_psno_check check(not (psno@>array[sno]::int[]))
);

CREATE TABLE prm_sch_log (
	sch_date varchar(8),
	run_time varchar(32),
	src_db varchar(128),
	src_tab varchar(128),
	des_db varchar(128),
	des_tab varchar(128),
	shell_info varchar(255),
	log_info varchar(2000),
	sql_text varchar(4000)
);
create index idx_prm_sch_log on prm_sch_log(sch_date);

--init prm_sch_dict
truncate table prm_sch_dict;
INSERT INTO prm_sch_dict VALUES ('##batch_no##', '$batch', 'defined in prm_sch_batch');
INSERT INTO prm_sch_dict VALUES ('##sch_date##', '$date', 'schedule date:yyyy-mm-dd');
INSERT INTO prm_sch_dict VALUES ('##start_time##', '$date $time', '$time is the starttime which defined in prm_sch_batch');
INSERT INTO prm_sch_dict VALUES ('##end_time##', '$date $time', '$time is the endtime which defined in prm_sch_batch');
INSERT INTO prm_sch_dict VALUES ('##src_tabname##', 'src_schema.src_tabname', 'will be replaced to schemaname.tablename');
INSERT INTO prm_sch_dict VALUES ('##des_tabname##', 'des_schema.des_tabname', 'will be replaced to schemaname.tablename');
INSERT INTO prm_sch_dict VALUES ('##schema##', 'schema name', 'only for prm_sch_cmd');

---------------------------------------drop for replace----------------------------------
drop view if exists v_sch_etl cascade;
drop view if exists v_sch_cmd cascade;
drop trigger if exists trg_etl on prm_sch_etl cascade;
drop trigger if exists trg_cmd on prm_sch_cmd cascade;
drop function if exists func_trg_etl_cmd() cascade;

---------------------------------------v_sch_etl---------------------------------------
create or replace view v_sch_etl as
select sno,psno,
	src_dbname,src_db_ip,src_db_port,src_db_user,src_db_passwd,src_schema,src_tabname,
	regexp_replace(src_qry_sql,E'(\r\n)|\n',' ','g') src_qry_sql,
	des_dbname,des_db_ip,des_db_port,des_db_user,des_db_passwd,des_schema,des_tabname,
	regexp_replace(des_clr_sql,E'(\r\n)|\n',' ','g') des_clr_sql
from(
	select 
		t.sno,
		case when 
			t.psno is null or t.psno = '{}' then '' 
		else
			(select '|' || array_to_string(array_agg(t2.sno||'-'||t2.src_schema||'-'||t2.des_schema), '|')::text
				from(
					select t1.sno,coalesce(a1.schema_name,'') src_schema,b1.schema_name des_schema
					from (
						with etl_cmd as(
							select sno,psno,src_dbname,src_schema,des_dbname,des_schema from prm_sch_etl	union all	
							select sno,psno,null,null,db_name,schema_name from prm_sch_cmd
						)
						select x.sno,y.src_dbname,y.src_schema,y.des_dbname,y.des_schema
						from (select unnest(t.psno) sno) x,etl_cmd y where x.sno=y.sno
					) t1
					left join prm_sch_schema a1 on t1.src_dbname =a1.db_name
						and (case when t1.src_schema ='*' then a1.schema_name else t1.src_schema end) =a1.schema_name
					left join prm_sch_schema b1 on t1.des_dbname =b1.db_name
						and (case when t1.des_schema ='*' then b1.schema_name else t1.des_schema end) =b1.schema_name
					order by t1.sno,a1.schema_name,b1.schema_name
				) t2
			)
		end psno,
		t.src_dbname,
		a.ip src_db_ip, a.port src_db_port,
		a.username src_db_user,
		coalesce(a.password,'') src_db_passwd,
		a.schema_name src_schema,
		t.src_tabname,
		case when t.src_qry_sql ='*' then
			case when t.src_schema ='*' then 'select * from ' ||a.schema_name || '.' ||t.src_tabname
			else 'select * from ' || t.src_schema || '.' || t.src_tabname end
		else
			case when t.src_schema ='*' then replace(t.src_qry_sql,'##src_tabname##',a.schema_name ||'.' ||t.src_tabname)
			else replace(src_qry_sql,'##src_tabname##',t.src_schema ||'.' ||t.src_tabname)  end
		end src_qry_sql,
		t.des_dbname, b.ip des_db_ip, b.port des_db_port,
		b.username des_db_user,
		coalesce(b.password,'') des_db_passwd,
		b.schema_name des_schema,
		t.des_tabname,
		case when 
			row_number() over(partition by t.sno,b.db_name,b.schema_name order by a.db_name,a.schema_name,t.src_tabname,t.des_tabname)=1 
			then (
				case when t.des_clr_sql ='*' then 'truncate table ' ||b.schema_name || '.' || t.des_tabname 
				else replace(coalesce(t.des_clr_sql,''),'##des_tabname##',b.schema_name ||'.' ||t.des_tabname) end
				--replace(des_clr_sql,'##des_tabname##',b.schema_name ||'.' ||t.des_tabname) des_clr_sql
			)
		else '' end des_clr_sql
	from prm_sch_etl t
	left join prm_sch_schema a on t.src_dbname =a.db_name
		and (case when t.src_schema ='*' then a.schema_name else t.src_schema end) =a.schema_name
	left join prm_sch_schema b on t.des_dbname =b.db_name
		and (case when t.des_schema ='*' then b.schema_name else t.des_schema end) =b.schema_name
	where t.enable
	order by t.sno,b.db_name,b.schema_name,a.db_name,a.schema_name,t.src_tabname,t.des_tabname
) tv;

---------------------------------------v_sch_cmd---------------------------------------
create or replace view v_sch_cmd as
select sno,psno,db_name,schema_name,ip,port,username,password,
	regexp_replace(sql_text,E'(\r\n)|\n',' ','g') sql_text
from(
	select 
		t.sno,
		case when 
			t.psno is null or t.psno = '{}' then '' 
		else
			(select '|' || array_to_string(array_agg(t2.sno||'-'||t2.src_schema||'-'||t2.des_schema), '|')::text
				from(
					select t1.sno,coalesce(a1.schema_name,'') src_schema,b1.schema_name des_schema
					from (
						with etl_cmd as(
							select sno,psno,src_dbname,src_schema,des_dbname,des_schema from prm_sch_etl	union all	
							select sno,psno,null,null,db_name,schema_name from prm_sch_cmd
						)
						select x.sno,y.src_dbname,y.src_schema,y.des_dbname,y.des_schema
						from (select unnest(t.psno) sno) x,etl_cmd y where x.sno=y.sno
					) t1
					left join prm_sch_schema a1 on t1.src_dbname =a1.db_name
						and (case when t1.src_schema ='*' then a1.schema_name else t1.src_schema end) =a1.schema_name
					left join prm_sch_schema b1 on t1.des_dbname =b1.db_name
						and (case when t1.des_schema ='*' then b1.schema_name else t1.des_schema end) =b1.schema_name
					order by t1.sno,a1.schema_name,b1.schema_name
				) t2
			)
		end psno,
	t.db_name,a.schema_name,a.ip,a.port,a.username,coalesce(a.password,'') as password,
	replace(t.sql_text,'##schema##',a.schema_name) sql_text
	from prm_sch_cmd t
	left join prm_sch_schema a on t.db_name =a.db_name 
		and (case when t.schema_name ='*' then a.schema_name else t.schema_name end) =a.schema_name
	where t.enable
	order by t.sno
) tv;

---------------------------------------func_trg_etl_cmd---------------------------------------
CREATE OR REPLACE FUNCTION func_trg_etl_cmd()
  RETURNS trigger AS $BODY$
declare
	n int;
begin
	execute 'set search_path=' || TG_TABLE_SCHEMA;
	IF (TG_OP in ('INSERT','UPDATE')) THEN
		/*check sno unique with prm_sch_etl union prm_sch_cmd*/
		select count(*) into n from prm_sch_etl t1, prm_sch_cmd t2 where t1.sno =t2.sno;
		if(n>0) then raise exception 'duplicate key value violates for sno in prm_sch_etl and prm_sch_cmd';end if;
		
		/*check schema name*/
		IF (TG_TABLE_NAME = 'prm_sch_etl') THEN
			select count(*) into n from prm_sch_schema where db_name =new.src_dbname;
			if(n=0) then raise exception 'src_dbname: % is not defined in prm_sch_schema',new.src_dbname;end if;
			
			select count(*) into n from prm_sch_schema where db_name =new.des_dbname;
			if(n=0) then raise exception 'des_dbname: % is not defined in prm_sch_schema',new.des_dbname;end if;
	
			if(new.src_schema<>'*') then
				select count(*) into n from prm_sch_schema where schema_name =new.src_schema;
				if(n=0) then raise exception 'src_schema: % is not defined in prm_sch_schema',new.src_schema;end if;
			end if;
	
			if(new.des_schema<>'*') then
				select count(*) into n from prm_sch_schema where schema_name =new.des_schema;
				if(n=0) then raise exception 'src_schema: % is not defined in prm_sch_schema',new.des_schema;end if;
			end if;
	
			select count(*) into n from prm_sch_etl
			where src_dbname =new.src_dbname and src_schema =new.src_schema
			and src_tabname =new.src_tabname and des_tabname =new.des_tabname
			and sno <> new.sno;
			if(n>0) then 
				raise exception 'duplicate key value violates for column "src_dbname, src_schema, src_tabname, des_tabname"';
			end if;
		ELSEIF (TG_TABLE_NAME = 'prm_sch_cmd') THEN
			select count(*) into n from prm_sch_schema where db_name =new.db_name;
			if(n=0) then raise exception 'des_dbname: % is not defined in prm_sch_schema',new.db_name;end if;
	
			if(new.schema_name<>'*') then
				select count(*) into n from prm_sch_schema where schema_name =new.schema_name;
				if(n=0) then raise exception 'src_schema: % is not defined in prm_sch_schema',new.schema_name;end if;
			end if;
		END IF;

		/*check the snoes in psno */
		with t1 as(
			select sno,psno from prm_sch_etl where enable
			union all
			select sno,psno from prm_sch_cmd where enable
		)
		select count(*) into n from t1 where sno =any(new.psno);
		if(n<>array_length(new.psno,1)) then
			raise exception 'sno in % invalid or redundant',new.psno;
		end if;
		
		/*check dead lock*/
		with recursive t1 as(
			select sno,psno from prm_sch_etl
			union all
			select sno,psno from prm_sch_cmd
		),
		t2 as(
			/*select sno,psno from t1 union*/
			select new.sno,new.psno union
			select t1.sno,array_cat(t1.psno,t2.psno) psno 
			from t1
			inner join t2 on array[t2.sno]<@t1.psno
			where array_length(t2.psno,1)<32
		)
		/*select * from t2;*/
		select count(*) into n from(
			select sno,max(psno) from t2 
			group by sno having array_length(max(psno),1)=32
		) t;

		if(n>0) then raise exception 'dead lock detected with psno:% in prm_sch_etl union prm_sch_cmd',new.psno;end if;
	END IF;
	
	IF(TG_OP ='DELETE' or (TG_OP ='UPDATE' and new.enable <>old.enable and not new.enable)) THEN
		/*check dependency relationship*/
		with t1 as(
			select sno,psno from prm_sch_etl where enable
			union all
			select sno,psno from prm_sch_cmd where enable
		)
		select count(*) into n from t1 where array[old.sno]<@psno;
		if(n>0) then raise exception 'delete not allowed when sno:% is referenced by any psno in prm_sch_etl union prm_sch_cmd',old.sno;end if;
	END IF;

	return null;

	end;
$BODY$
  LANGUAGE 'plpgsql' VOLATILE COST 100
;

---------------------------------------trg_etl---------------------------------------
create trigger trg_etl
after insert or update or delete
on prm_sch_etl for each row
execute procedure func_trg_etl_cmd();

---------------------------------------trg_cmd---------------------------------------
create trigger trg_cmd
after insert or update or delete
on prm_sch_cmd for each row
execute procedure func_trg_etl_cmd();

