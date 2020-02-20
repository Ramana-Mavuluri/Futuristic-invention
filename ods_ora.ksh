#!/bin/ksh

#------Sourcing Config file ----------#
#USERID=`whoami`
#MEPA_CONFIG_PATH=`getent passwd ${USERID} | cut -d: -f6`
. ${ODS_ConfigFolder}/MEPA_ODSConfig.dat


#---Initialize variables---#

ods_passw=`cat $ODSEncryptFolder/${SrcUserIDPwdFile}`
export ods_passw
export SrcUserPW=`perl -e 'print pack "H*",$ENV{"ods_passw"}'`

export DataSrcName=${DataSrcName}
export SrcUserID=${SrcUserID}
#echo "${SrcUserID}/${SrcUserPW}@${DataSrcName}"


ConnStr=${SrcUserID}/${SrcUserPW}@${DataSrcName}


function get_ods_bizdt
{
CtryCode=$1
RunFrq=$2
DateType=$3

sqlplus -s <<-here 2>&1
${ConnStr}
 set heading off;
SELECT TRIM(TO_CHAR(C_CRN_BATCH_PARAM.CRN_BATCH_DT,'$DateType'))
 FROM ${ODSSchema}.C_CRN_BATCH_PARAM
WHERE C_CRN_BATCH_PARAM.CTY_CODE='$CtryCode';

 quit;
here
}

function get_process_count
{
ctry_code=$1
system_code=$2
process_code=$3
frequency=$4
business_date=$5
ConnStr1=$6

sqlplus -s <<-here 2>&1
${ConnStr1}
set heading off;

SELECT count(DISTINCT b.job_name)
           FROM ${ODSSchema}.ods_ctrlm_jobs_dependency a,
                (SELECT job_name, ds_seq_name, project_name
                   FROM ${ODSSchema}.ods_ctrlm_jobs
                  WHERE ctry_code = '${ctry_code}'
                    AND system_name = '${system_code}'
                    AND process_name = '${process_code}'
                    AND TRUNC (business_date) = TO_DATE ('$business_date', 'DDMMYYYY')
                    AND frequency = '${frequency}'
                    AND run_status = 'N') b
          WHERE a.in_cond IN (SELECT job_name
                                FROM ${ODSSchema}.ods_ctrlm_jobs
                               WHERE run_status IN ('Y', 'S'))
            AND a.job_name NOT IN (
                   SELECT job_name
                     FROM ${ODSSchema}.ods_ctrlm_jobs_dependency
                    WHERE in_cond IN (
                                 SELECT job_name
                                   FROM ${ODSSchema}.ods_ctrlm_jobs
                                  WHERE run_status IN ('N', 'F', 'M', 'H', 'P')))
            AND a.job_name = b.job_name;

   quit;
here
}

function get_sub_process_count
{
ctry_code=$1
system_code=$2
sub_system_code=$3
process_code=$4
frequency=$5
business_date=$6

sqlplus -s <<-here 2>&1
${ConnStr}
set heading off;

SELECT count(DISTINCT b.job_name)
           FROM ${ODSSchema}.ods_ctrlm_jobs_dependency a,
                (SELECT job_name, ds_seq_name, project_name
                   FROM ${ODSSchema}.ods_ctrlm_jobs
                  WHERE ctry_code = '${ctry_code}'
                    AND system_name = '${system_code}'
                    AND sub_system_name = '${sub_system_code}'
                    AND process_name = '${process_code}'
                    AND TRUNC (business_date) = TO_DATE ('$business_date', 'DDMMYYYY')
                    AND frequency = '${frequency}'
                    AND run_status = 'N') b
          WHERE a.in_cond IN (SELECT job_name
                                FROM ${ODSSchema}.ods_ctrlm_jobs
                               WHERE run_status IN ('Y', 'S'))
            AND a.job_name NOT IN (
                   SELECT job_name
                     FROM ${ODSSchema}.ods_ctrlm_jobs_dependency
                    WHERE in_cond IN (
                                 SELECT job_name
                                   FROM ${ODSSchema}.ods_ctrlm_jobs
                                  WHERE run_status IN ('N', 'F', 'M', 'H', 'P')))
            AND a.job_name = b.job_name;

   quit;
here
}

function get_load_process_count
{
ctry_code=$1
system_code=$2
process_code=$3
frequency=$4
business_date=$5
ConnStrl=$6

sqlplus -s <<-here 2>&1
${ConnStr1}
set heading off;

select count(1)
  from ${ODSSchema}.ods_ctrlm_jobs a
 where a.ctry_code            = '${ctry_code}'
   and a.system_name          = '${system_code}'
   and a.process_name         = '${process_code}'
   and trunc(a.business_date) = to_date('$business_date','DDMMYYYY')
   and a.frequency            = '${frequency}'
   and a.run_status           = 'N'
   and a.file_avail_status   in ('Y','S');
   quit;
here
}

function get_load_process_count_pt
{
ctry_code=$1
system_code=$2
sub_system_code=$3
process_code=$4
frequency=$5
business_date=$6

sqlplus -s <<-here 2>&1
${ConnStr}
set heading off;

select count(1)
  from ${ODSSchema}.ods_ctrlm_jobs a
 where a.ctry_code            = '${ctry_code}'
   and a.system_name          = '${system_code}'
   and a.sub_system_name      = '${sub_system_code}'
   and a.process_name         = '${process_code}'
   and trunc(a.business_date) = to_date('$business_date','DDMMYYYY')
   and a.frequency            = '${frequency}'
   and a.run_status           = 'N'
   and a.file_avail_status   in ('Y','S');
   quit;
here
}

function get_job_list
{
ctry_code=$1
system_code=$2
process_code=$3
frequency=$4
business_date=$5
ConnStrl=$6

sqlplus -s <<-here 2>&1
${ConnStr1}
set heading off;
set linesize 1000;

select job_dtls from (
SELECT b.job_name||'|'||b.project_name||'|'||b.ds_seq_name job_dtls, b.priority
           FROM ${ODSSchema}.ods_ctrlm_jobs_dependency a,
                (SELECT job_name, ds_seq_name, project_name, nvl(priority,10) priority
                   FROM ${ODSSchema}.ods_ctrlm_jobs
                  WHERE ctry_code = '${ctry_code}'
                    AND system_name = '${system_code}'
                    AND process_name = '${process_code}'
                    AND TRUNC (business_date) = TO_DATE ('$business_date', 'DDMMYYYY')
                    AND frequency = '${frequency}'
                    AND run_status = 'N') b
          WHERE a.in_cond IN (SELECT job_name
                                FROM ${ODSSchema}.ods_ctrlm_jobs
                               WHERE run_status IN ('Y', 'S'))
            AND a.job_name NOT IN (
                   SELECT job_name
                     FROM ${ODSSchema}.ods_ctrlm_jobs_dependency
                    WHERE in_cond IN (
                                 SELECT job_name
                                   FROM ${ODSSchema}.ods_ctrlm_jobs
                                  WHERE run_status IN ('N', 'F', 'M', 'H', 'P')))
            AND a.job_name = b.job_name
order by b.priority)
          WHERE rownum < 2;

   quit;
here
}

function get_sub_job_list
{
ctry_code=$1
system_code=$2
sub_system_code=$3
process_code=$4
frequency=$5
business_date=$6

sqlplus -s <<-here 2>&1
${ConnStr}
set heading off;
set linesize 1000;

select a.job_name||'|'||a.project_name||'|'||a.ds_seq_name
  from ${ODSSchema}.ods_ctrlm_jobs a
 where a.ctry_code            = '${ctry_code}'
   and a.system_name          = '${system_code}'
   and a.sub_system_name      = '${sub_system_code}'
   and a.process_name         = '${process_code}'
   and trunc(a.business_date) = to_date('$business_date','DDMMYYYY')
   and a.frequency            = '${frequency}';
   quit;
here
}

function get_load_job_list
{
ctry_code=$1
system_code=$2
process_code=$3
frequency=$4
business_date=$5
ConnStrl=$6

sqlplus -s <<-here 2>&1
${ConnStr1}
set heading off;
set linesize 1000;

select job_dtls from (
select a.job_name||'|'||a.project_name||'|'||a.ds_seq_name job_dtls, nvl(a.priority,10) priority
  from ${ODSSchema}.ods_ctrlm_jobs a
 where a.ctry_code            = '${ctry_code}'
   and a.system_name          = '${system_code}'
   and a.process_name         = '${process_code}'
   and trunc(a.business_date) = to_date('$business_date','DDMMYYYY')
   and a.frequency            = '${frequency}'
   and a.run_status           = 'N'
   and a.file_avail_status   in ('Y','S')
 order by priority)
 where rownum < 2;
   quit;
here
}

function get_load_job_list_pt
{
ctry_code=$1
system_code=$2
sub_system_code=$3
process_code=$4
frequency=$5
business_date=$6

sqlplus -s <<-here 2>&1
${ConnStr}
set heading off;
set linesize 1000;

select job_dtls from (
select a.job_name||'|'||a.project_name||'|'||a.ds_seq_name job_dtls, nvl(a.priority,10) priority
  from ${ODSSchema}.ods_ctrlm_jobs a
 where a.ctry_code            = '${ctry_code}'
   and a.system_name          = '${system_code}'
   and a.sub_system_name      = '${sub_system_code}'
   and a.process_name         = '${process_code}'
   and trunc(a.business_date) = to_date('$business_date','DDMMYYYY')
   and a.frequency            = '${frequency}'
   and a.run_status           = 'N'
   and a.file_avail_status   in ('Y','S')
 order by priority)
 where rownum < 2;
   quit;
here
}

function get_file_list
{
ctry_code=$1
system_code=$2
process_code=$3
frequency=$4
business_date=$5

sqlplus -s <<-here 2>&1
${ConnStr}
set heading off;
set linesize 1000;

select file_details from (
select source_path||ack_file_name||'|'||job_name file_details, nvl(priority,10) priority
  from ${ODSSchema}.ods_ctrlm_jobs
 where ctry_code            = '${ctry_code}'
   and system_name          = '${system_code}'
   and process_name         = '${process_code}'
   and trunc(business_date) = to_date('$business_date','DDMMYYYY')
   and frequency            = '${frequency}'
   and file_avail_status    = 'N'
order by priority)
 where rownum < 2;
   quit;
here
}

function get_ack_file_list
{
ctry_code=$1
system_code=$2
process_code=$3
frequency=$4
business_date=$5
file_status=$6

sqlplus -s <<-here 2>&1
${ConnStr}
set heading off;
set linesize 1000;
set feedback off;

select file_details from (
select source_path||ack_file_name||'|'||job_name file_details, nvl(priority,10) priority
  from ${ODSSchema}.ods_ctrlm_jobs
 where ctry_code            = '${ctry_code}'
   and system_name          = '${system_code}'
   and process_name         = '${process_code}'
   and trunc(business_date) = to_date('$business_date','DDMMYYYY')
   and frequency            = '${frequency}'
   and file_avail_status    = '${file_status}'
order by priority);
   quit;
here
}

function get_file_list_pt
{
ctry_code=$1
system_code=$2
sub_system_code=$3
process_code=$4
frequency=$5
business_date=$6

sqlplus -s <<-here 2>&1
${ConnStr}
set heading off;
set linesize 1000;

select file_details from (
select source_path||ack_file_name||'|'||job_name file_details, nvl(priority,10) priority
  from ${ODSSchema}.ods_ctrlm_jobs
 where ctry_code            = '${ctry_code}'
   and system_name          = '${system_code}'
   and sub_system_name      = '${sub_system_code}'
   and process_name         = '${process_code}'
   and trunc(business_date) = to_date('$business_date','DDMMYYYY')
   and frequency            = '${frequency}'
   and file_avail_status    = 'N'
order by priority)
 where rownum < 2;
   quit;
here
}

function get_ack_file_list_pt
{
ctry_code=$1
system_code=$2
sub_system_code=$3
process_code=$4
frequency=$5
business_date=$6
file_status=$7

sqlplus -s <<-here 2>&1
${ConnStr}
set heading off;
set linesize 1000;
set feedback off;

select file_details from (
select source_path||ack_file_name||'|'||job_name file_details, nvl(priority,10) priority
  from ${ODSSchema}.ods_ctrlm_jobs
 where ctry_code            = '${ctry_code}'
   and system_name          = '${system_code}'
   and sub_system_name      = '${sub_system_code}'
   and process_name         = '${process_code}'
   and trunc(business_date) = to_date('$business_date','DDMMYYYY')
   and frequency            = '${frequency}'
   and file_avail_status    = '${file_status}'
order by priority);
   quit;
here
}

function update_job_status
{
ctry_code=$1
system_code=$2
process_code=$3
business_date=$4
job_name=$5
status=$6
ConnStr1=$7

sqlplus -s <<-here 2>&1
${ConnStr1}

update ${ODSSchema}.ods_ctrlm_jobs set run_status = '${status}'
         where ctry_code            = '${ctry_code}'
           and system_name          = '${system_code}'
           and process_name         = '${process_code}'
           and job_name             = '${job_name}'
           and trunc(business_date) = to_date('$business_date','DDMMYYYY');

   commit;
   quit;
here
}

function update_sub_job_status
{
ctry_code=$1
system_code=$2
sub_system_code=$3
process_code=$4
business_date=$5
status=$6
ConnStr1=$7

sqlplus -s <<-here 2>&1
${ConnStr1}

update ${ODSSchema}.ods_ctrlm_jobs set run_status = '${status}'
         where ctry_code            = '${ctry_code}'
           and system_name          = '${system_code}'
           and sub_system_name      = '${sub_system_code}'
           and process_name         = '${process_code}'
           and trunc(business_date) = to_date('$business_date','DDMMYYYY');

   commit;
   quit;
here
}

function update_mail_status
{
ctry_code=$1
system_code=$2
process_code=$3
business_date=$4
status=$5
ConnStrl=$6

sqlplus -s <<-here 2>&1
${ConnStr1}

update ${ODSSchema}.ods_ctrlm_jobs set run_status = 'M'
         where ctry_code            = '${ctry_code}'
           and system_name          = '${system_code}'
           and process_name         = '${process_code}'
	   and trunc(business_date) = to_date('$business_date','DDMMYYYY')
           and run_status           = 'F';

   commit;
   quit;
here
}

function update_mail_status_pt
{
ctry_code=$1
system_code=$2
sub_system_code=$3
process_code=$4
business_date=$5
status=$6

sqlplus -s <<-here 2>&1
${ConnStr}

update ${ODSSchema}.ods_ctrlm_jobs set run_status = 'M'
         where ctry_code            = '${ctry_code}'
           and system_name          = '${system_code}'
           and sub_system_name      = '${sub_system_code}'
           and process_name         = '${process_code}'
	   and trunc(business_date) = to_date('$business_date','DDMMYYYY')
           and run_status           = 'F';

   commit;
   quit;
here
}

function update_file_status
{
ctry_code=$1
system_code=$2
process_code=$3
business_date=$4
job_name=$5
status=$6

sqlplus -s <<-here 2>&1
${ConnStr}

update ${ODSSchema}.ods_ctrlm_jobs set file_avail_status = '${status}'
         where ctry_code            = '${ctry_code}'
           and system_name          = '${system_code}'
           and process_name         = '${process_code}'
           and job_name             = '${job_name}'
	   and trunc(business_date) = to_date('$business_date','DDMMYYYY');

   commit;
   quit;
here
}

function get_job_count
{
ctry_code=$1
system_code=$2
process_code=$3
frequency=$4
business_date=$5
job_status=$6
ConnStrl=$7

sqlplus -s <<-here 2>&1
${ConnStr1}
set heading off;

select count(1) from ${ODSSchema}.ods_ctrlm_jobs
         where ctry_code            = '${ctry_code}'
           and system_name          = '${system_code}'
           and process_name         = '${process_code}'
           and trunc(business_date) = to_date('$business_date','DDMMYYYY')
           and frequency = '${frequency}'
           and run_status = '${job_status}';

   quit;
here
}

function get_sub_job_count
{
ctry_code=$1
system_code=$2
sub_system_code=$3
process_code=$4
frequency=$5
business_date=$6
job_status=$7

sqlplus -s <<-here 2>&1
${ConnStr}
set heading off;

select count(1) from ${ODSSchema}.ods_ctrlm_jobs
         where ctry_code            = '${ctry_code}'
           and system_name          = '${system_code}'
           and sub_system_name      = '${sub_system_code}'
           and process_name         = '${process_code}'
           and trunc(business_date) = to_date('$business_date','DDMMYYYY')
           and frequency = '${frequency}'
           and run_status = '${job_status}';

   quit;
here
}

function get_sub_job_status
{
ctry_code=$1
system_code=$2
sub_system_code=$3
process_code=$4
frequency=$5
business_date=$6

sqlplus -s <<-here 2>&1
${ConnStr}
set heading off;

select run_status from ${ODSSchema}.ods_ctrlm_jobs
         where ctry_code            = '${ctry_code}'
           and system_name          = '${system_code}'
           and sub_system_name      = '${sub_system_code}'
           and process_name         = '${process_code}'
           and trunc(business_date) = to_date('$business_date','DDMMYYYY')
           and frequency = '${frequency}';

   quit;
here
}

function get_isis_job_status
{
ctry_code=$1
system_code=$2
sub_system_code=$3
process_code=$4
frequency=$5
ConnStrl=$6

sqlplus -s <<-here 2>&1
${ConnStr1}
set heading off;

select run_status from ${ODSSchema}.ods_ctrlm_jobs
         where ctry_code            = '${ctry_code}'
           and system_name          = '${system_code}'
           and sub_system_name      = '${sub_system_code}'
           and process_name         = '${process_code}'
           and frequency = '${frequency}';

   quit;
here
}

function get_load_job_count
{
ctry_code=$1
system_code=$2
process_code=$3
frequency=$4
business_date=$5
job_status=$6
ConnStrl=$7

sqlplus -s <<-here 2>&1
${ConnStr1}
set heading off;

select count(1) from ${ODSSchema}.ods_ctrlm_jobs
         where ctry_code            = '${ctry_code}'
           and system_name          = '${system_code}'
           and process_name         = '${process_code}'
           and trunc(business_date) = to_date('$business_date','DDMMYYYY')
           and frequency            = '${frequency}'
           and run_status           = '${job_status}';

   quit;
here
}

function get_file_count
{
ctry_code=$1
system_code=$2
process_code=$3
frequency=$4
business_date=$5
job_status=$6

sqlplus -s <<-here 2>&1
${ConnStr}
set heading off;

select count(1) from ${ODSSchema}.ods_ctrlm_jobs
         where ctry_code            = '${ctry_code}'
           and system_name          = '${system_code}'
           and process_name         = '${process_code}'
           and trunc(business_date) = to_date('$business_date','DDMMYYYY')
           and frequency            = '${frequency}'
           and file_avail_status    = '${job_status}';

   quit;
here
}

function get_file_count_pt
{
ctry_code=$1
system_code=$2
sub_system_code=$3
process_code=$4
frequency=$5
business_date=$6
job_status=$7

sqlplus -s <<-here 2>&1
${ConnStr}
set heading off;

select count(1) from ${ODSSchema}.ods_ctrlm_jobs
         where ctry_code            = '${ctry_code}'
           and system_name          = '${system_code}'
           and sub_system_name      = '${sub_system_code}'
           and process_name         = '${process_code}'
           and trunc(business_date) = to_date('$business_date','DDMMYYYY')
           and frequency            = '${frequency}'
           and file_avail_status    = '${job_status}';

   quit;
here
}

function get_ods_job_params
{
ctry_code=$1
system_code=$2
process_code=$3
sub_job_name=$4
ConnStrl=$5

sqlplus -s <<-here 2>&1
${ConnStr1}
set heading off;
set feedback off;

select param_name||'='||param_value
       from ${ODSSchema}.ods_ctrlm_jobs_params
      where ctry_code            = '${ctry_code}'
        and system_name          = '${system_code}'
        and process_name         = '${process_code}'
        and job_name             = '${sub_job_name}';

   quit;
here
}

function get_ods_param_count
{
ctry_code=$1
system_code=$2
process_code=$3
sub_job_name=$4
ConnStrl=$5

sqlplus -s <<-here 2>&1
${ConnStr1}
set heading off;

select count(1)
       from ${ODSSchema}.ods_ctrlm_jobs_params
      where ctry_code            = '${ctry_code}'
        and system_name          = '${system_code}'
        and process_name         = '${process_code}'
        and job_name             = '${sub_job_name}';

   quit;
here
}

function update_fw_time
{
ctry_code=$1
system_code=$2
process_code=$3

sqlplus -s <<-here 2>&1
${ConnStr}

update ${ODSSchema}.ods_job_limit set start_time = sysdate
         where ctry_code            = '${ctry_code}'
           and system_name          = '${system_code}'
           and process_name         = '${process_code}';

   commit;
   quit;
here
}

function update_fw_time_pt
{
ctry_code=$1
system_code=$2
sub_system_code=$3
process_code=$4

sqlplus -s <<-here 2>&1
${ConnStr}

update ${ODSSchema}.ods_job_limit set start_time = sysdate
         where ctry_code            = '${ctry_code}'
           and system_name          = '${system_code}'
           and sub_system_name      = '${sub_system_code}'
           and process_name         = '${process_code}';

   commit;
   quit;
here
}

function get_time_limit
{
ctry_code=$1
system_code=$2
process_code=$3

sqlplus -s <<-here 2>&1
${ConnStr}
set heading off;

select case when floor((sysdate-start_time)*24*60) > fw_time_limit 
             then 'Y' 
        else 'N' 
        end time_difference
       from ${ODSSchema}.ods_job_limit
      where ctry_code            = '${ctry_code}'
        and system_name          = '${system_code}'
        and process_name         = '${process_code}';

   quit;
here
}

function get_time_limit_pt
{
ctry_code=$1
system_code=$2
sub_system_code=$3
process_code=$4

sqlplus -s <<-here 2>&1
${ConnStr}
set heading off;

select case when floor((sysdate-start_time)*24*60) > fw_time_limit 
             then 'Y' 
        else 'N' 
        end time_difference
       from ${ODSSchema}.ods_job_limit
      where ctry_code            = '${ctry_code}'
        and system_name          = '${system_code}'
        and sub_system_name      = '${sub_system_code}'
        and process_name         = '${process_code}';

   quit;
here
}


function get_job_limit
{
ctry_code=$1
system_code=$2
process_code=$3
flag=$4
ConnStrl=$5

result=`sqlplus -s <<here 2>&1
${ConnStr1}
set serveroutput on size 10000;
set head off;

declare
a varchar(100);
begin
a:=FUN_GET_JOB_LIMIT ( '${ctry_code}','${system_code}','${process_code}','${flag}');
dbms_output.put_line(a);
end;
/
exit;
here`
echo "$result"
return "${result}"
}

function get_process_limit
{
ctry_code=$1
process_code=$2
flag=$3

result=`sqlplus -s <<here 2>&1
${ConnStr}
set serveroutput on size 10000;
set head off;

declare
a varchar(100);
begin
a:=FUN_GET_PROCESS_LIMIT ( '${ctry_code}','${process_code}','${flag}');
dbms_output.put_line(a);
end;
/
exit;
here`
echo "$result"
return "${result}"
}

function init_ctrl_tbl
{
ctry_code=$1
freq=$2
status=$3
biz_date=$4

sqlplus -s <<-here 2>&1
${ConnStr}

update ${ODSSchema}.ods_ctrlm_jobs 
   set run_status    = '${status}', 
       business_date = to_date('${biz_date}','DDMMYYYY'),
       start_time    = null,
       end_time      = null,
       retry_count   = 0,
       file_count    = null
 where ctry_code     = '${ctry_code}'
   and frequency     = '${freq}';

   commit;
   quit;
here
}

function ins_ctrl_tbl_hist
{
ctry_code=$1
freq=$2

sqlplus -s <<-here 2>&1
${ConnStr}

INSERT INTO ${ODSSchema}.ods_ctrlm_jobs_hist
            (business_date, ctry_code, system_name, sub_system_name,
             process_name, job_name, ds_seq_name, source_file_format,
             source_path, source_file_name, file_avail_status, run_status,
             project_name, frequency, DATE_FORMAT, priority, ack_file_name,
             start_time, end_time, retry_count, src_sys, sub_process_name,file_count)
   SELECT business_date, ctry_code, system_name, sub_system_name,
          process_name, job_name, ds_seq_name, source_file_format,
          source_path, source_file_name, file_avail_status, run_status,
          project_name, frequency, DATE_FORMAT, priority, ack_file_name,
          start_time, end_time, retry_count, src_sys, sub_process_name,file_count
     FROM ${ODSSchema}.ods_ctrlm_jobs 
    where ctry_code = '${ctry_code}' 
      and frequency = '${freq}';

   commit;
   quit;
here
}


function ins_tgt_tbl_hist
{
ctry_code=$1
freq=$2

sqlplus -s <<-here 2>&1
${ConnStr}

INSERT INTO ${ODSSchema}.ods_target_files_hist
            (business_date, ctry_code, system_name, sub_system_name,
             process_name, job_name, TARGET_FILE_FORMAT, TARGET_PATH,
             TARGET_FILE_NAME, FREQUENCY, DATE_FORMAT, SERVICE_NAME,
             OVERRIDE_PATH, OVERRIDE_FILE_NAME, ISIS_STATUS, FTF_ID, FAIL_DESCRIPTION,
             FTF_TIME_LIMIT, FTF_START_TIME, SRC_SYS,SUB_PROCESS_NAME)
   SELECT    business_date, ctry_code, system_name, sub_system_name,
             process_name, job_name, TARGET_FILE_FORMAT, TARGET_PATH,
             TARGET_FILE_NAME, FREQUENCY, DATE_FORMAT, SERVICE_NAME,
             OVERRIDE_PATH, OVERRIDE_FILE_NAME, ISIS_STATUS, FTF_ID, FAIL_DESCRIPTION,
             FTF_TIME_LIMIT, FTF_START_TIME, SRC_SYS,SUB_PROCESS_NAME
     FROM ${ODSSchema}.ods_target_files
    where ctry_code = '${ctry_code}'
     and frequency = '${freq}';   

   commit;
   quit;
here
}

function init_tgt_tbl
{
ctry_code=$1
freq=$2
status=$3
biz_date=$4

sqlplus -s <<-here 2>&1
${ConnStr}


update ${ODSSchema}.ods_target_files set business_date = to_date('${biz_date}','DDMMYYYY')
       where ctry_code            = '${ctry_code}'
         and frequency            = '${freq}';

   commit;
   quit;

here
}

function upd_file_status
{
ctry_code=$1
freq=$2
status=$3

sqlplus -s <<-here 2>&1
${ConnStr}

update ${ODSSchema}.ods_ctrlm_jobs set file_avail_status = '${status}',
                          source_file_name = replace(source_file_format,date_format,to_char(business_date,date_format))
       where ctry_code            = '${ctry_code}'
         and frequency            = '${freq}'
         and process_name         like 'STAGING%';

   commit;
   quit;
here
}

function init_isis_status
{
ctry_code=$1
freq=$2
status=$3
biz_date=$4

sqlplus -s <<-here 2>&1
${ConnStr}


update ${ODSSchema}.ods_target_files 
   set isis_status      = null,
       ftf_id           = null, 
       fail_description = null,
       ftf_start_time   = null,
       target_file_name = replace(target_file_format,date_format,to_char(business_date,date_format)),
	   override_file_name = replace(target_file_format,date_format,to_char(business_date,date_format))
 where ctry_code        = '${ctry_code}'
   and frequency        = '${freq}';

   commit;
<<COMMENT1   
   update ${ODSSchema}.ods_target_files 
   set isis_status      = '${status}',
       ftf_id           = null, 
       fail_description = null,
       ftf_start_time   = null,
       target_file_name = replace(target_file_format,date_format,to_char(business_date,date_format)),
	   override_file_name = replace(target_file_format,date_format,to_char(business_date,date_format))
 where ctry_code        = '${ctry_code}'
   and frequency        = '${freq}'
   and process_name	   = 'STAR_ISIS';

   commit;

 COMMENT 
   quit;
here
}

function upd_target_files_status
{
ctry_code=$1
sys_code=$2
sub_sys_code=$3
proc_code=$4
freq=$5
status=$6
ConnStr1=$7

sqlplus -s <<-here 2>&1
${ConnStr1}

update ${ODSSchema}.ods_target_files set isis_status = '${status}',ftf_id = null, fail_description = null
       where ctry_code            = '${ctry_code}'
         and system_name          = '${sys_code}'
         and sub_system_name      = '${sub_sys_code}'
         and process_name         = '${proc_code}'
         and frequency            = '${freq}';

   commit;
   quit;

here
}

function roll_out_biz_date
{
ctry=$1
run_freq=$2

sqlplus -s <<-here 2>&1
${ConnStr}

update ${ODSSchema}.C_CRN_BATCH_PARAM
   set PREV_BATCH_DT = (select crn_batch_dt from ${ODSSchema}.C_CRN_BATCH_PARAM where CTY_CODE = '${ctry}') 
  where CTY_CODE = '${ctry}';

COMMIT;

update ${ODSSchema}.C_CRN_BATCH_PARAM
   set CRN_BATCH_DT = (select next_batch_dt from ${ODSSchema}.C_CRN_BATCH_PARAM where CTY_CODE = '${ctry}')
 where CTY_CODE = '${ctry}';

commit;

update ${ODSSchema}.C_CRN_BATCH_PARAM
   set NEXT_BATCH_DT = (select next_batch_dt +1 from ${ODSSchema}.C_CRN_BATCH_PARAM where CTY_CODE = '${ctry}')
 where CTY_CODE = '${ctry}';

commit;

quit;
here
}

function get_src_dir_path
{
ctry_code=$1
freq=$2
biz_date=$3

sqlplus -s <<-here 2>&1
${ConnStr}
set heading off;
set feedback off;

select distinct source_path
       from ${ODSSchema}.ods_ctrlm_jobs
      where ctry_code            = '${ctry_code}'
        and frequency            = '${freq}'
        and trunc(business_date) = to_date('$biz_date','DDMMYYYY')
        and source_path is not null
        and process_name = 'STAGING';

   quit;
here
}

function get_tgt_dir_path
{
ctry_code=$1
freq=$2
biz_date=$3

sqlplus -s <<-here 2>&1
${ConnStr}
set heading off;
set feedback off;

select distinct target_path
       from ${ODSSchema}.ods_target_files
      where ctry_code            = '${ctry_code}'
        and frequency            = '${freq}'
        and trunc(business_date) = to_date('$biz_date','DDMMYYYY')
        and target_path is not null;

   quit;
here
}

function get_target_file_list
{
ctry_code=$1
sys_name=$2
sub_sys_name=$3
process_name=$4
freq=$5

sqlplus -s <<-here 2>&1
${ConnStr}
set heading off;
set linesize 1000;

select ctry_code||'|'||system_name||'|'||sub_system_name||'|'||target_file_name||'|'||target_path||'|'||SERVICE_NAME||'|'||OVERRIDE_PATH||'|'||OVERRIDE_FILE_NAME
       from ${ODSSchema}.ods_target_files
      where ctry_code            = '${ctry_code}'
        and system_name          = '${sys_name}'
        and sub_system_name      = '${sub_sys_name}'
        and process_name         = '${process_name}'
        and frequency            = '${freq}'
        and isis_status = 'N';

   quit;
here
}

function upd_ftf_id
{
ctry_code=$1
sys_name=$2
sub_sys_name=$3
process_name=$4
file_name=$5
ftf_id=$6
isis_stat=$7
ConnStr1=$8

sqlplus -s <<-here 2>&1
${ConnStr1}

update ${ODSSchema}.ods_target_files 
   set ftf_id = '${ftf_id}',
       isis_status = '${isis_stat}'                         
      where ctry_code            = '${ctry_code}'
        and system_name          = '${sys_name}'
        and sub_system_name      = '${sub_sys_name}'
        and process_name         = '${process_name}'
        and target_file_name     = '${file_name}';

   commit;
   quit;
here
}

function get_ftf_file_list
{
ctry_code=$1
sys_name=$2
sub_sys_name=$3
process_name=$4
freq=$5

sqlplus -s <<-here 2>&1
${ConnStr}
set heading off;
set linesize 1000;

select ctry_code||'|'||system_name||'|'||sub_system_name||'|'||target_file_name||'|'||ftf_id
       from ${ODSSchema}.ods_target_files
      where ctry_code            = '${ctry_code}'
        and system_name          = '${sys_name}'
        and sub_system_name      = '${sub_sys_name}'
        and process_name         = '${process_name}'
        and frequency            = '${freq}'
        and isis_status = 'P';

   quit;
here
}

function get_ftf_count
{
ctry_code=$1
sys_name=$2
sub_sys_name=$3
process_name=$4
freq=$5
isis_stat=$6

sqlplus -s <<-here 2>&1
${ConnStr}
set heading off;

select count(1)
       from ${ODSSchema}.ods_target_files
      where ctry_code            = '${ctry_code}'
        and system_name          = '${sys_name}'
        and sub_system_name      = '${sub_sys_name}'
        and process_name         = '${process_name}'
        and frequency            = '${freq}'
        and isis_status          = '${isis_stat}';

   quit;
here
}

function upd_isis_status
{
ctry_code=$1
sys_name=$2
sub_sys_name=$3
process_name=$4
ftf_id=$5
status=$6
fail_des=$7

sqlplus -s <<-here 2>&1
${ConnStr}

update ${ODSSchema}.ods_target_files set isis_status = '${status}', fail_description = '${fail_des}'
      where ctry_code            = '${ctry_code}'
        and system_name          = '${sys_name}'
        and sub_system_name      = '${sub_sys_name}'
        and process_name         = '${process_name}'
        and ftf_id               = '${ftf_id}';

   commit;
   quit;
here
}


function system_hol_chk
{
ctry_code=$1
sys_name=$2
biz_date=$3

sqlplus -s <<-here 2>&1
${ConnStr}
set heading off;

select instr(upper(holiday_desc),upper(trim(to_char(TO_DATE('${biz_date}','DDMMYYYY'), 'Day')))) from ${ODSSchema}.ods_src_system
where ctry_code = '${ctry_code}'
  and system_name = '${sys_name}';

   quit;
here
}

function general_hol_chk
{
ctry_code=$1
sys_name=$2
biz_date=$3

sqlplus -s <<-here 2>&1
${ConnStr}
set heading off;

select count(1) from ${ODSSchema}.ods_holiday_calendar 
where ctry_code = '${ctry_code}'
  and system_name = '${sys_name}'
   and trunc(business_date) = to_date('${biz_date}','DDMMYYYY');

   quit;
here
}


function chk_job_count
{
job_id=$1
ConnStrl=$2

sqlplus -s <<-here 2>&1
${ConnStr1}
set heading off;

select count(1) from ${ODSSchema}.ods_ctrlm_jobs
         where job_name = '${job_id}';

   quit;
here
}

function reset_job_status
{
ctm_job_name=$1
ctm_job_status=$2

sqlplus -s <<-here 2>&1
${ConnStr}

update ${ODSSchema}.ods_ctrlm_jobs set run_status = '${ctm_job_status}'
        where job_name     = '${ctm_job_name}';

   commit;
   quit;
here
}

function reset_file_status
{
ctm_job_name=$1
ctm_file_status=$2

sqlplus -s <<-here 2>&1
${ConnStr}

update ${ODSSchema}.ods_ctrlm_jobs set file_avail_status = '${ctm_file_status}'
        where job_name     = '${ctm_job_name}';

   commit;
   quit;
here
}

function reset_isis_status
{
ctm_job_name=$1
ctm_isis_status=$2

sqlplus -s <<-here 2>&1
${ConnStr}

/*
update ${ODSSchema}.ods_target_files set isis_status = '${ctm_isis_status}'
        where job_name     = '${ctm_job_name}';

   commit;
   quit;
*/
here
}

function init_params_table
{
ctry_code=$1
business_date=$2

sqlplus -s <<-here 2>&1
${ConnStr}

update ${ODSSchema}.ods_ctrlm_jobs_params set param_value = '${business_date}'
         where ctry_code  = '${ctry_code}'
           and param_name like '%BatchDate%';

   commit;
   quit;
here
}

function upd_date_param_hol
{
ctry_code=$1
business_date=$2

sqlplus -s <<-here 2>&1
${ConnStr}
set serveroutput on size 10000;
set head off;


/* 
          
begin
   ods_holiday_pkg.proc_holiday_date_param('${ctry_code}','${dly_bcp_date2}');
end;
/

update ${ODSSchema}.C_CRN_BATCH_PARAM
   set NEXT_BATCH_DT = (select nextworkingdate from holiday_date_param where CCY_CODE = '${ctry_code}')
 where CTY_CODE = '${ctry_code}';
commit;

exit;
*/

here
}



function upd_stmt_hol
{
ctry_code=$1
business_date=$2

sqlplus -s <<-here 2>&1
${ConnStr}

update ${ODSSchema}.ods_ctrlm_jobs 
   set run_status = 'S', file_avail_status = 'S'
 where job_name in (select distinct job_name 
                      from ${ODSSchema}.ods_stmt_dtls a, 
                           ${ODSSchema}.ods_stmt_jobs b
                     where ((instr(upper(holiday_desc),upper(trim(to_char(TO_DATE('${business_date}','DDMMYYYY'), 'Day')))) > 0) or 
                            (instr((holiday_desc),to_char(to_date('${business_date}','DDMMYYYY'),'DD'))) > 0) 
                       and a.ctry_code = '${ctry_code}'
                       and a.ctry_code = b.ctry_code                      
                       and a.system_name = b.system_name
                       and a.stmt_name = b.stmt_name);


   commit;
   quit;
here
}

function upd_stmt_work
{
ctry_code=$1
business_date=$2

sqlplus -s <<-here 2>&1
${ConnStr}

update ${ODSSchema}.ods_ctrlm_jobs 
   set run_status = 'N', file_avail_status = 'N'
 where job_name in (select distinct job_name 
                      from ${ODSSchema}.ods_stmt_dtls a, 
                           ${ODSSchema}.ods_stmt_jobs b
                     where ((instr(upper(working_desc),upper(trim(to_char(TO_DATE('${business_date}','DDMMYYYY'), 'Day')))) > 0) or 
                            (instr((working_desc),to_char(to_date('${business_date}','DDMMYYYY'),'DD'))) > 0) 
                       and a.ctry_code = '${ctry_code}'
                       and a.ctry_code = b.ctry_code
                       and a.stmt_name = b.stmt_name);

   commit;
   quit;
here
}

function upd_ctry_hol
{
ctry_code=$1
business_date=$2
run_freqency=$3

sqlplus -s <<-here 2>&1
${ConnStr}

update ${ODSSchema}.ods_ctrlm_jobs 
	set run_status = 'S', file_avail_status = 'S' 
 where frequency = '${run_freqency}' 
   and ctry_code in
		(select ctry_code from ${ODSSchema}.biz_calendar 
			where eff_dt =to_date('${business_date}','DDMMYYYY') and is_holiday='Y' and ctry_code = '${ctry_code}');

   commit;
   quit;
here
}

function upd_cash_high_hol
{
ctry_code=$1
business_date=$2
run_freqency=$3

sqlplus -s <<-here 2>&1
${ConnStr}

update ${ODSSchema}.ods_ctrlm_jobs 
	set run_status = 'S', file_avail_status = 'S' 
 where job_name in (select job_name 
	from ${ODSSchema}.ods_stmt_jobs b
		where ctry_code = '${ctry_code}'
	      and (stmt_name like '%CASH%' or stmt_name like '%YIELD%')) 
			and ctry_code = '${ctry_code}'
				and frequency = '${run_freqency}'
					and business_date != (select FIRSTWORKINGDATE from ${ODSSchema}.holiday_date_param where ccy_code='${ctry_code}');

   commit;
   quit;
here
}

function upd_biu_hol
{
ctry_code=$1
business_date=$2
run_freqency=$3

sqlplus -s <<-here 2>&1
${ConnStr}

update ${ODSSchema}.ods_ctrlm_jobs 
	set run_status = 'S', file_avail_status = 'S' 
 where job_name in (select job_name 
	from ${ODSSchema}.ods_stmt_jobs b
		where ctry_code = '${ctry_code}'
	      and (stmt_name like '%BIU%')) 
			and ctry_code = '${ctry_code}'
				and frequency = '${run_freqency}'
					and business_date! = (select biubatchdate from ${ODSSchema}.holiday_date_param where ccy_code='${ctry_code}');
					

   commit;

   quit;
here
}


function upd_bonus_hol
{
ctry_code=$1
business_date=$2
run_freqency=$3

sqlplus -s <<-here 2>&1
${ConnStr}

update ${ODSSchema}.ods_ctrlm_jobs 
	set run_status = 'S', file_avail_status = 'S' 
 where job_name in (select job_name 
	from ${ODSSchema}.ods_stmt_jobs b
		where ctry_code = '${ctry_code}'
	      and (stmt_name like '%BONUS_ESAVER%')) 
			and ctry_code = '${ctry_code}'
				and frequency = '${run_freqency}'
					and business_date != (select lastworkingdate_curmonth from ${ODSSchema}.holiday_date_param where ccy_code='${ctry_code}');

  commit;
   quit;
here
}

function upd_cda_hol
{
ctry_code=$1
business_date=$2
run_freqency=$3

sqlplus -s <<-here 2>&1
${ConnStr}

update ${ODSSchema}.ods_ctrlm_jobs 
	set run_status = 'S', file_avail_status = 'S' 
 where job_name in (select job_name 
	from ${ODSSchema}.ods_stmt_jobs b
		where ctry_code = '${ctry_code}'
	      and (stmt_name like '%CDA%')) 
			and ctry_code = '${ctry_code}'
				and frequency = '${run_freqency}'
					and business_date != (select lastworkingdate_curmonth from ${ODSSchema}.holiday_date_param where ccy_code='${ctry_code}');

  commit;
   quit;
here
}

function upd_esaver_hol
{
ctry_code=$1
business_date=$2
run_freqency=$3

sqlplus -s <<-here 2>&1
${ConnStr}

update ${ODSSchema}.ods_ctrlm_jobs 
	set run_status = 'S', file_avail_status = 'S' 
 where job_name in (select job_name 
	from ${ODSSchema}.ods_stmt_jobs b
		where ctry_code = '${ctry_code}'
	      and (stmt_name like '%EBBS_ESAVER%')) 
			and ctry_code = '${ctry_code}'
				and frequency = '${run_freqency}'
					and business_date != (select lastworkingdate_curmonth from ${ODSSchema}.holiday_date_param where ccy_code='${ctry_code}');

  commit;
   quit;
here
}

function upd_tda_hol
{
ctry_code=$1
business_date=$2
run_freqency=$3

sqlplus -s <<-here 2>&1
${ConnStr}

/*
begin
	ods_holiday_pkg.proc_tdaworkingdate('${ctry_code}','${dly_bcp_date2}');
end;
/
  quit;

*/
here
}



function upd_mm_hol
{
ctry_code=$1
business_date=$2
run_freqency=$3

sqlplus -s <<-here 2>&1
${ConnStr}

update ${ODSSchema}.ods_ctrlm_jobs
   set run_status = 'N', file_avail_status = 'N'
	where job_name in (select job_name 
                      from ${ODSSchema}.ods_stmt_jobs b
                     where ctry_code = '${ctry_code}'
                       and stmt_name like '%MM%')  
  and ctry_code = '${ctry_code}'
  and frequency = '${run_freqency}'
  and business_date = (
	SELECT CASE
          WHEN    (    TO_CHAR (c.eff_dt, 'DD') = '02'
                   AND c.is_holiday = 'N'
                   AND TO_CHAR (p1.eff_dt, 'DD') = '01'
                   AND p1.is_holiday = 'N'
                   AND TO_CHAR (n1.eff_dt, 'DD') = '03'
                   AND n1.is_holiday = 'Y')
	       OR  (    TO_CHAR (c.eff_dt, 'DD') = '02'
                   AND c.is_holiday = 'N'
                   AND TO_CHAR (p1.eff_dt, 'DD') = '01'
                   AND p1.is_holiday = 'Y'
                   AND TO_CHAR (n1.eff_dt, 'DD') = '03'
                   AND n1.is_holiday = 'Y')
               OR (    TO_CHAR (c.eff_dt, 'DD') = '01'
                   AND c.is_holiday = 'N'
                   AND TO_CHAR (n1.eff_dt, 'DD') = '02'
                   AND n1.is_holiday = 'Y'
                   AND TO_CHAR (n2.eff_dt, 'DD') = '03'
                   AND n2.is_holiday = 'Y')
               OR (    TO_CHAR (c.eff_dt, 'DD') =
                          TO_CHAR (LAST_DAY (c.eff_dt), 'DD')
                   AND TO_CHAR (n1.eff_dt, 'DD') = '01'
                   AND n1.is_holiday = 'Y'
                   AND TO_CHAR (n2.eff_dt, 'DD') = '02'
                   AND n2.is_holiday = 'Y'
                   AND TO_CHAR (n3.eff_dt, 'DD') = '03'
                   AND n3.is_holiday = 'Y'
                   AND TO_CHAR (n4.eff_dt, 'DD') = '04'
                   AND n4.is_holiday = 'N')
          THEN
             c.eff_dt
       END
          c
  FROM (SELECT *
          FROM ${ODSSchema}.biz_calendar
         WHERE cty_code = '${ctry_code}' AND eff_dt = to_date('${business_date}','DDMMYYYY')) c,
       (SELECT *
          FROM ${ODSSchema}.biz_calendar
         WHERE     cty_code = '${ctry_code}'
               AND eff_dt = to_date('${business_date}','DDMMYYYY') + 1) n1,
       (SELECT *
          FROM ${ODSSchema}.biz_calendar
         WHERE     cty_code = '${ctry_code}'
               AND eff_dt = to_date('${business_date}','DDMMYYYY') + 2) n2,
       (SELECT *
          FROM ${ODSSchema}.biz_calendar
         WHERE     cty_code = '${ctry_code}'
               AND eff_dt = to_date('${business_date}','DDMMYYYY') + 3) n3,
       (SELECT *
          FROM ${ODSSchema}.biz_calendar
         WHERE     cty_code = '${ctry_code}'
               AND eff_dt = to_date('${business_date}','DDMMYYYY') + 4) n4,
       (SELECT *
          FROM ${ODSSchema}.biz_calendar
         WHERE     cty_code = '${ctry_code}'
               AND eff_dt = to_date('${business_date}','DDMMYYYY') - 1) p1
WHERE     c.cty_code = n1.cty_code
       AND n1.cty_code = n2.cty_code
       AND n2.cty_code = n3.cty_code
       AND n3.cty_code = n4.cty_code
       AND n4.cty_code = p1.cty_code);


   commit;

	update ${ODSSchema}.ods_ctrlm_jobs
   		set run_status = 'N', file_avail_status = 'N'
   	         where job_name in (select job_name 
                      from ${ODSSchema}.ods_stmt_jobs b
                     where ctry_code = '${ctry_code}'
                       and stmt_name like '%MM%')  
  and ctry_code = '${ctry_code}'
  and frequency = '${run_freqency}'
  and business_date = (select max(eff_dt) from ${ODSSchema}.biz_calendar where eff_dt >= to_date('${business_date}','DDMMYYYY') 
 		and  eff_dt <= to_char((select lastworkingdate_curmonth from ${ODSSchema}.holiday_date_param where ccy_code='${ctry_code}') )
   and is_holiday='N' and cty_code='${ctry_code}')
  and exists  (select 'Y'  from ${ODSSchema}.biz_calendar Where cty_code='${ctry_code}' and eff_dt=last_day(to_date('${business_date}','DDMMYYYY'))+1 and is_holiday='Y')
  and exists  (select 'Y'  from ${ODSSchema}.biz_calendar Where cty_code='${ctry_code}' and eff_dt=last_day(to_date('${business_date}','DDMMYYYY'))+2 and is_holiday='Y')
  and exists  (select 'Y'  from ${ODSSchema}.biz_calendar Where cty_code='${ctry_code}' and eff_dt=last_day(to_date('${business_date}','DDMMYYYY'))+3 and is_holiday='Y');
  
  COMMIT;

   quit;
here
}

function upd_system_hol
{
ctry_code=$1
business_date=$2

sqlplus -s <<-here 2>&1
${ConnStr}

update ${ODSSchema}.ods_ctrlm_jobs
   set run_status = 'S', file_avail_status = 'S'
 where ctry_code = '${ctry_code}'
   and src_sys     in (select system_name 
                         from ${ODSSchema}.ods_holiday_calendar
                        where ctry_code = '${ctry_code}'
                          and business_date = to_date('${business_date}','DDMMYYYY')
                       union
                       select system_name 
                         from ${ODSSchema}.ods_src_system
                        where ctry_code = '${ctry_code}'
                          and instr(upper(holiday_desc),upper(trim(to_char(TO_DATE('${business_date}','DDMMYYYY'), 'Day')))) > 0);

   commit;
   quit;
here
}

function get_src_tgt_mapping
{
ctry_code=$1
system_code=$2
sub_system_code=$3
process_code=$4
frequency=$5
business_date=$6

sqlplus -s <<-here 2>&1
${ConnStr}
set heading off;
set linesize 1000;

select 'cp '||src_file||' '||tgt_file from
(select source_path||source_file_name src_file, system_name, sub_system_name
 from ${ODSSchema}.ods_ctrlm_jobs
where system_name = '${system_code}'
and sub_system_name = '${sub_system_code}'
and process_name = 'STAGING_PT') src,
(select target_path||target_file_name tgt_file, system_name, sub_system_name
from ${ODSSchema}.ods_target_files
where system_name = '${system_code}'
and sub_system_name = '${sub_system_code}'
and process_name = 'ISIS') tgt
where src.system_name = tgt.system_name
and   src.sub_system_name = tgt.sub_system_name;

   quit;
here
}

function update_job_start_time
{
ctry_code=$1
system_code=$2
process_code=$3
job_name=$4
ConnStrl=$5

sqlplus -s <<-here 2>&1
${ConnStr1}

update ${ODSSchema}.ods_ctrlm_jobs set start_time = sysdate
         where ctry_code            = '${ctry_code}'
           and system_name          = '${system_code}'
           and process_name         = '${process_code}'
           and job_name             = '${job_name}';

   commit;
   quit;
here
}

function update_job_end_time
{
ctry_code=$1
system_code=$2
process_code=$3
job_name=$4
ConnStr1=$5

sqlplus -s <<-here 2>&1
${ConnStr1}

update ${ODSSchema}.ods_ctrlm_jobs set end_time = sysdate
         where ctry_code            = '${ctry_code}'
           and system_name          = '${system_code}'
           and process_name         = '${process_code}'
           and job_name             = '${job_name}';

   commit;
   quit;
here
}

function get_regen_job_count
{
Regen_id=$1
Regen_status=$2

sqlplus -s <<-here 2>&1
${ConnStr}
set heading off;

select count(1) from ${ODSSchema}.ods_regen_jobs
         where regen_id = '${Regen_id}';

   quit;
here
}

function get_regen_job_dtls
{
regen_code=$1
regen_seq=$2

sqlplus -s <<-here 2>&1
${ConnStr}
set heading off;
set linesize 1000;

select regen_job_dtls from (
select a.job_name||'|'||a.project_name||'|'||a.ds_seq_name||'|'||a.ctry_code||'|'||a.system_name||'|'||a.process_name||'|'||to_char(a.business_date,'DDMMYYYY')||'|'||run_status regen_job_dtls, run_sequence 
  from ${ODSSchema}.ods_regen_jobs a
 where a.regen_id = '${regen_code}'
   and a.run_sequence = ${regen_seq});

   quit;
here
}

function ods_regen_chk
{
Regen_job_name=$1
Regen_seq_name=$2

sqlplus -s <<-here 2>&1
${ConnStr}
set heading off;

select count(1) from ${ODSSchema}.ods_ctrlm_jobs
         where job_name = '${Regen_job_name}'
           and ds_seq_name = '${Regen_seq_name}';

   quit;
here
}

function update_regen_status
{
ctry_code=$1
system_code=$2
process_code=$3
business_date=$4
job_name=$5
status=$6
regen_seq=$7
regen_id=$8

sqlplus -s <<-here 2>&1
${ConnStr}

update ${ODSSchema}.ods_regen_jobs set run_status = '${status}'
         where ctry_code            = '${ctry_code}'
           and system_name          = '${system_code}'
           and process_name         = '${process_code}'
           and job_name             = '${job_name}'
           and trunc(business_date) = to_date('$business_date','DDMMYYYY')
           and run_sequence         = '${regen_seq}'
           and regen_id             = '${regen_id}';

   commit;
   quit;
here
}

function update_regen_start_time
{
ctry_code=$1
system_code=$2
process_code=$3
job_name=$4
regen_seq=$5
regen_id=$6

sqlplus -s <<-here 2>&1
${ConnStr}

update ${ODSSchema}.ods_regen_jobs set start_time = sysdate
         where ctry_code            = '${ctry_code}'
           and system_name          = '${system_code}'
           and process_name         = '${process_code}'
           and job_name             = '${job_name}'
           and run_sequence         = '${regen_seq}'
           and regen_id             = '${regen_id}';

   commit;
   quit;
here
}

function update_regen_end_time
{
ctry_code=$1
system_code=$2
process_code=$3
job_name=$4
regen_seq=$5
regen_id=$6

sqlplus -s <<-here 2>&1
${ConnStr}

update ${ODSSchema}.ods_regen_jobs set end_time = sysdate
         where ctry_code            = '${ctry_code}'
           and system_name          = '${system_code}'
           and process_name         = '${process_code}'
           and job_name             = '${job_name}'
           and run_sequence         = '${regen_seq}'
           and regen_id             = '${regen_id}';

   commit;
   quit;
here
}

function reset_regen_status
{
RegenID=$1
RegenStatus=$2

sqlplus -s <<-here 2>&1
${ConnStr}

update ${ODSSchema}.ods_regen_jobs set run_status = '${RegenStatus}'
         where REGEN_ID = '${RegenID}';

   commit;
   quit;
here
}

function get_src_files
{
ctry_code=$1
freq=$2
biz_date=$3
src_path=$4

sqlplus -s <<-here 2>&1
${ConnStr}
set heading off;
set feedback off;

select distinct source_file_name
       from ${ODSSchema}.ods_ctrlm_jobs
      where ctry_code            = '${ctry_code}'
        and frequency            = '${freq}'
        and trunc(business_date) = to_date('$biz_date','DDMMYYYY')
        and source_path = '${src_path}';

   quit;
here
}

function get_tgt_files
{
ctry_code=$1
freq=$2
biz_date=$3
tgt_path=$4

sqlplus -s <<-here 2>&1
${ConnStr}
set heading off;
set feedback off;

select distinct target_file_name
       from ${ODSSchema}.ods_target_files
      where ctry_code            = '${ctry_code}'
        and frequency            = '${freq}'
        and trunc(business_date) = to_date('$biz_date','DDMMYYYY')
        and target_path = '${tgt_path}';

   quit;
here
}

function get_failed_job_details
{
ctry_code=$1
system_code=$2
process_code=$3
biz_date=$4
freq=$5
ConnStrl=$6

sqlplus -s <<-here 2>&1
${ConnStr1}
set heading off;
set linesize 1000;

select job_name||'-->'||ds_seq_name||'-->'||project_name
       from ${ODSSchema}.ods_ctrlm_jobs
      where ctry_code            = '${ctry_code}'
        and system_name          = '${system_code}'
        and process_name         = '${process_code}'
        and trunc(business_date) = to_date('$biz_date','DDMMYYYY')
        and frequency            = '${freq}'
        and run_status           = 'F';

   quit;
here
}

function update_ftf_time
{
ctry_code=$1
sys_name=$2
sub_sys_name=$3
process_name=$4
freq=$5
ConnStrl=$6

sqlplus -s <<-here 2>&1
${ConnStr1}

update ${ODSSchema}.ods_target_files set ftf_start_time = sysdate
      where ctry_code            = '${ctry_code}'
        and system_name          = '${sys_name}'
        and sub_system_name      = '${sub_sys_name}'
        and process_name         = '${process_name}'
        and frequency            = '${freq}'
        and isis_status = 'N';

   commit;
   quit;
here
}

function get_ftf_time_limit
{
ctry_code=$1
sys_name=$2
sub_sys_name=$3
process_name=$4
ftf_id=$5

sqlplus -s <<-here 2>&1
${ConnStr}
set heading off;

select case when floor((sysdate-ftf_start_time)*24*60) > ftf_time_limit 
             then 'Y' 
        else 'N' 
        end ftf_time_difference
       from ${ODSSchema}.ods_target_files
      where ctry_code            = '${ctry_code}'
        and system_name          = '${sys_name}'
        and sub_system_name      = '${sub_sys_name}'
        and process_name         = '${process_name}'
        and ftf_id               = '${ftf_id}';

   quit;
here
}

function get_job_retry_count
{
ctry_code=$1
system_code=$2
process_code=$3
business_date=$4
ctm_job_name=$5
ConnStrl=$6

sqlplus -s <<-here 2>&1
${ConnStr1}
set heading off;

select nvl(retry_count,0) from ${ODSSchema}.ods_ctrlm_jobs
         where ctry_code            = '${ctry_code}'
           and system_name          = '${system_code}'
           and process_name         = '${process_code}'
           and trunc(business_date) = to_date('$business_date','DDMMYYYY')
           and job_name             = '${ctm_job_name}';

   quit;
here
}

function update_job_retry_count
{
ctry_code=$1
system_code=$2
process_code=$3
business_date=$4
ctm_job_name=$5
ConnStrl=$6

sqlplus -s <<-here 2>&1
${ConnStr1}

update ${ODSSchema}.ods_ctrlm_jobs set retry_count = nvl(retry_count,0)+1
         where ctry_code            = '${ctry_code}'
           and system_name          = '${system_code}'
           and process_name         = '${process_code}'
           and trunc(business_date) = to_date('$business_date','DDMMYYYY')
           and job_name             = '${ctm_job_name}';

   commit;
   quit;
here
}

function update_job_stat_proces_count
{
ctry_code=$1
echo "deleting for the country $ctry_code"
sqlplus -s <<-here 2>&1
${ConnStr}

update ${ODSSchema}.ods_job_limit set job_run_count = 0, start_time=null
         where ctry_code            = '${ctry_code}';

 commit;

   quit;
here
}

function upd_target_files_status_new
{
ctry_code=$1
sys_code=$2
sub_sys_code=$3
proc_code=$4
freq=$5
status=$6

sqlplus -s <<-here 2>&1
${ConnStr}


update ${ODSSchema}.ods_target_files set isis_status = '${status}',ftf_id = null, fail_description = null
       where (ctry_code,system_name,frequency,target_file_name) IN (select 
       ctry_code,system_name,frequency,source_file_name from ${ODSSchema}.ods_ctrlm_jobs
       where ctry_code            = '${ctry_code}'
         and system_name          = '${sys_code}'
         and process_name         = 'BULK_LOAD'
         and frequency            = '${freq}'
         and run_status           ='Y') 
         and isis_status is null;
   commit;
   quit;


here
}

function update_sub_job_status_new
{
ctry_code=$1
system_code=$2
sub_system_code=$3
process_code=$4
business_date=$5
status=$6

sqlplus -s <<-here 2>&1
${ConnStr}

update ${ODSSchema}.ods_ctrlm_jobs set run_status = '${status}'
         where ctry_code            = '${ctry_code}'
           and system_name          = '${system_code}'
           and sub_system_name      = '${sub_system_code}'
           and process_name         = '${process_code}';

   commit;
   quit;
here
}

function get_ack_files
{
ctry_code=$1
freq=$2
biz_date=$3
src_path=$4

sqlplus -s <<-here 2>&1
${ConnStr}
set heading off;
set feedback off;

select distinct source_path||ack_file_name
       from ${ODSSchema}.ods_ctrlm_jobs
      where ctry_code            = '${ctry_code}'
        and frequency            = '${freq}'
        and trunc(business_date) = to_date('$biz_date','DDMMYYYY')
        and source_path = '${src_path}'
        and process_name='STAGING';

   quit;
here
}

function gather_stats
{
ctry_code=$1

result=`sqlplus -s <<here 2>&1
${ConnStr}
set serveroutput on size 10000;
set head off;

declare
a varchar(200);
begin
a:=${ODSSchemaMain}.FN_MEPA_GATHER_TABLE_STATS ( '${ctry_code}');
COMMIT;
dbms_output.put_line(a);
end;
/
exit;
here`
echo "$result"
return "${result}"
}

function recon_table_load
{
ctry_code=$1
batch_date=$2

result=`sqlplus -s <<here 2>&1
${ConnStr}
set serveroutput on size 10000;
set head off;

declare
a varchar(200);
begin
a:=${ODSSchemaTemp}.FN_MEPA_RECON  ( '${ctry_code}','${batch_date}');
COMMIT;
dbms_output.put_line(a);
end;
/
exit;
here`
echo "$result"
return "${result}"
}


function chk_proc_status
{
ctry_code=$1
proc_name=$2
batch_date=$3


sqlplus -s <<-here 2>&1
${ConnStr}
set heading off;
set feedback off;

SELECT STATUS 
FROM ${ODSSchemaTemp}.PROC_STATUS
      where country          = '${ctry_code}'
        and proc_name          = '${proc_name}'
        and trunc(batch_date) = to_char(to_date('$batch_date','DDMMYYYY'),'YYYYMMDD');

   quit;
here
}

function get_source_file
{
ctry_code=$1
job_name=$2
ConnStrl=$3

sqlplus -s <<-here 2>&1
${ConnStr1}
set heading off;
set feedback off;

select source_path||source_file_name
FROM ${ODSSchema}.ods_ctrlm_jobs
      where ctry_code        = '${ctry_code}'
        and job_name          = '${job_name}';

   quit;
here
}



function upd_file_count
{
ctry_code=$1
job_name=$2
count=$3
ConnStrl=$4


sqlplus -s <<-here 2>&1
${ConnStr1}

update ${ODSSchema}.ods_ctrlm_jobs set file_count=${count}
         where ctry_code            = '${ctry_code}'
           and job_name         = '${job_name}';
   commit;
   quit;
here
}

function get_isis_job_list
{
ctry_code=$1
system_code=$2
process_code=$3
frequency=$4
business_date=$5
ConnStr1=$6

sqlplus -s <<-here 2>&1
${ConnStr1}
set heading off;
set linesize 1000;

select job_dtls from (
SELECT b.job_name||'|'||b.project_name||'|'||b.sub_system_name job_dtls, b.priority
           FROM ${ODSSchema}.ods_ctrlm_jobs_dependency a,
                (SELECT job_name, ds_seq_name, project_name, nvl(priority,10) priority, sub_system_name
                   FROM ${ODSSchema}.ods_ctrlm_jobs
                  WHERE ctry_code = '${ctry_code}'
                    AND system_name = '${system_code}'
                    AND process_name = '${process_code}'
                    AND TRUNC (business_date) = TO_DATE ('$business_date', 'DDMMYYYY')
                    AND frequency = '${frequency}'
                    AND run_status = 'N') b
          WHERE a.in_cond IN (SELECT job_name
                                FROM ${ODSSchema}.ods_ctrlm_jobs
                               WHERE run_status IN ('Y', 'S'))
            AND a.job_name NOT IN (
                   SELECT job_name
                     FROM ${ODSSchema}.ods_ctrlm_jobs_dependency
                    WHERE in_cond IN (
                                 SELECT job_name
                                   FROM ${ODSSchema}.ods_ctrlm_jobs
                                  WHERE run_status IN ('N', 'F', 'M', 'H', 'P')))
            AND a.job_name = b.job_name
order by b.priority)
          WHERE rownum < 2;

   quit;
here
}

function gather_stats_stage
{
ctry_code=$1

result=`sqlplus -s <<here 2>&1
${ConnStr}
set serveroutput on size 10000;
set head off;

declare
a varchar(200);
begin
a:=${ODSSchema}.FN_MEPASTG_GATHER_TABLE_STATS ( '${ctry_code}');
COMMIT;
dbms_output.put_line(a);
end;
/
exit;
here`
echo "$result"
return "${result}"
}

function gather_stats_temp
{
ctry_code=$1

result=`sqlplus -s <<here 2>&1
${ConnStr}
set serveroutput on size 10000;
set head off;

declare
a varchar(200);
begin
a:=${ODSSchemaTemp}.FN_MEPATEMP_GATHER_TABLE_STATS ( '${ctry_code}');
COMMIT;
dbms_output.put_line(a);
end;
/
exit;
here`
echo "$result"
return "${result}"
}
