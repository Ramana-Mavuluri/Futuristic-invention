#!/bin/ksh
#---------------------------------------------------------------------------------------------------
# Scripts     : ods_sod.ksh
# Description : Common script to call ODS SOD
# Usage       : ODS_SOD.ksh <Country Code> <Frequency>
# Return Stat :
#               exit 1 - ended ok
#               exit 5 = ended not ok
# Revision History :
# S.no          Date                    Revised by                              Description of Change
#
#---------------------------------------------------------------------------------------------------

ctry_code=$1
run_freq=$2

#------Sourcing Config file ----------#
#USERID=`whoami`
#MEPA_CONFIG_PATH=`getent passwd ${USERID} | cut -d: -f6`
. ${ODS_ConfigFolder}/MEPA_ODSConfig.dat

dspath=`cat /.dshome`/bin

. ${ODSCtrlMJobsFolder}/ods_setenv.ksh
. ${ODSCtrlMJobsFolder}/ods_ora.ksh

log_name=${ctry_code}_SOD_${run_freq}

LogMessage3 $log_name " [ info ] Country Code  : ${ctry_code}"
LogMessage3 $log_name " [ info ] Run Frequency : ${run_freq}"
LogMessage3 $log_name " [ info ] Log Name      : ${log_name}"

# ----------------------------------------------------------
# Fetch Previous Business Date
# ----------------------------------------------------------
dly_bcp_date2=`get_ods_bizdt "${ctry_code}" "${run_freq}" "DDMMYYYY"`

chk_ora_error "${dly_bcp_date2}" "2" > /dev/null 2>&1

if [ $? -ne 0 ];then
   LogMessage3 $log_name " [ error ] Failed during fetch prev biz date"
   LogMessage3 $log_name " [ error ] [$dly_bcp_date2]"
   exit 5
else
   dly_bcp_date=`echo  "${dly_bcp_date2}" |awk 'NR==2 {print $1}'`
fi

LogMessage3 $log_name " [ info ] Fetch Previous Biz Date Success : ${dly_bcp_date}"

echo ${dly_bcp_date} > ${ODSCtrlMLogsFolder}/${ctry_code}_SOD_BizDate.old
old_bzdt=${dly_bcp_date}

# ----------------------------------------------------------
# Rollout New Business Date
# ----------------------------------------------------------
dly_bcp_date=
ora_err_flag=`roll_out_biz_date "${ctry_code}" "${run_freq}"`
chk_ora_error "${ora_err_flag}" "0" > /dev/null 2>&1

if [ $? -ne 0 ];then
   LogMessage3 $log_name " [ error ] Failed to rollout the business date"
   LogMessage3 $log_name " [ error ] [$ora_err_flag]"
   exit 5
else
# ----------------------------------------------------------
# Fetch Curr Business Date DDMMYYYY
# ----------------------------------------------------------
   dly_bcp_date2=`get_ods_bizdt "${ctry_code}" "${run_freq}" "DDMMYYYY"`

   chk_ora_error "${dly_bcp_date2}" "2" > /dev/null 2>&1

   if [ $? -ne 0 ];then
      LogMessage3 $log_name " [ error ] Failed during fetch curr biz date DDMMYYYY"
      LogMessage3 $log_name " [ error ] [$dly_bcp_date2]"
      exit 5
   else
      dly_bcp_date=`echo  "${dly_bcp_date2}" |awk 'NR==2 {print $1}'`
   fi

# ----------------------------------------------------------
# Fetch Curr Business Date YYYYMMDD
# ----------------------------------------------------------
   dly_bcp_date2=`get_ods_bizdt "${ctry_code}" "${run_freq}" "YYYYMMDD"`

   chk_ora_error "${dly_bcp_date2}" "2" > /dev/null 2>&1

   if [ $? -ne 0 ];then
      LogMessage3 $log_name " [ error ] Failed during fetch curr biz date YYYYMMDD"
      LogMessage3 $log_name " [ error ] [$dly_bcp_date2]"
      exit 5
   else
      dly_bcp_date1=`echo  "${dly_bcp_date2}" |awk 'NR==2 {print $1}'`
   fi

   echo ${dly_bcp_date} > ${ODSCtrlMLogsFolder}/${ctry_code}_SOD_BizDate.txt
fi

LogMessage3 $log_name " [ info ] Roll out New Biz Date Success : ${dly_bcp_date}"


# ----------------------------------------------------------
# Copy to History Control Table
# ----------------------------------------------------------
ora_err_flag=`ins_ctrl_tbl_hist "${ctry_code}" "${run_freq}"`
chk_ora_error "${ora_err_flag}" "0" > /dev/null 2>&1

if [ $? -ne 0 ];then
   LogMessage3 $log_name " [ error ] Failed to insert into Control table history."
   LogMessage3 $log_name " [ error ] [$ora_err_flag]"
   exit 5
fi

LogMessage3 $log_name " [ info ] Insert Src Control Table history Success"



# ----------------------------------------------------------
# Copy to History Target File Table
# ----------------------------------------------------------
ora_err_flag=`ins_tgt_tbl_hist "${ctry_code}" "${run_freq}"`
chk_ora_error "${ora_err_flag}" "0" > /dev/null 2>&1

if [ $? -ne 0 ];then
   LogMessage3 $log_name " [ error ] Failed to insert into Control table history."
   LogMessage3 $log_name " [ error ] [$ora_err_flag]"
   exit 5
fi

LogMessage3 $log_name " [ info ] Insert Src Control Table history Success"



# ----------------------------------------------------------
# Initialize RUN_STATUS
# ----------------------------------------------------------
ora_err_flag=`init_ctrl_tbl "${ctry_code}" "${run_freq}" "N" ${dly_bcp_date}`
chk_ora_error "${ora_err_flag}" "0" > /dev/null 2>&1

if [ $? -ne 0 ];then
   LogMessage3 $log_name " [ error ] Failed to initialize Control table."
   LogMessage3 $log_name " [ error ] [$ora_err_flag]"
   exit 5
fi

LogMessage3 $log_name " [ info ] Initialize Src Control Table Success"

# ----------------------------------------------------------
# Initialize FILE_STATUS
# ----------------------------------------------------------
ora_err_flag=`upd_file_status "${ctry_code}" "${run_freq}" "N"`
chk_ora_error "${ora_err_flag}" "0" > /dev/null 2>&1

if [ $? -ne 0 ];then
   LogMessage3 $log_name " [ error ] Failed to initialize FILE_AVAIL_STATUS in Control table."
   LogMessage3 $log_name " [ error ] [$ora_err_flag]"
   exit 5
fi

LogMessage3 $log_name " [ info ] Update File Available Status Success"

# ----------------------------------------------------------
# Initialize TGT_TABLE
# ----------------------------------------------------------
ora_err_flag=`init_tgt_tbl "${ctry_code}" "${run_freq}" "N" ${dly_bcp_date}`
chk_ora_error "${ora_err_flag}" "0" > /dev/null 2>&1

if [ $? -ne 0 ];then
   LogMessage3 $log_name " [ error ] Failed to initialize Target table."
   LogMessage3 $log_name " [ error ] [$ora_err_flag]"
   exit 5
fi

LogMessage3 $log_name " [ info ] Initialize Tgt Control Table Success"


# ----------------------------------------------------------
# Initialize ISIS_STATUS
# ----------------------------------------------------------
ora_err_flag=`init_isis_status "${ctry_code}" "${run_freq}" "N" ${dly_bcp_date}`
chk_ora_error "${ora_err_flag}" "0" > /dev/null 2>&1

if [ $? -ne 0 ];then
   LogMessage3 $log_name " [ error ] Failed to initialize ISIS_STATUS in Control table."
   LogMessage3 $log_name " [ error ] [$ora_err_flag]"
   exit 5
fi

LogMessage3 $log_name " [ info ] Initialize ISIS Status Success"

# ----------------------------------------------------------
# Initialize PARAM_TABLE
# ----------------------------------------------------------
ora_err_flag=`init_params_table ${ctry_code} ${dly_bcp_date1}`
chk_ora_error "${ora_err_flag}" "0" > /dev/null 2>&1
 
if [ $? -ne 0 ];then
   LogMessage3 $log_name " [ error ] Failed to initialize ODS_CTRLM_JOBS_PARAMS table."
   LogMessage3 $log_name " [ error ] [$ora_err_flag]"
   exit 5
fi

LogMessage3 $log_name " [ info ] Initialize Param Control Table Success"

# ----------------------------------------------------------
# Update Statement Holiday 
# ----------------------------------------------------------
ora_err_flag=`upd_stmt_hol "${ctry_code}" "${dly_bcp_date}"`
chk_ora_error "${ora_err_flag}" "0" > /dev/null 2>&1

if [ $? -ne 0 ];then
   LogMessage3 $log_name " [ error ] Failed to update Statement Holiday..."
   LogMessage3 $log_name " [ error ] [$ora_err_flag]"
   exit 5
fi

LogMessage3 $log_name " [ info ] Update Statement Holiday Success"

#----------------------------------------------------------
# Update System Holiday 
# ----------------------------------------------------------
ora_err_flag=`upd_system_hol "${ctry_code}" "${dly_bcp_date}"`
chk_ora_error "${ora_err_flag}" "0" > /dev/null 2>&1
 
if [ $? -ne 0 ];then
  LogMessage3 $log_name " [ error ] Failed to update System Holiday..."
   LogMessage3 $log_name " [ error ] [$ora_err_flag]"
  exit 5
fi

LogMessage3 $log_name " [ info ] Update System Holiday Success"

# ----------------------------------------------------------
# Update limit Status to 0
# ----------------------------------------------------------
ora_err_flag=`update_job_stat_proces_count ${ctry_code}`
chk_ora_error "${ora_err_flag}" "0" > /dev/null 2>&1

if [ $? -ne 0 ];then
   LogMessage3 $log_name " [ error ] while updating limit status 0"
   LogMessage3 $log_name " [ error ] [$ora_err_flag]"
   exit 5
fi

LogMessage3 $log_name " [ info ] Update limit status Success"

LogMessage3 $log_name " [ info ] SOD Job Completed for Country [${ctry_code}]"

exit 1
