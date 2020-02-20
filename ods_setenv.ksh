
#!/bin/ksh

#------Sourcing Config file ----------#
#USERID=`whoami`
#MEPA_CONFIG_PATH=`getent passwd ${USERID} | cut -d: -f6`
. ${ODS_ConfigFolder}/MEPA_ODSConfig.dat

#---Path & Variables
sysdate=`date +%Y%m%d`
Owner=`whoami`
dspath=`cat /.dshome`/bin

LogMessage1()
{
  rundate=`date +%Y%m%d`
  msg_txt="$1"
  echo "[${rundate}] - ${msg_txt}"
}

LogMessage2()
{
  rundate=`date +%Y%m%d`
  LogFile2=${ODSCtrlMLogsFolder}/"$1"_${rundate}.log_$$
  msg_txt="$2"
  echo "${msg_txt}" >> ${LogFile2}
}

LogMessage3()
{
  rundate=`date +%Y%m%d`
  LogFile3=${ODSCtrlMLogsFolder}/"$1"_${rundate}.log_$$
  msg_txt="$2"
  echo "${msg_txt}"
  echo "${msg_txt}" >> ${LogFile3}
}

FileWatch()
{
	LogMessage1  " [ info ] Filewatcher is checking for the presence of file $1"

	LogMessage1  " [ info ] Total time limit for the file checking is 240 minutes"

	#/opt/controlm/ctmagent/ctm/exe/ctmfw "$1" CREATE 0 30 10 5 5 Y > /dev/null 2>&1
        #Remove the control-m agent directory 

        ${CTMFolder}/ctmfw  "$1" CREATE 0 60 10 3 1 > /dev/null 2>&1

        if [ $? != 0 ]
        then
                cnt=0
		#echo $cnt
                while [ $cnt -le 48 ]
                do 
                     if [ -f "$1" ] && [ `du -m $1 | cut -f1` -gt 2000 ]
                     then
                        LogMessage1 " [ info ] Detected file size more than 2 Gig. Using script method to check file growth.."
                        iteration=0
                        initialsize=`du -k $1 | cut -f1`
                        while [ ${iteration} -le 10 ]
                        do
                           newsize=`du -k $1 | cut -f1`
                           if [ ${newsize} -eq ${initialsize} ]
                           then
                              iteration=`expr $iteration + 1`
                              LogMessage1 " [ info ] iteration : $iteration"
                           else
                              initialsize=$newsize   
                              iteration=0
                              LogMessage1 " [ info ] New File Size : $initialsize"
                           fi
                           sleep 10
                        done 
                        LogMessage1 " [ info ] Filesize remain at $newsize for 10 iterations ... Filewatcher completed.."
                        return
                     fi 
                     ${CTMFolder}/ctmfw  "$1" CREATE 0 60 10 3 10  > /dev/null 2>&1
                     if [ $? != 0 ] 
                     then
			 #echo $cnt
                         cnt=`expr $cnt + 1`
                     else
                        LogMessage1 "File Check successful for  $1"
                        file_exist=0
                        return 
                     fi
                done
	       	if [ ! -f "$1" ]
        	then
                	LogMessage1 " [ error ] while finding file $1..ERROR"
                	file_exist=1
                	return
                fi
	fi
        LogMessage1 " [ info ] File Check successful for $1"
        file_exist=0
}

DSJobStat1()
{
#-check for status of the job-#

$dspath/dsjob -jobinfo $1 $2 > /dev/null 2>&1

if ! [ "$?" = "0" ] 
then
   jobStat=1
   LogMessage3 ${3} "[ error ] : Error in connecting to project $1 - `date`"
   return
else
   jobStat=0
   return   
fi
}

DSJobStat2()
{
 dsreset=1
 dsreset_Stat=1
 dsstat=`$dspath/dsjob -jobinfo $1 $2  | grep "Job Status" | awk '{FS=":"} {print $2}' | awk '{FS="("} {print $2}' | awk '{FS=")"} {print $1}'`

if [ "$dsstat" = "" ];then
   echo "Error checking the status of the job. " 
   dsreset_Stat=5
   return
fi

if [ $dsstat = 1 ] || [ $dsstat = 2 ] || [ $dsstat = 21 ] || [ $dsstat = 99 ]; then
  dsreset=0
  return
else
  LogMessage3 ${3} "Job is not in runnable state. Try to reset.."
  LogMessage3 ${3} "Resetting datastage job : $DSJobName "
   
  $dspath/dsjob -run -mode RESET $1 $2
  
  DSJob_Status=$?
  echo "DSJob_Status : $DSJob_Status"
  
  if [ $DSJob_Status -eq 255 ];then
     dsreset_Stat=2
     return  
  else
    dsstatusb=`$dspath/dsjob -jobinfo $1 $2 | grep "Job Status" | awk -F ':' '{print $2}'`
    while [ "$dsstatusb" = " RUNNING (0)" ]
    do
      LogMessage3 ${3} "[ info ] : status-> $dsstatusb"
      sleep 2
      dsstatusb=`$dspath/dsjob -jobinfo $1 $2 | grep "Job Status" | awk -F ':' '{print $2}'`
    done
      LogMessage ${3} "[ info ] : status-> $dsstatusb"
      LogMessage ${3} "[ info ] : finished resetting job"
      dsreset_Stat=1
      return
  fi     
fi
}


chk_ora_error()
{
ora_value=$1
chk_flag=$2

err_count=`echo "${ora_value}" | grep -i "error" | wc -l`
no_row_count=`echo "${ora_value}" | grep -i "no rows" | wc -l`

if [ "${chk_flag}" = "0" ];then
{
   if [ "${err_count}" = "0" ];then
      return 0;
   else
      return 1;
   fi
}
elif [ "${chk_flag}" = "1" ];then
{
   if [ "${no_row_count}" = "0" ];then
      return 0;
   else
      return 1;
   fi
}
elif [ "${chk_flag}" = "2" ];then
{
   if [ "${err_count}" = "0" ] && [ "${no_row_count}" = "0" ];then
      return 0;
   else
      return 1;
   fi
}
else
{
   return 1;
}
fi

}
