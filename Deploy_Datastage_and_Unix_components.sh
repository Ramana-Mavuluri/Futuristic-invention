#!/bin/sh
# The Script used to import the dsx file into the datastage component through the server.Also it used to deploy the scripts and schema on the respective location.
#In order to execute the shell script,three parameters to be provided for trigger the import commmand
#Pre-Requisite parameters for the Execution

 
# To Read all the input paramater from the configuration file.


C_CTRY=$1
DOD_NUM=$2

if [ $# -ne 2 ]

then 

echo "Please pass the valid parameters"

exit 1

fi

if [[ -f $C_CTRY.cnf ]]

then
 
. ./$C_CTRY.cnf

else 

echo "Configuration file not available for the country $C_CTRY"

exit 1 

fi 


	#------To unzip the DOD package as part of the CR ---------#


if [[ -f ${DOD_Base_Path}/${DOD_NUM}.zip ]]

then

rm -r ${DOD_Base_Path}/${DOD_NUM}

mkdir -p ${DOD_Base_Path}/${DOD_NUM}

unzip ${DOD_Base_Path}/${DOD_NUM}.zip  -d ${DOD_Base_Path}/${DOD_NUM}/

echo "unzip the DOD package Successfully"

else

echo "DOD Package Not Available in the directory ${DOD_Base_Path}" 

exit 1

fi

# To Validate the Country code as part of the DOD package #

if [[ -e ${DOD_Base_Path}/${DOD_NUM}/$C_CTRY ]]

then 

echo "Valid country for the DOD package"

else

echo "Country Code Not Match for the DOD package"

exit 1

fi

#To Create the Backup directory for Scripts,Schema and DSX to place the Backup package incase of any rollback.

        #------To create the Backup DSX directory for Rollback cases---------#



if [[ -e $DOD_Base_Path/BACKUP/Rollback_$DOD_NUM/$C_CTRY ]];

then

echo "Backup Directory already exist for  $DOD_NUM/$C_CTRY"

else 

mkdir -p $DOD_Base_Path/BACKUP/Rollback_$DOD_NUM/$C_CTRY

fi

				#To list the DOD package and make the respective directory structure# 

find $DOD_Base_Path/$DOD_NUM/ -type f |awk -F"/" -v ctry_code=$C_CTRY '{if($6==ctry_code) print $0}' |sed "s/${DOD_NUM}/|/g" > $DOD_Base_Path/${DOD_NUM}_COMPONENT_LIST.txt

find $DOD_Base_Path/$DOD_NUM/ -type d |awk -F"/" -v ctry_code=$C_CTRY '{if($6==ctry_code) print $0}' | awk -F"${DOD_NUM}" '{print $2}' > $DOD_Base_Path/${DOD_NUM}_TEMP.txt

for i in `cat $DOD_Base_Path/${DOD_NUM}_TEMP.txt`
 
do

if [[ ! -e $DOD_Base_Path/BACKUP/Rollback_$DOD_NUM/$i ]]
 
then

mkdir $DOD_Base_Path/BACKUP/Rollback_$DOD_NUM/$i

else

echo "Rollback Directory Exist"

fi

Base_Dir=`echo $i |awk -F"/" '{print $3}'`


if [[ ! -e ${Base_Path}/$i && ${Base_Dir} != 'Documents' ]]

then

mkdir -p $Base_Path/$i

else

echo "Base Directory Exist"

fi

done

#To remove the temp files on the directory#

rm -f ${DOD_Base_Path}/BACKUP/Rollback_${DOD_NUM}/${DOD_NUM}_${C_CTRY}_NEW_CODE_PACKAGE.txt
rm -f ${DOD_Base_Path}/Datastage_Jobs_List_${C_CTRY}_${DOD_NUM}.txt
rm -f ${DOD_Base_Path}/JOB_IMPORT_STATUS_${C_CTRY}_${DOD_NUM}.txt
rm -f ${DOD_Base_Path}/JOB_IMPORT_STATUS_${C_CTRY}_${DOD_NUM}.log
rm -f ${DOD_Base_Path}/JOB_IMPORT_FAILED_${C_CTRY}_${DOD_NUM}.log
rm -f ${DOD_Base_Path}/JOB_IMPORT_SUCCESS_${C_CTRY}_${DOD_NUM}.log
 
	#-------- Copy the package into the Rollback directory-------------------#

awk -F'|' '{print $2}' $DOD_Base_Path/${DOD_NUM}_COMPONENT_LIST.txt | while read DOD_Cmp_name
	
do
	#-------- Step 1 : To Copy the package into the Rollback directory from the Base path -------------#

if [ -f ${Base_Path}${DOD_Cmp_name} ]

then
		
cp ${Base_Path}${DOD_Cmp_name} ${DOD_Base_Path}/BACKUP/Rollback_${DOD_NUM}${DOD_Cmp_name}
                
echo "Files copied to backup directroy $DOD_Cmp_name"
         
else

echo " New Files are added as part of the CR ${Base_Path}/${DOD_Cmp_name}" >> ${DOD_Base_Path}/BACKUP/Rollback_${DOD_NUM}/${DOD_NUM}_${C_CTRY}_NEW_CODE_PACKAGE.txt
	
fi

	#---------Step 2 : To copy the DOD package to Production Base path ------------------------------#

	#--------- To deploy the code package into the Prod Environment Only for Schema and Scripts ------- #
	
File_Extn=`echo ${DOD_Cmp_name} | cut -f2 -d'.'`
        
File_Dir=`echo ${DOD_Cmp_name} |cut -f3 -d'/'`
              
if [[ ${File_Dir} != 'Documents' ]]

then

echo "$DOD_Cmp_name" >> ${DOD_Base_Path}/val.txt

cp  ${DOD_Base_Path}/${DOD_NUM}/${DOD_Cmp_name} ${Base_Path}${DOD_Cmp_name}
                  
chmod 774 ${Base_Path}${DOD_Cmp_name} 

fi

       # To list the Dsx components from teh DOD package#       

if [ "${File_Extn}" == 'dsx' ]
	
then
	
echo ${DOD_Base_Path}/${DOD_NUM}${DOD_Cmp_name} >> ${DOD_Base_Path}/Datastage_Jobs_List_${C_CTRY}_${DOD_NUM}.txt
	
fi

done

# To place the config files

if [ -f ${DOD_Base_Path}/${DOD_NUM}/${C_CTRY}/config/*.* ]

then

cp ${DOD_Base_Path}/${DOD_NUM}/${C_CTRY}/config/*.*  ${Base_Path}/config/
chmod -R 775 ${Base_Path}/config/

else

echo "No config files available"

fi

	#-----------To Import the Dsx Code package------------------------#

ls ${DOD_Base_Path}/Datastage_Jobs_List_${C_CTRY}_${DOD_NUM}.txt

DS_JOBS_CNT=`wc -l ${DOD_Base_Path}/Datastage_Jobs_List_${C_CTRY}_${DOD_NUM}.txt | awk '{ print $1 }'`

LOOP_DSIMPORT=0

for i in `cat ${DOD_Base_Path}/Datastage_Jobs_List_${C_CTRY}_${DOD_NUM}.txt`

do

echo "$i"

project=`echo ${i}|awk -F"/" '{print $8}'`

PROJECT_NAME=`grep -w $project ${C_CTRY}.cnf | awk -F"|" '{print $2}'` 


LOOP_DSIMPORT=`expr ${LOOP_DSIMPORT} + 1`

PENDINGDS_JOB_CNT=`expr ${DS_JOBS_CNT} - ${LOOP_DSIMPORT}` 

echo "Pending Jobs Count is ${PENDINGDS_JOB_CNT}"

sh $DS_Import_Path/DSXImportService.sh -ISHost $Host_Name -ISuser $DS_User_Name -ISpassword $DS_Password -DSProject $PROJECT_NAME -DSXFile $i -Overwrite -verbose >> ${DOD_Base_Path}/JOB_IMPORT_STATUS_${C_CTRY}_${DOD_NUM}.log

if [[ $C_CTRY == "MEPA" ]]

then

PROJECT_NAME2=`grep -w $project ${C_CTRY}.cnf | awk -F"|" '{print $3}'` 

sh $DS_Import_Path/DSXImportService.sh -ISHost $Host_Name -ISuser $DS_User_Name -ISpassword $DS_Password -DSProject $PROJECT_NAME2 -DSXFile $i -Overwrite -verbose >> ${DOD_Base_Path}/JOB_IMPORT_STATUS_${C_CTRY}_${DOD_NUM}.log

else 

echo "Import not required for this $C_CTRY implementation"

fi

done

#To generate the report for import job list#

filename=${DOD_Base_Path}/JOB_IMPORT_STATUS_${C_CTRY}_${DOD_NUM}.log

ln_num=`grep -n 'Items not imported' $filename |awk -F":" '{print $1}'`

for i in $ln_num

do

echo $i

joblist=`sed -n "${i}p" $filename|awk -F"=" '{print $2}'|awk '{$1=$1;print}'`

echo "$joblist"

if [[ $joblist != "0" ]]

then

echo "inside if"

prj=`expr ${i} - 6`

job=`expr ${i} - 8`

echo "${prj} || ${job}"

sed -n "${job}","${i}p" $filename >> ${DOD_Base_Path}/JOB_IMPORT_FAILED_${C_CTRY}_${DOD_NUM}.log

else

echo "inside else"
prj=`expr ${i} - 6`

job=`expr ${i} - 8`

echo "${prj} || ${job}"

sed -n "${job}","${i}p" $filename >> ${DOD_Base_Path}/JOB_IMPORT_SUCCESS_${C_CTRY}_${DOD_NUM}.log

fi

done
   
 # To remove the Temp Files#
rm -f ${DOD_Base_Path}/*.txt
