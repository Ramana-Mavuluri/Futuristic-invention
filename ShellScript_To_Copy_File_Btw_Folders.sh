PrimaryFolder=$1;
SecondaryFolder=$2;
FileNm=$3;
BatchDate=$4;
ScriptPath=$5;

if [[ -z  $BatchDate ]]
then
	SrcFile=${FileNm};
else
	SrcFile=${FileNm}"_"${BatchDate}".txt";
fi

Cut_Off_Time=`date +'%Y%m%d %H:%M:%S'  -d "+ 480 minutes"`;

if [ -d ${SecondaryFolder} ]
then
	echo "Secondary folder ${SecondaryFolder} exists"
	while [ ! -f ${PrimaryFolder}"/"${SrcFile} ]
	do
		if [ -f ${SecondaryFolder}"/"${SrcFile} ]
		then
			sh ${ScriptPath}"CN_transfFileFinished.sh" ${SecondaryFolder}"/"${SrcFile} ${BatchDate}
			cp ${SecondaryFolder}"/"${SrcFile} ${PrimaryFolder}"/"${SrcFile}
			if [ $? == 0 ] 
			then 
				I_flg=1
				break
			fi
		fi
		
		sleep 120
		
		Present_Time=`date +'%Y%m%d %H:%M:%S'`;
		
		if [[ "${Present_Time}" >  "${Cut_Off_Time}" ]]
		then
			echo "Waited too long for the file, breaking now"
			I_flg=0
			break
		fi
		
	done
else
	echo "Secondary folder ${SecondaryFolder} does not exists, file has to come to Primary folder ${PrimaryFolder} folder"
fi



if [ -f ${PrimaryFolder}"/"${SrcFile} ]
then
	if [ -f ${SecondaryFolder}"/"${SrcFile} ]
	then
		if [ $I_flg == 1 ]
		then
			echo "File in Primary folder has been copied from Secondary folder"
		else
			STATUS="$(cmp --silent ${PrimaryFolder}"/"${SrcFile} ${SecondaryFolder}"/"${SrcFile}; echo $?)" 
			if [ $STATUS == 0 ]
			then
				echo "Both files are same"
			else
				echo "File present in both the folders but, they are different. File in primary folder takes precedence"
			fi
		fi
	fi
else
	if [ $I_flg == 0 ]
	then
		echo "Check with source team, because file is not present in either of the folders"
		exit 1;
	fi
fi
