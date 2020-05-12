#!/usr/bin/env bash

# --------------------------------------------------------------------------- #
# Author: Marcia Cunha ############################### Created >> 20190707    #
# -----------------------------Update-----------------------------------------#
# Date ###################### Author ################### Obs ################ #
#                                                                             #
# --------------------------------------------------------------------------- #
#  Este programa irá extrair arquivos de um local, separar por dia e jogar em #
#  outro repositorio particionado por ano,mes,dia                             #
# --------------------------------------------------------------------------- #
from_days=$1
to_days=$2
from_sch="s3://pathbucket_origin"
to_sch="s3://pathbucket_destiny"

until [ $from_days -gt $to_days ]
do
# ------------------------------- Var-----------------------------------------#
	year=$(date -d "-$from_days days" +"%Y")
	month=$(date -d "-$from_days days" +"%m")
	day=$(date -d "-$from_days days" +"%d")
    my_fulldate=`date -d "$from_days days ago" +%Y-%m-%d`
# ------------------------------- Check delta is empty -----------------------#
  if [[ -n  "$my_fulldate" ]]
	 then
# ------------------------------- attrib files to list -----------------------#
  
  	files=`aws s3 ls "${from_sch}"/ | grep -i "[a-z0-9]*_${my_fulldate}T[^ ]*\.csv\.gz" -o`

# ---------------Save local path for to compare in future --------------------#
    `aws s3 ls "${to_sch}"/path"/year=$year/month=$month/day=$day/ | grep -i "[a-z0-9]*_${my_fulldate}T[^ ]*\.csv\.gz" -o > metadata_queue.csv`
# ------------------To Create partition------------------ --------------------#
     echo "************************************************************"
		 echo "*********** CHECK DAY OF EXEC:${my_fulldate} *******************"
		 echo "************************************************************"

		 aws athena start-query-execution --query-string "ALTER TABLE raw_schema.stg_tablename ADD IF NOT EXISTS PARTITION (year='$year',month='$month', day='$day')" --output text --result-configuration OutputLocation="s3://patchbucket_destiny/retorno_query/"
     #aws athena start-query-execution --query-string "MSCK REPAIR TABLE raw_schema.stg_tablename;" --result-configuration OutputLocation="s3://patchbucket_destiny"
# -------------------To read path local file ---------------------------------#
	  SCRIPT=$(readlink -f metadata_queue.csv)
		METADATA_UNIQUE_COUNT=`sort "${SCRIPT}"|uniq| wc -l`

# -------------------Cp files in other bucket --------------------------------#
		if [ $METADATA_UNIQUE_COUNT -gt 1 ]
      then
			for file in ${files}
			do
					w=`grep "$file"  "$SCRIPT"|uniq|wc -l`
					if [ $w -lt 1 ]

					 	then
						  aws s3 cp "${from_sch}"/$file "${to_sch}"/path_dir/year=$year/month=$month/day=$day/
					fi

			done
			echo "ALL FILE THIS OF ${my_fulldate} EXISTS in S3 BUCKET"
		  else
				for file in ${files}
				do
					aws s3 cp "${from_sch}"/$file "${to_sch}"/path_dir/year=$year/month=$month/day=$day/
				done
	  fi

	 else
	   exit 1
	fi
	#talvez nao precise
  if [ $from_days == $to_days ]
			then
			exit 1
		else
			from_days=$((from_days+1))
	fi
#aws athena start-query-execution --query-string "ALTER TABLE raw_schema.stg_tablename ADD IF NOT EXISTS PARTITION (year='$year',month='$month', day='$day')" --output text --result-configuration OutputLocation="s3://bucket-destiny/"
done
--
#depois basta criar uma tabela com location para o buxket de destino