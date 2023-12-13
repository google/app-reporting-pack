base_template_report_id=187f1f41-16bc-434d-8437-7988bed6e8b9
lite_template_report_id=6c386d70-7a1f-4b31-a29b-173f5b671310
base_template_report_id_v1_2_0=3f042b13-f767-4195-b092-35b94e0b430c
report_id=$base_template_report_id_v1_2_0
report_name="app_reporting_pack_copy"
return_link=0

while :; do
case $1 in
	-c|--config)
		shift
		config=$1
		project_id=`grep -A1 "gaarf-bq" $config | tail -n1 | cut -d ":" -f 2 | tr -d " "`
		dataset_id=`grep target_dataset $config | tail -n1 | cut -d ":" -f 2 | tr -d " "`
		incremental=`grep 'incremental: true' $config | wc -l`
		;;
  -p|--project)
		shift
		project_id=$1
		;;
	-d|--dataset)
		shift
		dataset_id=$1
		;;
	-l|--lite)
		report_id=$lite_template_report_id
		;;
  -L|--link)
    return_link=1
    ;;
	-n|--report-name)
		shift
		report_name=`echo "$1" | tr  " " "_"`
		;;
	-h|--help)
		echo -e $usage;
		exit
		;;
	*)
		break
	esac
	shift
done

if [ $incremental -eq 0 ]; then
	link=`cat $(dirname $0)/linking_api.http | sed "s/REPORT_ID/$report_id/; s/REPORT_NAME/$report_name/; s/YOUR_PROJECT_ID/$project_id/g; s/YOUR_DATASET_ID/$dataset_id/g" | sed '/^$/d;' | tr -d '\n'`
else
	link=`cat $(dirname $0)/linking_api_incremental.http | sed "s/REPORT_ID/$report_id/; s/REPORT_NAME/$report_name/; s/YOUR_PROJECT_ID/$project_id/g; s/YOUR_DATASET_ID/$dataset_id/g" | sed '/^$/d;' | tr -d '\n'`
fi
if [ $return_link -eq 1 ]; then
  echo "$link"
else
  open "$link"
fi
