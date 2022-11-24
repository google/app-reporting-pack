project_id=$1
dataset_id=$2

open `cat linking_api.http | sed "s/YOUR_PROJECT_ID/$project_id/g; s/YOUR_DATASET_ID/$dataset_id/g" | sed '/^$/d;' | tr -d '\n'`
