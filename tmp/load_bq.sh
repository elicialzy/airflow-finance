#!/usr/bin/env bash

# Fail out if anything fails
set -e -o pipefail

# To do some timing
startTime="$(date -u +%s)"

for dir in */; do
    # go into dir
    cd $dir
    # loop and upload every json
    for json in *.json; do
        if [ -f "$json" ]; then
            echo
            echo "Preparing to load $json to BigQuery..."
            #TODO: need to decide the table name
            # bq load --project_id=project-346314  \
            #     --autodetect=true \
            #     --source_format=NEWLINE_DELIMITED_JSON  \
            #     3107.Restaurant \
            #     gs://3107-cloud-storage/restaurant.json
            echo "$json loaded to BigQuery ✅"
        fi
    done
    cd ../
done

echo
echo "All Loaded To BigQuery 🙌"
endTime="$(date -u +%s)"
duration="$(($endTime - $startTime))"
echo
echo "> $(($duration / 60)) minutes and $(($duration % 60)) seconds elapsed ⏱"
echo