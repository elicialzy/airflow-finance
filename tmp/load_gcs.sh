#!/usr/bin/env bash

# Fail out if anything fails
set -e -o pipefail

# To do some timing
startTime="$(date -u +%s)"

for dir in */; do
    # skip schemas dir
    # if [[ "$dir" == "schemas/" ]]; then
    #     continue
    # fi

    # go into dir
    cd $dir
    # loop and upload every json
    for json in *.json; do
        if [ -f "$json" ]; then
            echo
            echo "Preparing to copy $json to GCS..."
            gsutil cp $json gs://3107-cloud-storage/$json
            echo "$json copied to GCS ✅"
        fi
    done
    cd ../
done

echo
echo "All Loaded To GCS 🙌"
endTime="$(date -u +%s)"
duration="$(($endTime - $startTime))"
echo
echo "> $(($duration / 60)) minutes and $(($duration % 60)) seconds elapsed ⏱"
echo