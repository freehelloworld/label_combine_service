# Label jobs combiner
Combine all jobs that were done on the same image and generate a final label.

## Overview
Solution is delivered as a microservice in term of API. Three API endpoints are exposed to cater the requirement for both historical jobs and new jobs.
1. `/combine/<loc_id>`, combines jobs for a given location id.
2. `/combine/all`, combines jobs for all locations.
3. `/combine/new/<date>`, combines jobs for locations that have been updated after given date.

Combined labels for a location is saved to output_path with name `{loc_id}_combined.h5`.

`attribute_manifest.csv` is used as the source of truth, from which new jobs or all jobs are calculated.
Note that endpoint 2 and 3 combine jobs sequentially. To finish endpoint 2 for given dataset (1134 locations) could take a long time. 

The best way to combine jobs for all locations is to call /combine/<loc_id> in parallel.

## Implementation detail
### Key methods
`Combiner._read_label_data`, it reads label data of a location, stores the data in a dictionary and returns a dictionary. For example location 4289, it has 11 jobs for both class 2 and 3, so the returned dictionary has two keys, and each corresponding value is a list with 11 elements, each of which is a nparray with shape (896,896).

`Combiner.combine`, it combines labels of all jobs and take the majority as the final label. Take location 4289, class 2 as example, the labels of 11 jobs are in a list of nparrays, which firstly are stacked into a 3 dimensional nparray, with shape(896,896,11). Then the inner most dimension is an array of 11 labels, so we are able to apply a function to pick the most common value.

`Combiner._get_most_common` is a util function that finds the most common value in a given array.

 Interaction with storage is implemented in class `StorageHelper`, in which local storage is fully tested, but the s3 part is not tested yet.

### Toolsets and frameworks
Solution is implemented in Python and a few important libraries are worth mentioning:
numpy is used to manipulate multi-dimensional label data. h5py is used to read h5 file. Pandas is used to handle the manifest file and find new jobs according to date.

Dockerfile is provided to containerize the program. AWS fargate and GCP Cloudrun are recommended Cloud products to host the service.

## Test and Deployment
### Local test
Update file `run-local.sh` to set up the environment variables with dataset location and then run:
```
bash run-local.sh
```

### Deployment
Best to build the solution into container and deploy to a service like AWS Fargate or GCP Cloudrun.
Here are sample scripts for deploying to Google Cloudrun:
1 Build the image and submit it to GCR
```
gcloud builds submit --tag gcr.io/{PROJECT_ID}/{image-name}
```

 2 Deploy it to Cloud Run
```
gcloud run deploy {service-name} --image gcr.io/{PROJECT_ID}/{image-name} --platform managed --memory 512Mi --set-env-vars=SOURCE_PATH={SOURCE_PATH_VALUE},OUTPUT_PATH={PATH_VALUE} --region=australia-southeast1
```