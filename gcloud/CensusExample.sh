# Adapted from https://cloud.google.com/ml-engine/docs/how-tos/getting-started-training-prediction 
# CLOUD SHELL version

# Cloud ML Engine sample zip file.
wget https://github.com/GoogleCloudPlatform/cloudml-samples/archive/master.zip 
unzip master.zip
cd cloudml-samples-master/census/estimator 

# Get the data to a local dir
mkdir data
gsutil -m cp gs://cloudml-public/census/data/* data/

# Train and Test Data 
TRAIN_DATA=$(pwd)/data/adult.data.csv
EVAL_DATA=$(pwd)/data/adult.test.csv

# Set output dir
MODEL_DIR=output
rm -rf $MODEL_DIR

# Train locally
gcloud ml-engine local train \
    --module-name trainer.task \
    --package-path trainer/ \
    -- \
    --train-files $TRAIN_DATA \
    --eval-files $EVAL_DATA \
    --train-steps 1000 \
    --job-dir $MODEL_DIR
    
# Inspect using tensorboard
python -m tensorflow.tensorboard --logdir=output --port=8080

# Local trainer in distributed mode
gcloud ml-engine local train \
    --module-name trainer.task \
    --package-path trainer/ \
    --distributed \
    -- \
    --train-files $TRAIN_DATA \
    --eval-files $EVAL_DATA \
    --train-steps 1000 \
    --job-dir $MODEL_DIR


# Check output
ls -R output-dist/
python -m tensorflow.tensorboard --logdir=$MODEL_DIR --port=8080

# Cloud storage bucket setup

PROJECT_ID=$(gcloud config list project --format "value(core.project)")
BUCKET_NAME=${PROJECT_ID}-mlengine

BUCKET_NAME="tf1-bucket"

echo $BUCKET_NAME

REGION=europe-west1-d

# Create the bucket 
gsutil mb -l $REGION gs://$BUCKET_NAME

# Copy files to the bucket
gsutil cp -r data gs://$BUCKET_NAME/data

TRAIN_DATA=gs://$BUCKET_NAME/data/adult.data.csv
EVAL_DATA=gs://$BUCKET_NAME/data/adult.test.csv

gsutil cp ../test.json gs://$BUCKET_NAME/data/test.json
TEST_JSON=gs://$BUCKET_NAME/data/test.json

# Single instance trainer in the cloud
JOB_NAME=census_single_1
OUTPUT_PATH=gs://$BUCKET_NAME/$JOB_NAME
gcloud ml-engine jobs submit training $JOB_NAME \
--job-dir $OUTPUT_PATH \
--runtime-version 1.0 \
--module-name trainer.task \
--package-path trainer/ \
--region $REGION \
-- \
--train-files $TRAIN_DATA \
--eval-files $EVAL_DATA \
--train-steps 1000 \
--verbose-logging true


gsutil ls -r $OUTPUT_PATH

python -m tensorflow.tensorboard --logdir=$OUTPUT_PATH --port=8080

# Distributed training in the cloud
JOB_NAME=census_dist_1
OUTPUT_PATH=gs://$BUCKET_NAME/$JOB_NAME

gcloud ml-engine jobs submit training $JOB_NAME \
    --job-dir $OUTPUT_PATH \
    --runtime-version 1.0 \
    --module-name trainer.task \
    --package-path trainer/ \
    --region $REGION \
    --scale-tier STANDARD_1 \
    -- \
    --train-files $TRAIN_DATA \
    --eval-files $EVAL_DATA \
    --train-steps 1000 \
    --verbose-logging true


python -m tensorflow.tensorboard --logdir=$OUTPUT_PATH --port=808
