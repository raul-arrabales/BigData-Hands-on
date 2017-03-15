# Select the project from the list
gcloud projects list 

# Using the project Id
gcloud config set core/project scaleml-161622

# Set zone
gcloud config set compute/zone europe-west1-b

# Create datalab instance
datalab create datalabml1

# If connection loss
atalab connect datalabml1

# To delete the instance
datalab delete datalabml1
