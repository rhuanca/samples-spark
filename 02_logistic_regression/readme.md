# Overview
Sample notebooks that demonstrates the usage of kafka as a data lake.

The first two notebooks need access to a s3 bucket. So in order to test them a test bucket needs to be provided with its keys.

The keys need to set as environment variables with the following command:
```
export AWS_ACCESS_KEY=...
export AWS_SECRET_KEY=...
```

Also it is ok to use localfile system.

# The Notebooks

## 01_Collect_Tracking.ipynb
Demostrates how to build a spart structured stream that pulls data from a s3 bucket into a topic named *tracking* in kafka.

This started, this notebook never ends because it continously checks if data is available in bucket.

## 02_Collect_Advertising.ipynb
Similar to the previous one except that it pulls data from a advertising.csv file into a topic names *advertising* in kafka.

This one is needed to run notebok 03.

## 03_Click_Prediction.ipynb
Demonstrates the usage of the data populated in the *advertising* topic made in the previous notebook to build a LogisticRegression model with Apache Spark.