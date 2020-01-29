bad-renter
===========
Working examples of [Spark ML Pipeline](src/main/scala/com/sgcharts/badrenter/LassoRegressionTraining.scala) and [SMOTE algorithm](src/main/scala/com/sgcharts/badrenter/Sampler.scala) for synthetic data augmentation.

# Prerequisites
Spark 2.4.0

# Class imbalance problem
- Minority class: 1% defaulted on payments
- Under-sample majority class: lose data
- Over-sample minority class: risk overfitting
- Generate synthetic data to augment minority class

## SMOTE algorithm
Over-sample minority class by creating synthetic examples instead of over-sampling with replacement.

Chawla, N. V., Bowyer, K. W., Hall, L. O., & Kegelmeyer, W. P. (2002). SMOTE: synthetic minority over-sampling technique. Journal of artificial intelligence research, 16, 321-357.

## My contributions for this project
- [Implemented SMOTE](src/main/scala/com/sgcharts/badrenter/Sampler.scala) on Spark ML
- Ability to handle discrete attributes (not covered by original paper)
- Speed up kNN search with Locality Sensitive Hashing (LSH)

# Implementation on AWS EMR
## Preprocess
- Spark ML Pipeline
- Cleaning
  - Date formats: 12/31/2010, 20101231
  - Imput dob=“1st Jan 1900” with median age ~39
  - Treat NULL `payment_amount` as zero, get absolute value
- Feature engineering
  - Compute age
  - Categorical variables → numbers: string indexing, one-hot encoding
  - Numbers → categorical variables: bucketing
- Split data to train/test
  - Assign random unique ID
- Store data in hive
  - Partitioned by date (snapshot), clustered by ID
- SMOTE sampling

## Train
Lasso regression

## Validation
- R2 evaluation metric
- 3-fold cross validation (saves time; trains on ⅔ majority of data)
- Tune hyperparameters: L1 regularization
- Store results in hive table
- Save model to S3, later deserialize for serving

## Test
Store results in hive table
