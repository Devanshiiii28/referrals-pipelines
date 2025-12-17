# Referral Validation Pipeline

## Overview
This project implements an end-to-end data engineering pipeline to validate referral rewards using defined business logic and transactional consistency checks.

The pipeline ingests multiple CSV datasets, performs profiling, cleaning, joins them across entities, applies validation rules, and generates a final referral validation report.

## This project demonstrates practical skills in:
- Data analysis and validation
- Data cleaning and standardization
- Multi-table joins using pandas
- Business logic implementation
- Report generation

## Features
The pipeline performs the following steps:

### Data Profiling
- Column names and data types
- Null counts
- Distinct value counts
- Separate profiling reports for each dataset

### Data Cleaning & Standardization
- Standardized string values
- Unified referral source categories
- Handled missing and inconsistent values

### Multi-table Joins
- User referrals
- Referral logs
- Referral statuses
- Referral rewards
- Paid transactions
- Lead logs

## Business Logic Validation
- Verified valid referral flow
- Checked transaction success
- Ensured reward eligibility
- Flagged invalid referrals

### Final Report Generation
- Output only valid referrals
- Exported final CSV report

## Project Structure
project/
├── src/
│ └── main.py
├── data/
│ ├── user_referrals.csv
│ ├── user_referral_logs.csv
│ ├── user_referral_statuses.csv
│ ├── referral_rewards.csv
│ ├── paid_transactions.csv
│ ├── lead_log.csv
│ └── user_logs.csv
├── profiling/
│ └── *_profile.csv
├── output/
│ └── referral_validation_report.csv
├── .gitignore
├── Dockerfile
└── README.md


## Technologies Used
- Python 3
- Pandas
- Docker
- Git / GitHub

## How to Run

### Local Execution
1. Ensure Python 3 is installed  
2. Install required dependencies:
```bash
pip install pandas

## Run the pipeline
python src/main.py

docker build -t referral-validation .
docker run --rm referral-validation

Output

Final validated referrals report:

output/referral_validation_report.csv

Data profiling reports:

profiling/*_profile.csv

Business Logic Summary

A referral is considered valid if:

Referral status is successful

Associated transaction is completed

Referral reward is eligible

No logical inconsistencies exist across joined datasets

Invalid records are filtered out before generating the final report.



Author

Chanchal


