import os
print("CURRENT WORKING DIRECTORY:", os.getcwd())

import pandas as pd
import os

# -----------------------------
# STEP 2: DATA PROFILING
# -----------------------------

# Create profiling folder inside src
os.makedirs("profiling", exist_ok=True)

def profile_dataframe(df, table_name):
    profile = pd.DataFrame({
        "column_name": df.columns,
        "data_type": df.dtypes.astype(str),
        "null_count": df.isnull().sum().values,
        "distinct_count": df.nunique().values
    })
    profile.to_csv(f"profiling/{table_name}_profile.csv", index=False)

# Load all datasets (FIXED PATHS)
user_referrals = pd.read_csv("../data/user_referrals.csv")
user_referral_logs = pd.read_csv("../data/user_referral_logs.csv")
user_logs = pd.read_csv("../data/user_logs.csv")
user_referral_statuses = pd.read_csv("../data/user_referral_statuses.csv")
referral_rewards = pd.read_csv("../data/referral_rewards.csv")
paid_transactions = pd.read_csv("../data/paid_transactions.csv")
lead_logs = pd.read_csv("../data/lead_log.csv")  # or rename to lead_logs.csv

# Run profiling
profile_dataframe(user_referrals, "user_referrals")
profile_dataframe(user_referral_logs, "user_referral_logs")
profile_dataframe(user_logs, "user_logs")
profile_dataframe(user_referral_statuses, "user_referral_statuses")
profile_dataframe(referral_rewards, "referral_rewards")
profile_dataframe(paid_transactions, "paid_transactions")
profile_dataframe(lead_logs, "lead_logs")

print(" COMPLETE: Data profiling files generated.")

# -----------------------------
# STEP 3: DATA CLEANING
# -----------------------------

# Convert datetime columns
datetime_columns = {
    "user_referrals": ["referral_at", "updated_at"],
    "user_referral_logs": ["created_at"],
    "user_logs": ["membership_expired_date"],
    "user_referral_statuses": ["created_at"],
    "referral_rewards": ["created_at"],
    "paid_transactions": ["transaction_at"],
    "lead_logs": ["created_at"]
}

tables = {
    "user_referrals": user_referrals,
    "user_referral_logs": user_referral_logs,
    "user_logs": user_logs,
    "user_referral_statuses": user_referral_statuses,
    "referral_rewards": referral_rewards,
    "paid_transactions": paid_transactions,
    "lead_logs": lead_logs
}

for table_name, cols in datetime_columns.items():
    for col in cols:
        if col in tables[table_name].columns:
            tables[table_name][col] = pd.to_datetime(
                tables[table_name][col], errors="coerce"
            )

# Remove duplicates
for df in tables.values():
    df.drop_duplicates(inplace=True)

print("COMPLETE: Data cleaning and datetime conversion done.")

# -----------------------------
# STEP 4: TIMEZONE CONVERSION
# -----------------------------

import pytz

def convert_utc_to_local(df, time_col, tz_col):
    """
    Convert UTC datetime to local timezone per row
    """
    def convert(row):
        if pd.isna(row[time_col]) or pd.isna(row[tz_col]):
            return row[time_col]
        try:
            return row[time_col].tz_localize("UTC").tz_convert(row[tz_col])
        except Exception:
            return row[time_col]

    return df.apply(convert, axis=1)


# Paid transactions → use timezone_transaction
if "timezone_transaction" in paid_transactions.columns:
    paid_transactions["transaction_at"] = convert_utc_to_local(
        paid_transactions,
        "transaction_at",
        "timezone_transaction"
    )

# Lead logs → use timezone_location
if "timezone_location" in lead_logs.columns:
    lead_logs["created_at"] = convert_utc_to_local(
        lead_logs,
        "created_at",
        "timezone_location"
    )

# User logs → use timezone_homeclub
if "timezone_homeclub" in user_logs.columns:
    user_logs["membership_expired_date"] = convert_utc_to_local(
        user_logs,
        "membership_expired_date",
        "timezone_homeclub"
    )

# User referrals → get timezone from referrer (join with user_logs)
user_referrals = user_referrals.merge(
    user_logs[["user_id", "timezone_homeclub"]],
    left_on="referrer_id",
    right_on="user_id",
    how="left"
)

user_referrals["referral_at"] = convert_utc_to_local(
    user_referrals,
    "referral_at",
    "timezone_homeclub"
)

user_referrals["updated_at"] = convert_utc_to_local(
    user_referrals,
    "updated_at",
    "timezone_homeclub"
)

print("COMPLETE: Timezone conversion applied.")

# -----------------------------
# STEP 5: JOIN ALL TABLES
# -----------------------------

# Join referral logs
df = user_referrals.merge(
    user_referral_logs,
    left_on="referral_id",
    right_on="user_referral_id",
    how="left"
)

# Join referral statuses
df = df.merge(
    user_referral_statuses[["id", "description"]],
    left_on="user_referral_status_id",
    right_on="id",
    how="left"
).rename(columns={"description": "referral_status"})

# Join referral rewards
df = df.merge(
    referral_rewards,
    left_on="referral_reward_id",
    right_on="id",
    how="left",
    suffixes=("", "_reward")
)

# Join paid transactions
df = df.merge(
    paid_transactions,
    on="transaction_id",
    how="left"
)

# Join referrer user details
df = df.merge(
    user_logs[["user_id", "name", "phone_number", "homeclub", "membership_expired_date", "is_deleted"]],
    left_on="referrer_id",
    right_on="user_id",
    how="left",
    suffixes=("", "_referrer")
)

df.rename(columns={
    "name": "referrer_name",
    "phone_number": "referrer_phone_number",
    "homeclub": "referrer_homeclub"
}, inplace=True)

# Join referee user details
df = df.merge(
    user_logs[["user_id", "name", "phone_number"]],
    left_on="referee_id",
    right_on="user_id",
    how="left",
    suffixes=("", "_referee")
)

df.rename(columns={
    "name": "referee_name",
    "phone_number": "referee_phone"
}, inplace=True)

# Join lead logs (only used when referral_source = 'Lead')
df = df.merge(
    lead_logs[["lead_id", "source_category"]],
    left_on="referee_id",
    right_on="lead_id",
    how="left"
)

# Remove duplicate rows
df.drop_duplicates(inplace=True)

print(f"COMPLETE: Joined dataset created with {df.shape[0]} rows.")

# -----------------------------
# STEP 6: SOURCE CATEGORY & STRING STANDARDIZATION
# -----------------------------

# Referral source category logic
def get_referral_source_category(row):
    if row["referral_source"] == "User Sign Up":
        return "Online"
    elif row["referral_source"] == "Draft Transaction":
        return "Offline"
    elif row["referral_source"] == "Lead":
        return row["source_category"]
    else:
        return None

df["referral_source_category"] = df.apply(get_referral_source_category, axis=1)

# String columns to apply InitCap
string_columns = [
    "referrer_name",
    "referee_name",
    "referral_source",
    "referral_status",
    "transaction_status",
    "transaction_type",
    "transaction_location"
]

for col in string_columns:
    if col in df.columns:
        # Only apply string operation if it's a Series (not DataFrame)
        if isinstance(df[col], pd.Series):
            df[col] = df[col].astype(str).str.title()


# IMPORTANT: Do NOT change club names
# referrer_homeclub must remain as-is (usually uppercase)

print("COMPLETE: Source category created & strings standardized.")

# -----------------------------
# STEP 7: BUSINESS LOGIC VALIDATION
# -----------------------------

def is_valid_referral(row):
    reward_value = row.get("reward_value")
    referral_status = row.get("referral_status")
    transaction_id = row.get("transaction_id")
    transaction_status = row.get("transaction_status")
    transaction_type = row.get("transaction_type")
    transaction_at = row.get("transaction_at")
    referral_at = row.get("referral_at")
    membership_expired = row.get("membership_expired_date")
    is_deleted = row.get("is_deleted")
    reward_granted = row.get("is_reward_granted")

    # Condition B: Pending / Failed with no reward
    if referral_status in ["Menunggu", "Tidak Berhasil"] and (pd.isna(reward_value) or reward_value == 0):
        return True

    # Condition A: Successful referral with valid reward
    if (
        pd.notna(reward_value) and reward_value > 0
        and referral_status == "Berhasil"
        and pd.notna(transaction_id)
        and transaction_status == "Paid"
        and transaction_type == "New"
        and pd.notna(transaction_at)
        and pd.notna(referral_at)
        and transaction_at > referral_at
        and transaction_at.month == referral_at.month
        and (pd.isna(membership_expired) or membership_expired > referral_at)
        and is_deleted is False
        and reward_granted is True
    ):
        return True

    return False

# Convert reward_value to numeric
df["reward_value"] = pd.to_numeric(df["reward_value"], errors="coerce")

# Apply business logic
df["is_business_logic_valid"] = df.apply(is_valid_referral, axis=1)


print("COMPLETE: Business logic validation applied.")

print(df["is_business_logic_valid"].value_counts())
print("Row count:", df.shape[0])

print("Valid referrals BEFORE final selection:", df[df["is_business_logic_valid"] == True].shape[0])

missing_counts = df[df["is_business_logic_valid"] == True].isnull().sum()
print("Missing values in valid referrals:\n", missing_counts)

# -----------------------------
# STEP 8: FINAL OUTPUT
# -----------------------------

final_df = df[df["is_business_logic_valid"] == True].copy()

print("Final row count:", final_df.shape[0])

final_df = final_df[[
    "id",
    "referral_id",
    "referral_source",
    "referral_source_category",
    "referral_at",
    "referrer_id",
    "referrer_name",
    "referrer_phone_number",
    "referrer_homeclub",
    "referee_id",
    "referee_name",
    "referee_phone",
    "referral_status",
    "reward_value",
    "transaction_id",
    "transaction_status",
    "transaction_at",
    "transaction_location",
    "transaction_type",
    "updated_at",
    "created_at",
    "is_business_logic_valid"
]]

final_df.rename(columns={
    "id": "referral_details_id",
    "reward_value": "num_reward_days",
    "created_at": "reward_granted_at"
}, inplace=True)

os.makedirs("../output", exist_ok=True)

final_df.to_csv("../output/referral_validation_report.csv", index=False)

print("FINAL REPORT GENERATED: output/referral_validation_report.csv")
