import os
import pandas as pd

# Read Netflow Data
netflowDf = pd.read_csv(os.getcwd().split("flink-kafka-scala-tutorial/scripts/python")[0] + "flink-kafka-scala-tutorial/data/netflow.csv")

# Read CDR Data
cdrDf = pd.read_csv(os.getcwd().split("flink-kafka-scala-tutorial/scripts/python")[0] + "flink-kafka-scala-tutorial/data/cdr.csv")

# Join netflowDf with cdrDf based on SOURCE_ADDRESS and PRIVATE_IP columns, respectively.
final_df = netflowDf.merge(cdrDf, left_on = "SOURCE_ADDRESS", right_on = "PRIVATE_IP", how="inner")

# Specify integer columns
int_cols = ["START_DATETIME", "NETFLOW_DATETIME", "END_DATETIME", "SOURCE_PORT", "START_REAL_PORT", "END_REAL_PORT"]

# Change the int_cols columns format to int in order to be able to apply equal, smaller, and bigger calculations on them 
for intCol in int_cols:
    final_df[intCol] = final_df[intCol].astype(int)

# Take observations which achive the following crateria:
# A. NETFLOW_DATETIME betwen START_DATETIME and END_DATETIME
# B. SOURCE_PORT between START_REAL_PORT and END_REAL_PORT
final_df = final_df[(final_df["START_DATETIME"] <= final_df["NETFLOW_DATETIME"]) & (final_df["NETFLOW_DATETIME"] <= final_df["END_DATETIME"])]
final_df = final_df[(final_df["START_REAL_PORT"] <= final_df["SOURCE_PORT"]) & (final_df["SOURCE_PORT"] <= final_df["END_REAL_PORT"])]

# Change the IN_BYTES column format to float in order to be able to apply sum aggregation on it
final_df["IN_BYTES"] = final_df["IN_BYTES"].astype(float)

# Group by the df based on CUSTOMER_ID column and sum the IN_BYTES column
final_df = final_df.groupby(["CUSTOMER_ID"], as_index=False).agg(sumOutBytes = ("IN_BYTES", "sum"))

# Convert the obtained column from Bytes to MegaBytes
final_df["sumOutBytes"] = final_df["sumOutBytes"] / (1024 * 1024)

# Print the first 5 rows of the obtained result
print(final_df.head(5))

final_df.to_csv(os.getcwd().split("flink-kafka-scala-tutorial/scripts/python")[0] + "flink-kafka-scala-tutorial/data/result.csv", index=False)
