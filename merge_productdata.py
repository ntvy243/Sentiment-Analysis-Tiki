# Import library
import pandas as pd
import os
from datetime import datetime

# Announce directory name
directory = "D:\KHÓA LUẬN TỐT NGHIỆP\CrawlData"


# Return list file_name in product and append in to a dataframe
files = [
    filename for filename in os.listdir(os.path.join(directory, "product")) 
    if filename.endswith(".csv")
    ]


# Check if files were found
if not files:
    print("No CSV files found in the directory.")
else:
    print("CSV files found:", files)

# Read each filename into a dataframe 
dfs = []
for filename in files:
    df = pd.read_csv(os.path.join(directory, "product", filename))
    print(f"Content of file {filename}:")
    print(df.head())  # Print the first few rows of the dataframe
    dfs.append(df)

# Merged all dataframe
mergedproduct = pd.concat(dfs, ignore_index=True)
print("Merged DataFrame (before processing):")
print(mergedproduct.head())

# Fill null values with a specific value (e.g., empty string or 0)
mergedproduct = mergedproduct.fillna("")

# Display information about null values
print("Null values in the DataFrame after filling null values:")
print(mergedproduct.isnull().sum())

# Drop duplicates
mergedproduct = mergedproduct.drop_duplicates(subset=['id'])
print("DataFrame after dropping duplicates:")
print(mergedproduct.head())

# Number of rows
num_rows = mergedproduct.shape[0]
print("Number of rows is:", num_rows)

# Check if DataFrame is not empty before saving
if num_rows > 0:
    # Convert df to csv
    current_datetime = datetime.now().strftime("%Y%m%d_%H%M")
    mergedproduct_filename = f"mergedproduct_{current_datetime}.csv"
    os.makedirs(os.path.join(directory, "merged"), exist_ok=True)
    mergedproduct.to_csv(os.path.join(directory, "merged", mergedproduct_filename), index=False, encoding='utf-8-sig')
    print(f"Saved merged CSV to {os.path.join(directory, 'merged', mergedproduct_filename)}")
else:
    print("No rows to save.")