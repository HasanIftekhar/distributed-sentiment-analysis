import os
import pandas as pd

# Define input and output directories and filenames
input_dir = "/home/ubuntu/csvDatasetsProcessed"
output_dir = "/home/ubuntu/csvDatasetsProcessed"
output_filename = "merged_csv.csv"

columns_to_select = ["created_at", "text", "favourites_count", "retweet_count"]

data = []
num_datasets = 0


# Loop through each CSV file in the input directory
for filename in os.listdir(input_dir):
    if filename.endswith(".csv") and num_datasets < 5: # Only select 5 datasets
        # Read the CSV file and select specific columns
        filepath = os.path.join(input_dir, filename)
        df = pd.read_csv(filepath, usecols=columns_to_select)
        
        # Append the data to the list
        data.append(df)
        
        # Increment the counter
        num_datasets += 1
        
# Concatenate all dataframes in the list
merged_df = pd.concat(data)

# Save the concatenated dataframe to a CSV file
output_filepath = os.path.join(output_dir, output_filename)
merged_df.to_csv(output_filepath, index=False)