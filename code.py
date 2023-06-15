# Imported Package(s)

import pandas as pd

"""Stage 1: Load the data and modify the indexes

Description

Your HR boss gave you three datasets. The first two are from different offices:
A and B (./Data/A_office_data.xml and ./Data/B_office_data.xml); the third is
the HR's dataset (./Data/hr_data.xml). The guy wants you to investigate the
data. The first thing you need to do is to check and reindex it for further
stages.

Objectives

1 - Load all three datasets.
2 - Reindex all three datasets. It is required  It is required because some of
the employee_office_id column values for offices A and B match. For HR data, use
the employee_id column as the index. For A and B offices, use the name of the
office and the employee_office_id column as indexes. For example, for office A,
employee #125 will be A125. The offices' data index should resemble HR's data
index.

"""

# Loading the data

A_df = pd.read_xml("../Data/A_office_data.xml")
B_df = pd.read_xml("../Data/B_office_data.xml")
HR_df = pd.read_xml("../Data/hr_data.xml")

# Re-indexing Office A's Data Frame

A_ids_list = A_df["employee_office_id"].tolist()
A_ids_list_strs = ['A' + str(num) for num in A_ids_list]
A_df.index = A_ids_list_strs

# Re-indexing Office B's Data Frame

B_ids_list = B_df["employee_office_id"].tolist()
B_ids_list_strs = ['B' + str(num) for num in B_ids_list]
B_df.index = B_ids_list_strs

# Re-indexing HR Office's Data Frame

HR_df.index = HR_df["employee_id"].tolist()

"""Stage 2: Merge everything

Description

Your datasets are ready for merging! It will make data analysis much more 
straightforward.

Objectives

Use concatenation to generate a dataset with information from both offices. Use 
the left merging by index to merge the previously created dataset with the HR's 
dataset. When joining, generate a column containing information about where each 
row came from. Keep only those columns that are present in both datasets.

"""

# Creating a unified data_set that merges both Office A's and B's DataFrames

unified_df = pd.concat([A_df, B_df])

# Merge this <unified_df> with that from HR, keeping only the data from the
# employees included in the <unified_df> (HR doesn't have data for all the
# employees)

total_data_df = unified_df.merge(HR_df, indicator=True, left_index=True,
                                 right_index=True)

# Drop the "employee_office_id", "employee_id", and "_merge" columns

total_data_df.drop(columns=['employee_office_id', 'employee_id', '_merge'],
                   inplace=True)

# Sort the <total_data_df> DataFrame by index

total_data_df.sort_index(inplace=True)
