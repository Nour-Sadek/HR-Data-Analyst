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
