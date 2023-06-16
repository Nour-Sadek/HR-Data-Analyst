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

A_df = pd.read_xml("./Data/A_office_data.xml")
B_df = pd.read_xml("./Data/B_office_data.xml")
HR_df = pd.read_xml("./Data/hr_data.xml")

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

# Printing Office A, B, and HR data indexes

print("Office A data's indexes:")
print([index for index in A_df.index])
print("Office B data's indexes:")
print([index for index in B_df.index])
print("HR Office data's indexes:")
print([index for index in HR_df.index])

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

# Printing out <total_data_df> index and column names

print("The merged dataframe's indexes:")
print([index for index in total_data_df.index])
print("The merged dataframe's column names:")
print([column for column in total_data_df.columns])

"""Stage 3: Get the insights

Description

The HR boss needs to know something about the employees.

Objectives

Find out the answers to the following questions:

1 - What are the departments of the top ten employees in terms of working hours? 
Output a Python list of values
2 - What is the total number of projects on which IT department employees with 
low salaries have worked? Output a number
3 - What are the last evaluation scores and the satisfaction levels of the 
employees A4, B7064, and A3033? Output a Python list where each entry is a list 
of values of the last evaluation score and the satisfaction level of an 
employee. The data for each employee should be specified in the same order as 
the employees' IDs in this question.

"""

# Answer the question: What are the departments of the top ten employees in
# terms of working hours?

top_10_departments = \
    total_data_df.sort_values('average_monthly_hours', ascending=False)[
        'Department'].head(10).tolist()

print("What are the departments of the top ten employees in terms of working \
hours?")
print(top_10_departments)

# Answer the question: What is the total number of projects on which IT
# department employees with low salaries have worked? Output a number

low_salary_IT = total_data_df.query("salary == 'low' & Department == 'IT'")
low_salary_IT_projects_sum = sum(low_salary_IT['number_project'].tolist())

print("What is the total number of projects on which IT department employees \
with low salaries have worked?")
print(low_salary_IT_projects_sum)

# Answer the question: What are the last evaluation scores and the
# satisfaction levels of the employees A4, B7064, and A3033?

required_employees = ['A4', 'B7064', 'A3033']
final_list = []

for employee in required_employees:
    employee_list = []
    employee_list.append(total_data_df.loc[employee, 'last_evaluation'])
    employee_list.append(total_data_df.loc[employee, 'satisfaction_level'])
    final_list.append(employee_list)

print("What are the last evaluation scores and the satisfaction levels of the \
employees A4, B7064, and A3033?")
print(final_list)

"""Stage 4: Aggregate the data

Description

The HR boss wants to delve into the metrics of the two employee groups: those 
who left the company and those who still work for us. You decided to present the 
information in a table.

Objectives

The HR boss asks for the following metrics for the two different groups (those 
who left and those who are still working):

1 - the median number of projects the employees in a group worked upon, and how 
many employees worked on more than five projects
2 - the mean and median time spent in the company
3 - the share of employees who've had work accidents
4 - the mean and standard deviation of the last evaluation score

"""


# Creating count_bigger_5 function that counts the number of employees who
# worked on more than five projects to use in thw .agg() method

def count_bigger_5(series) -> int:
    """Returns the number of employees that have worked on more than 5
    projects."""
    more_than_5_bool = series > 5
    return more_than_5_bool.sum()


# Determining, in each group, the median number of projects the employees
# worked on and how many employees worked on more than five projects
number_project_df = total_data_df.groupby('left').agg(
    {'number_project': ['median', count_bigger_5]}).round(2)

# Determining, in each group, the mean and median time spent in the company

time_spend_company_df = total_data_df.groupby('left').agg(
    {'time_spend_company': ['mean', 'median']}).round(2)

# Determining, in each group, the share of employees who've had work
# accidents
work_accident_df = total_data_df.groupby('left').agg(
    {'Work_accident': ['mean']}).round(2)

# Determining, in each group, the mean and standard deviation of the last
# evaluation score
last_evaluation_df = total_data_df.groupby('left').agg(
    {'last_evaluation': ['mean', 'std']}).round(2)

# Merging the 4 DataFrames <number_project_df>, <time_spend_company_df>,
# <work_accident_df>, and <last_evaluation_df>

merged_df = number_project_df.merge(time_spend_company_df,
                                    left_index=True,
                                    right_index=True).merge(
    work_accident_df,
    left_index=True,
    right_index=True).merge(last_evaluation_df,
                            left_index=True,
                            right_index=True)

print("The dataframe with the required metrics after data aggregation:")
print(merged_df)

# Returning <merged_df> as a dictionary

merged_as_dict = merged_df.to_dict()

print("Dictionary form of the dataframe with the required data:")
print(merged_as_dict)

"""Stage 5: Draw up pivot tables

Objectives

The HR boss desperately needs your pivot tables for their report:

1 - First pivot table:
The first pivot table displays departments as rows, employees' current status, 
and their salary level as columns. The values should be the median number of 
monthly hours employees have worked.
In the table, the HR boss wants to see only those departments where either one 
is true:
  - For the currently employed: the median value of the working hours of 
    high-salary employees is smaller than the hours of the medium-salary employees, 
    OR:
  - For the employees who left: the median value of working hours of low-salary 
    employees is smaller than the hours of high-salary employees

2 - Second pivot table:
The second pivot table is where each row is an employee's time in the company; 
the columns indicate whether an employee has had any promotion. The values are 
the last evaluation score's minimum, maximum, mean, and satisfaction level. 
Filter the table by the following rule: select only those rows where the 
previous mean evaluation score is higher for those without promotion than those 
who had.

"""

"""Creating the first pivot table"""

# Creating the data frame with the required columns and changing the
# datatype of the 'left' column to float

required_columns_df = total_data_df[
    ['Department', 'left', 'salary', 'average_monthly_hours']]
required_columns_df['left'] = required_columns_df['left'].astype(float)

# Aggregating the data by applying the median function to
# 'average_monthly_hours' column

required_columns_df = required_columns_df.groupby(
    ['salary', 'Department', 'left']).agg(
    {'average_monthly_hours': 'median'})

# Creating the required pivot table before any filtering

first_pivot_table_unfiltered = required_columns_df.pivot_table(
    index='Department',
    columns=['left', 'salary'],
    values='average_monthly_hours').round(2)

# Creating a temporary short name for <first_pivot_table_unfiltered> for
# readability, then filtering the pivot table according to the requirements

df = first_pivot_table_unfiltered
first_pivot_table_filtered = df.loc[
    (df[(0, 'high')] < df[(0, 'medium')]) | (
            df[(1, 'low')] < df[(1, 'high')])]

# Printing the required first pivot table as a DataFrame and as a dictionary

print("The dataframe and dictionary form for the first required pivot table:")
print(first_pivot_table_filtered)
print(first_pivot_table_filtered.to_dict())

"""Creating the second pivot table"""

# Creating the data frame with the required columns

required_columns_df = total_data_df[
    ['last_evaluation', 'satisfaction_level', 'time_spend_company',
     'promotion_last_5years']]


# Creating the function that will create the three DataFrames to be merged


def create_df(df: pd.DataFrame, action: str) -> pd.DataFrame:
    """Return a pd.DataFrame with the required format where data have been
    aggregated using the <action> function during pivoting from the originally
    provided <df> == <required_columns_df>.

    The outputted pd.DataFrame has the required column level names in the right
    order.

    """
    last_evaluation_max = df.pivot_table(index='time_spend_company',
                                         columns='promotion_last_5years',
                                         values='last_evaluation',
                                         aggfunc=action)
    last_evaluation_max.columns.name = None
    last_evaluation_max.index.name = None

    satisfaction_level_max = df.pivot_table(index='time_spend_company',
                                            columns='promotion_last_5years',
                                            values='satisfaction_level',
                                            aggfunc=action)
    satisfaction_level_max.columns.name = None
    satisfaction_level_max.index.name = None

    concat_df = pd.concat([last_evaluation_max,
                           satisfaction_level_max],
                          axis=1,
                          keys=['last-evaluation', 'satisfaction_level'])

    concat_df.columns = pd.MultiIndex.from_product([[action],
                                                    ['last_evaluation',
                                                     'satisfaction_level'],
                                                    [0, 1]])

    return concat_df


# Creating the three pivot tables to be merged

max_df = create_df(required_columns_df, 'max')
mean_df = create_df(required_columns_df, 'mean')
min_df = create_df(required_columns_df, 'min')

# Merging the three pivot tables

second_pivot_table = max_df.merge(mean_df,
                                  left_index=True,
                                  right_index=True).merge(min_df,
                                                          left_index=True,
                                                          right_index=True)

# Creating a temporary short name for <second_pivot_table> for readability,
# then filtering the merged pivot table according to the requirements
df = second_pivot_table
second_pivot_table = df.loc[(df[('mean', 'last_evaluation', 0)] > df[
    ('mean', 'last_evaluation', 1)])]

# Printing the required first pivot table as a DataFrame and as a dictionary

print("The dataframe and dictionary form for the second required pivot table:")
print(second_pivot_table)
print(second_pivot_table.to_dict())
