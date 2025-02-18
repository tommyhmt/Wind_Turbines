# Wind_Turbines
I first looked at the source files to see if it makes any sense to me or not and found the following:

Each file contains 5 turbines, each turbine has exactly 31 days of records all in March 2024, which explains why each file contains 5*31*24 = 3720 number of records.

Although the requirement says "the system is known to sometimes miss entries due to sensor malfunctions.", I'm not sure if that means records could be missing or just the values of the records can be NULL.  I have assumed the latter which I have handled using the apply_schema function to coalesce text fields with "Unknown" and numbers with "0".

If it is the former, then I have assumed that these missing records will be appended next time the file is updated.

I have put in a data quality check in the silver layer DLT to ensure wind direction is always between 0 and 359, where the pipeline would fail if this is not satisfied.
For the gold layer DLT I have put in a data quality check for anomaly is true, but that won't stop the process it's just to highlight there are anomalies.

I have also assumed that the number of files and filenames will remain the same if the CSVs are appended only.
Unfortunately this means autoloader will not work, because it will think it has already processed that file.  However full load in DLT will be sufficient enough to reprocess the files agin.
If over time this becomes inefficient, an alternative approach would be to build a history of the files and use a merge statement to process only new records.

There is only one parameter in the silver layer DLT called storageAccountUrl, to define where the files are stored in external storage.

I have built 2 separate DLT pipelines because a single DLT pipeline can only write to one schema, and I wanted to keep the two tables in separate schemas (cleansed and conformed)
![image](https://github.com/user-attachments/assets/a6a6f3a5-3d8b-491f-9628-cf4743957d4f)

![image](https://github.com/user-attachments/assets/9db4b911-b886-410f-a958-1b23bcb9fd3c)

Unity Catalog:

![image](https://github.com/user-attachments/assets/70b197bb-471c-474d-a0b3-1c80799364d4)

