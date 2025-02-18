# Wind_Turbines
I first looked at the source files to see if it makes any sense to me or not and found the following:
  Each file contains 5 turbines, each turbine has exactly 31 days of records all in March 2024, which explains why each file contains 5*31*24 = 3720 number of records.
  Although the requirement does say "the system is known to sometimes miss entries due to sensor malfunctions.", not sure if that means records could be missing or just the values of the records can be NULL.  I have assumed the latter which I have handled using the apply_schema function to coalesce text fields with "Unknown" and numbers with "0".
  I have put in a data quality check in the DLT to ensure wind direction is always between 0 and 359, where the pipeline would fail if this is not satisfied.
  Finally I do not see any text or NULLs which is perfect for when I apply the data file to a schema which is ideal in this situation

I have assumed that the number of files and filenames will remain the same if the CSVs are appended only.
Unfortunately this means autoloader will not work, because it will think it has already processed that file.  However full load in DLT will be sufficient enough to reprocess the files agin.
If over time this becomes inefficient, an alternative approach would be to build a history of the files and use a merge statement to process only new records.
