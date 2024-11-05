# Wind_Turbines
I first looked at the source files to see if it makes any sense to me or not and found the following:
  Each file contains 5 turbines, each turbine has exactly 31 days of records all in March 2024, which explains why each file contains 5*31*24 = 3720 number of records.
  Although the requirement does say "the system is known to sometimes miss entries due to sensor malfunctions.", not sure if that means records could be missing or just the values of the records can be NULL.
  Either way I'll be asking the client if they want to simply reject the next updated file if that happens or just continue with the process, and if so do they want me to fill wind_speed and power_output as 0 or something else.
  Furthermore the wind direction column is always between 0 and 359, which makes sense as it must be within 360 degrees
  Finally I do not see any text or NULLs which is perfect for when I apply the data file to a schema which is ideal in this situation
With all the above findings I'll then aim to create data quality checks later down the line
