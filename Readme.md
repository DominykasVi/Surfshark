# Pipeline execution

This code reads data from https://www.ic3.gov/Media/PDF/AnnualReport/2016State/StateReport.aspx#?s=1 and saves it into parquet files.
To run the code, you can simply run the pipeline/pipeline.py file.

The pipeline takes into two arguments, year_range and state_range. 
To see the arguments you can use command line command python **pipeline/pipeline.py -h**

Example command line execution with arguments:

**python pipeline/pipeline.py --year_range 2016-2020 --state_range 1-10**

The default arguments are 2016-2021 and 1-58.
You can also run it with 2016 and 1 to get only one specific report.

The data is saved into annual_report folder, with stucture {year}/{state_number}/{table_name}.parquet
# Logging
The logs are created for each url (year/state) we query in the logs/{run_start}/run_{year}_{state}.log file. 

# Unit tests
I did not write extensive unit tests since this is just an example tasks and proper unit test creation takes time.
The tests are save in tests folder. To run unit tests, just execute the command below in the top folder.

**python -m unittest discover tests**   

# Decisions taken for the pipeline
Here I will explain some of the decisions taken during the creation of the pipeline
1. The pipeline has paralel execution wrapper, even though it is possible to run it in sequentially. But since sequantial execution took some time, I decided to implement paralel processing.
2. There is no modification of request headers, since this source does not limit requests.
3. Argument paring is written in the main pipeline code, since they are completely optional and the pipeline could function with arguments being hardcoded into.
4. Logging is only done at the top level, it would be possible to implement logging for each function, but I think this type of logging gives enough data for debugging/monitoring and does not generate a lot of extra code.
5. Logging is done into separate files, so that the paralel execution would work properly with logging.
6. I had the idea when saving date, to have structure {year}/{state_name}/{table_name}.parquet, which would be easier for some person to read, but decided against it. Because saving in numbers make it easier to read all files programatically, and we can just keep a reference which number means what state. Of course, if the states numbering was ever changed, it would make sense to save by name. I made a quick check, and state numbering does not seem to change. For a proper solution, some data quality test (to check if numbering has not changed) should be implemented.
7. Since descriptor data did not seem to fit in by crime type data, I still saved it, but it has descriptor flag set to 1, so that they would be easily filtered out from the dataset.
8. I wanted the numeric types to be usable (addition, subtraction, ect.) so I remove the dollar sign and save it into a different column, so that we know what kind of monetery value we are analyzing. The function works well enough for this dataset, but could be improved to be more fool-proof. In a working environment I would expect such functions to be used from some main library, not separately written for each solution. 