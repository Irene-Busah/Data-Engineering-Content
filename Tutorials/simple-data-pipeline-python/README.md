# Getting Started with Data Pipelines for ETL
Data pipelines are everywhere! More than ever, data practitioners find themselves needing to extract, transform, and load data to power the work they do. During this code-along, I'll walk through the basics of building a data pipeline using Python, pandas, and sqlite.

Throughout the tutorial, I'll be using the "Google Play Store Apps" dataset, available in DataCamp Workspaces. The two datasets we'll be using is made available as .csv files, and will be transformed throughout the code-along before being loaded into a sqlite database.


# Extracting Data
Extracting data is almost always the first step when building a data pipelines. There are tons of shapes and sizes that data can be extracted from. Here are just a few:

- API's
- SFTP sites
- Relational databases
- NoSQL databases (columnar, document, key-value)
- Flat-files

In this code-along, we'll focus on extracting data from flat-files. A flat file might be something like a .csv or a .json file. The two files that we'll be extracting data from are the apps_data.csv and the review_data.csv file. To do this, we'll used pandas. Let's take a closer look!

* After importing pandas, read the apps_data.csv DataFrame into memory. Print the head of the DataFrame.
* Similar to before, read in the DataFrame stored in the review_data.csv file. Take a look at the first few rows of this DataFrame.
* Print the column names, shape, and data types of the apps DataFrame.


The code above works perfectly well, but this time let's try using DRY-principles to build a function to extract data.

- Create a function called extract, with a single parameter of name file_path.
- Sprint the number of rows and columns in the DataFrame, as well as the data type of each column. Provide instructions about how to use the value that will eventually be returned by this function.
- Return the variable data.
- Call the extract function twice, once passing in the apps_data.csv file path, and another time with the review_data.csv file path. Output the first few rows of the apps_data DataFrame.



# Transforming Data
We're interested in working with the apps and their corresponding reviews in the"FOOD_AND_DRINK" category. We'd like to do the following:

- Define a function with name transform. This function will have five parameters; apps, review, category, min_rating, and min_reviews.
- Drop duplicates from both DataFrames.
- For each of the apps in the desired category, find the number of positive reviews, and filter the columns.
- Join this back to the apps dataset, only keeping the following columns:
    - App
    - Rating
    - Reviews
    - Installs
    - Sentiment_Polarity
- Filter out all records that don't have at least the min_rating, and more than the min_reviews.
- Order by the rating and number of installs, both in descending order.
- Call the function for the "FOOD_AND_DRINK" category, with a minimum average rating of 4 stars, and at least 1000 reviews.

Alright, let's give it a shot!
