# Data warehouses using MySQL and SQLite: Publications
This project involves extracting data from an XML document and storing it relationally in a SQLite database. The data is then used to create an analytical database using a star schema in MySQL. The project connects to two different databases simultaneously, which is a common practice for data warehouses. 

## Task Summary 
### Part 1: Load XML Data into Database
1. Inspect the XML file and design a normalized relational schema with entities/tables for articles, journals, and authors.
2. Implement the relational schema in SQLite using R.
3. Extract and transform data from the XML file, and load it into the appropriate tables in the database.

### Part 2: Create Star/Snowflake Schema
1. Set up a MySQL database and establish a connection.
2. Create and populate a star schema for author facts, including the author's ID, name, number of articles, and total number of co-authors.
3. Create and populate a star schema for journal facts, including the journal name, number of articles per year, quarter, and month.

### Part 3: Explore and Mine Data
1. Write a report using markdown to present the results of analytical queries on the MySQL data warehouse.
2. Perform queries to find the top ten authors with the most publications and the top journal with the most articles per year.

## Key Learnings
1. Extracting and transforming data from XML documents.
2. Designing and implementing a normalized relational schema.
3. Working with SQLite and MySQL databases.
4. Creating star schemas (and fact tables) for analytical purposes.
5. Writing efficient SQL queries for data exploration and analysis (including hashes).
6. Connecting multiple databases to a single data warehouse. 

## How to run
This project includes 3 R notebooks, one for each part. After getting the XML data [here](https://s3.us-east-2.amazonaws.com/artificium.us/lessons/06.r/l-6-183-extractxml-data-in-r/pubmed-xml-tfm/pubmed22n0001-tf.xml), running `LoadXML2DB.R` (R script) will preform step 1, running `LoadDataWarehouse.R` (R script) will preform step 2 (need a MySQL db called `starSchemaAuthor`), and running `AnalyzeData.Rmd` will preform part 3. Also see `AnalyzeData.Report.pdf` for a brief summary report of part 3.
