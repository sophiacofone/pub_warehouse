## author: "Sophia Cofone"
### MySQL setup ###
# 1. Library
require(RMySQL)
library(sqldf)

# 2. Settings
# db_user <- 'root'
# db_password <- 'password'
# db_host <- 'localhost'
# db_port <- 3306

# 3. Initial connection
dbcon_mysql <-  dbConnect(MySQL(), user = db_user, password = db_password,
                          host = db_host, port = db_port, local_infile=TRUE)

dbSendQuery(dbcon_mysql, "set global local_infile=true;")

### 4. Create starSchemaAuthor ###
dbSendQuery(dbcon_mysql, "CREATE SCHEMA IF NOT EXISTS starSchemaAuthor;")
dbSendQuery(dbcon_mysql, "USE starSchemaAuthor;")

# 5. Update Settings/connection
db_name <- 'starSchemaAuthor'
dbcon_mysql <-  dbConnect(MySQL(), user = db_user, password = db_password,
                          dbname = db_name, host = db_host, port = db_port, local_infile=TRUE)

### SQLite ###
library(RSQLite)
library(DBI)

# setting up DB
fpath = ""
dbfile = "pubmed.db"

# if database file already exists, we connect to it, otherwise
# we create a new database (we already made it in the last step so just connecting)
dbcon_sqlite <- dbConnect(RSQLite::SQLite(), paste0(fpath,dbfile), readonly = FALSE)

### AuthorDim ###
# select auid from the Authors and rename authordim_key (SQLite)
# select last_name,fore_name,initials from Authors (SQLite)
sqlite_query <- "SELECT auid AS authordim_key,last_name,fore_name,initials FROM Authors"
sqlite_result <- dbGetQuery(dbcon_sqlite, sqlite_query)

# create the AuthorDim table in the MySQL database
mysql_query <- "CREATE TABLE starSchemaAuthor.AuthorDim (
                  authordim_key INT,
                  last_name VARCHAR(255),
                  fore_name VARCHAR(255),
                  initials VARCHAR(255)
                )"

dbExecute(dbcon_mysql, mysql_query)

# populate the AuthorDim table with data from SQLite
dbWriteTable(dbcon_mysql, "AuthorDim", sqlite_result, row.names = FALSE, overwrite = TRUE)

### Article_AuthorDim ###
# select ar_au_id from the Authors and rename ar_au_iddim_key (SQLite)
# select arid,auid from Authors (SQLite)
sqlite_query <- "SELECT ar_au_id AS ar_au_iddim_key,arid,auid FROM Article_Author"
sqlite_result <- dbGetQuery(dbcon_sqlite, sqlite_query)

# create the Article_AuthorDim table in the MySQL database
mysql_query <- "CREATE TABLE starSchemaAuthor.Article_AuthorDim (
                  ar_au_iddim_key INT,
                  arid INT,
                  auid INT
                )"

dbExecute(dbcon_mysql, mysql_query)

# populate Article_AuthorDim with data from SQLite
dbWriteTable(dbcon_mysql, "Article_AuthorDim", sqlite_result, row.names = FALSE, overwrite = T)

### AuthorFact table ###
# create the AuthorFact table in the MySQL database
mysql_query <- "CREATE TABLE starSchemaAuthor.AuthorFact (
                  authordim_key INT,
                  last_name VARCHAR(255),
                  fore_name VARCHAR(255),
                  initials VARCHAR(255),
                  articles_author INT,
                  co_authors INT
                )"

dbExecute(dbcon_mysql, mysql_query)

# populating AuthorFact table 
# using a temporary table to store all the information ended up being a lot more
# efficient than inserting from one of the Dim tables, then updating with the info
# from the other Dim table
# query first identifies the selected cols from the joins (authordim_key, last_name, fore_name, initials)
# COUNT used to get the total number of distinct arids from the Article_AuthorDim table
# SUM used to get the total number of co-authors. -1 is used to exclude the author from the count
# Joining the Article_AuthorDim and the AuthorDim by auid
# sub query counts the number of authors for each article in the Article_AuthorDim (grouped by arid)
# grouping by the selected columns
mysql_query <- "
CREATE TEMPORARY TABLE tmpAuthorFacts AS
SELECT
  ad.authordim_key,
  ad.last_name,
  ad.fore_name,
  ad.initials,
  COUNT(DISTINCT aad.arid) AS articles_author,
  SUM(articles_in_arid - 1) AS co_authors
FROM
  AuthorDim AS ad
  INNER JOIN Article_AuthorDim AS aad ON ad.authordim_key = aad.auid
  INNER JOIN (
    SELECT arid, COUNT(*) AS articles_in_arid
    FROM Article_AuthorDim
    GROUP BY arid
  ) AS arid_count ON aad.arid = arid_count.arid
GROUP BY
  ad.authordim_key, ad.last_name, ad.fore_name, ad.initials
"

dbExecute(dbcon_mysql, mysql_query)

# now we can easily insert into AuthorFact
mysql_query <- "
INSERT INTO AuthorFact (authordim_key, last_name, fore_name, initials, articles_author, co_authors)
SELECT
  authordim_key,
  last_name,
  fore_name,
  initials,
  articles_author,
  co_authors
FROM
  tmpAuthorFacts
"

dbExecute(dbcon_mysql, mysql_query)

mysql_query <- "DROP TEMPORARY TABLE tmpAuthorFacts"
dbExecute(dbcon_mysql, mysql_query)

### 4. Create starSchemaJournal ###
dbSendQuery(dbcon_mysql, "CREATE SCHEMA IF NOT EXISTS starSchemaJournal;")
dbSendQuery(dbcon_mysql, "USE starSchemaJournal;")

# 5. Update Settings/connection
db_name <- 'starSchemaJournal'

# update connection
dbcon_mysql <-  dbConnect(MySQL(), user = db_user, password = db_password,
                          dbname = db_name, host = db_host, port = db_port, local_infile=TRUE)

# select pid from Journals and rename piddim_keyj (SQLite)
# select journal_title,date from Journals (SQLite)
sqlite_query <- "SELECT pid AS piddim_keyj,journal_title,date FROM Journals"
sqlite_result <- dbGetQuery(dbcon_sqlite, sqlite_query)

# create the JournalDim table in the MySQL database
mysql_query <- "CREATE TABLE starSchemaJournal.JournalDim (
                  piddim_keyj INT,
                  journal_title VARCHAR(255),
                  date DATE
                )"

dbExecute(dbcon_mysql, mysql_query)

# populate the JournalDim table with data from SQLite
dbWriteTable(dbcon_mysql, "JournalDim", sqlite_result, row.names = FALSE, overwrite = TRUE)

# select pid from Articles and rename piddim_keya (SQLite)
sqlite_query <- "SELECT pid AS piddim_keya FROM Articles"
sqlite_result <- dbGetQuery(dbcon_sqlite, sqlite_query)

# create the ArticleDim table in the MySQL database
mysql_query <- "CREATE TABLE starSchemaJournal.ArticleDim (
                  piddim_keya INT
                )"

dbExecute(dbcon_mysql, mysql_query)

# populate the ArticleDim table with data from SQLite
dbWriteTable(dbcon_mysql, "ArticleDim", sqlite_result, row.names = FALSE, overwrite = TRUE)

### JournalFact table ###
# create the JournalFact table in the MySQL database
mysql_query <- "CREATE TABLE starSchemaJournal.JournalFact (
                  journal_title VARCHAR(255),
                  num_a_year INT,
                  num_a_quarter INT,
                  num_a_month INT
                )"

dbExecute(dbcon_mysql, mysql_query)

# similar approach for the same reasons above
# query first identifies the selected cols from the joins (piddim_keyj,journal_title, year, quarter, month)
# COUNT used to get the total number of articles from each journal 
# join the ArticleDim and JournalDim by pid keys
# the next 3 joins work the same, get the distinct journal ids and year/quarter/month but only if the date is not NA
# grouping by the selected columns
mysql_query <- "
CREATE TEMPORARY TABLE tmpArticleFacts AS
SELECT
  j.piddim_keyj,
  j.journal_title,
  yr.year,
  qu.quarter,
  mo.month,
  COUNT(a.piddim_keya) AS article_count
FROM
  JournalDim AS j
  INNER JOIN ArticleDim AS a ON j.piddim_keyj = a.piddim_keya
  INNER JOIN (SELECT DISTINCT piddim_keyj, CASE WHEN date NOT LIKE '%NA%' THEN YEAR(date) ELSE NULL END AS year FROM JournalDim) AS yr ON j.piddim_keyj = yr.piddim_keyj
  INNER JOIN (SELECT DISTINCT piddim_keyj, CASE WHEN date NOT LIKE '%NA%' THEN QUARTER(date) ELSE NULL END AS quarter FROM JournalDim) AS qu ON j.piddim_keyj = qu.piddim_keyj
  INNER JOIN (SELECT DISTINCT piddim_keyj, CASE WHEN date NOT LIKE '%NA%' THEN MONTH(date) ELSE NULL END AS month FROM JournalDim) AS mo ON j.piddim_keyj = mo.piddim_keyj
GROUP BY
  j.piddim_keyj, j.journal_title, yr.year, qu.quarter, mo.month
"
dbExecute(dbcon_mysql, mysql_query)

# now we can insert into JournalFact
# AVG is used to make 1 value per fact row
# please note the result of this will be a table where it appears there are "repeating" Journal titles, 
# this is because those titles actually had different dates/pids
# I will account for this in the report in part 3, as it may be useful to retain the journal fact at the publication date level
mysql_query <- "
INSERT INTO starSchemaJournal.JournalFact (journal_title, num_a_year, num_a_quarter, num_a_month)
SELECT
  journal_title,
  AVG(CASE WHEN year IS NOT NULL THEN article_count END) AS num_a_year,
  AVG(CASE WHEN quarter IS NOT NULL THEN article_count END) AS num_a_quarter,
  AVG(CASE WHEN month IS NOT NULL THEN article_count END) AS num_a_month
FROM
  tmpArticleFacts
GROUP BY
  piddim_keyj, journal_title
"

dbExecute(dbcon_mysql, mysql_query)

mysql_query <- "DROP TEMPORARY TABLE tmpArticleFacts"
dbExecute(dbcon_mysql, mysql_query)

dbDisconnect(dbcon_mysql)
dbDisconnect(dbcon_sqlite)

print("done")