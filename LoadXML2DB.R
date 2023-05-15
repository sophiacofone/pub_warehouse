## author: "Sophia Cofone"
library(XML)
library(RSQLite)
library(DBI)
library(hash)

### Loading XML file ###
xmlFile <- "pubmed-tfm-xml/pubmed22n0001-tf.xml"
# Reading the XML file and parse into DOM
xmlDOM <- xmlParse(file = xmlFile)
# get the root node of the DOM tree
r <- xmlRoot(xmlDOM)
numArt <- xmlSize(r)
# get number of children of root
numArt <- xmlSize(r)

### create data frames to hold data ###
Articles.df <- data.frame (arid = vector (mode = "integer", length = numArt),
                           article_title = vector (mode = "character", length = numArt),
                           pid = vector (mode = "integer", length = numArt),
                           ar_au_id = vector (mode = "integer", length = numArt),
                           stringsAsFactors = F)

Authors.df <- data.frame (auid = integer(),
                          last_name = character(),
                          fore_name = character(),
                          initials = character(),
                          stringsAsFactors = F)

Journals.df <- data.frame (pid = integer(),
                           issn = character(),
                           journal_title = character(),
                           year = integer(),
                           month = integer(),
                           day = integer(),
                           stringsAsFactors = F)

Article_Authors.df <- data.frame (ar_au_id = integer(),
                                  arid = integer(),
                                  auid = integer(),
                                  stringsAsFactors = F)

### create hash tables to speed up the "search" part of the parsing algorithm for Authors and Journals ###
Authors.hash <- hash()
Journals.hash <- hash()

### helper functions for parsing Authors and Journals ###
parseJournal <- function(aJournalNode) {
  # parse the journal into its components
  issn <- xmlValue(aJournalNode[[1]])
  journal_title <- xmlValue(aJournalNode[[3]])
  
  # setting defaults, I decided to use 1 as the default for day
  # it seems reasonable that the day isn't as important as the month and year for the questions to come
  # 0s will be changed to N/A later, but it was more useful to have 0 as a placeholder for the search (searching the journal hash for duplicates)
  year <- 0
  month <- 0
  day <- 1
  
  # if there is something in the pubdate node
  if (xmlSize(aJournalNode[[2]][[3]]) > 0) {
    # get the children of the pubdate node
    medline_date_node_list <- xmlChildren(aJournalNode[[2]][[3]])
    # for all the children
    for (date_node in medline_date_node_list) {
      # get the name of the node and the value
      node_name <- xmlName(date_node)
      node_value <- xmlValue(date_node)
      # if the node is the MedlineDate format, parse into y,m,d
      # otherwise, parse into y,m,d
      if (node_name == "MedlineDate") {
        year <- substr(node_value, 1, 4)
        month <- strsplit(node_value, " ")[[1]][2]
        day <- 0
      } else if (node_name == "Year") {
        year <- node_value
      } else if (node_name == "Month") {
        month <- node_value
      } else if (node_name == "Day") {
        day <- node_value
      }
    }
  }
  
  # give back the parsed row
  hash_key <- paste(issn, journal_title, year, month, day, sep = "|")
  return(hash_key)
}

parseAuthor <- function (aAuthorNode)
{
  # parse the date into its components
  last_name <- xmlValue(aAuthorNode[[1]])
  fore_name <- xmlValue(aAuthorNode[[2]])
  initials <- xmlValue(aAuthorNode[[3]])
  
  # give back the parsed row
  hash_key <- paste(last_name, fore_name, initials, sep = "|")
  return(hash_key)
}

rowExists <- function (aRow, aDF)
{
  # check if that address is already in the hash
  n <- nrow(aDF)
  c <- ncol(aDF)
  
  if (n == 0)
  {
    # hash is empty, so can't exist
    return(0)
  }
  
  for (a in 1:n)
  {
    # check if all columns match for a row; ignore the ID column
    if (all(aDF[a,] == aRow[1,]))
    {
      # found a match; return it's ID
      return(a)
    }
  }
  
  # none matched
  return(0)
}

### main parsing code ###
# iterate over the first-level child elements off the root the <Article> elements
for (i in 1:numArt)
{
  # get next article node
  aArt <- r[[i]]
  
  # get the Article ID attributes
  a <- xmlAttrs(aArt)
  
  # we making sure that the PMID is a number
  artNum <- as.numeric(a[1])
  
  # adding arid to Articles DF (PK)
  Articles.df$arid[i] <- artNum
  # adding article titles to Articles DF
  article_title <- xmlValue(aArt[[1]][[2]])
  Articles.df$article_title[i] <- article_title
  
  # parse journal
  journal_key <- parseJournal(aArt[[1]][[1]])
  
  # check if journal already exists in the hash, and add to the dataframe if it doesn't
  if (is.null(Journals.hash[[journal_key]])) {
    # assigning unique id
    pk.Journal <- length(Journals.hash) + 1
    Journals.hash[[journal_key]] <- pk.Journal
    # adding row to DF
    Journals.df <- rbind(Journals.df, data.frame(pid=pk.Journal, journal_key, stringsAsFactors=FALSE))
  } else {
    # getting the unique id if the row already exists (this is useful for the FK)
    pk.Journal <- Journals.hash[[journal_key]]
  }
  
  #set FK in articles DF to the journal 
  # (in either case, if the row is new or old we can get the right PK for the articles DF)
  Articles.df$pid[i] <- pk.Journal
  
  # parse author
  # same idea here, we are looking at the children in the authorlist node, 
  # seeing if the author already exists in our hash, and acting accordingly
  authorListNode <- aArt[[1]][[3]]
  author.dfs <- list()
  
  if (!is.null(authorListNode)) {
    for (authorNode in xmlChildren(authorListNode)) {
      author_key <- parseAuthor(authorNode)
      
      if (is.null(Authors.hash[[author_key]])) {
        pk.Author <- length(Authors.hash) + 1
        Authors.hash[[author_key]] <- pk.Author
        Authors.df <- rbind(Authors.df, data.frame(auid=pk.Author, author_key, stringsAsFactors=FALSE))
      } else {
        pk.Author <- Authors.hash[[author_key]]
      }
      
      # creating the junction table
      # this will run for the same number of times that there are children in the author list
      # so the junction table will be the same arid, but with however many auids there are
      # create synthetic PK
      pk.Article_Authors <- nrow(Article_Authors.df) + 1
      # get author ID (FK)
      auid <- pk.Author
      # get article ID (FK)
      arid <- artNum
      # add to Article_Author table
      Article_Authors.df[pk.Article_Authors,1] <- pk.Article_Authors
      Article_Authors.df[pk.Article_Authors,2] <- arid
      Article_Authors.df[pk.Article_Authors,3] <- auid
      
    }
  }
}

### cleaning up the Authors and Journals DFs ###
Authors.df <- cbind(Authors.df, do.call(rbind, strsplit(as.character(Authors.df$author_key), "|", fixed = TRUE)))
colnames(Authors.df) <- c("auid", "author_key", "last_name", "fore_name", "initials")
Authors.df <- Authors.df[, -2]

Journals.df <- cbind(Journals.df, do.call(rbind, strsplit(as.character(Journals.df$journal_key), "|", fixed = TRUE)))
colnames(Journals.df) <- c("pid", "journal_key", "issn", "journal_title", "year", "month", "day")
Journals.df <- Journals.df[, -2]

#fixing dates in Journals.df
# replace 0 values with NA
Journals.df$year[Journals.df$year == 0] <- NA
Journals.df$month[Journals.df$month == 0] <- NA
# convert year and day columns to numeric
Journals.df$year <- as.numeric(Journals.df$year)
Journals.df$day <- as.numeric(Journals.df$day)
# create a new date column in YYYY-MM-DD format
Journals.df$date <- with(Journals.df, ifelse(is.na(year) | is.na(month),NA,sprintf("%04d-%02d-%02d", year, match(month, month.abb), day)))
# remove the year, month, and day columns
Journals.df <- Journals.df[, -c(4:6)]

### Setting up DB ###
fpath = ""
dbfile = "pubmed.db"

# if database file already exists, we connect to it, otherwise we create a new database
dbcon <- dbConnect(RSQLite::SQLite(), paste0(fpath,dbfile), readonly = FALSE)

### creating tables ###
sql_create_table <- "CREATE TABLE Journals(
  pid TEXT NOT NULL,
  journal_title TEXT,
  date DATE,
  PRIMARY KEY (pid)
)"
dbExecute(dbcon, sql_create_table)

sql_create_table <- "CREATE TABLE Authors(
  auid NUMBER NOT NULL,
  last_name TEXT,
  fore_name TEXT,
  initials TEXT,
  PRIMARY KEY (auid)
)"
dbExecute(dbcon, sql_create_table)

sql_create_table <- "CREATE TABLE Articles(
  arid NUMBER NOT NULL,
  title TEXT,
  pid NUMBER NOT NULL,
  PRIMARY KEY (arid),
  FOREIGN KEY(pid) REFERENCES Journals(pid)
)"
dbExecute(dbcon, sql_create_table)

sql_create_table <- "CREATE TABLE Article_Author(
  ar_au_id NUMBER NOT NULL,
  arid TEXT NOT NULL,
  auid TEXT NOT NULL,
  PRIMARY KEY (ar_au_id),
  FOREIGN KEY(arid) REFERENCES Articles(arid),
  FOREIGN KEY(auid) REFERENCES Authors(auid)
)"
dbExecute(dbcon, sql_create_table)

### writing to tables using DFs ###
dbWriteTable(dbcon, "Journals", Journals.df, overwrite = T)
dbWriteTable(dbcon, "Authors", Authors.df, overwrite = T)
dbWriteTable(dbcon, "Articles", Articles.df, overwrite = T)
dbWriteTable(dbcon, "Article_Author", Article_Authors.df, overwrite = T)

### disconnecting from DB and completion message ###
dbDisconnect(dbcon)
print("done")