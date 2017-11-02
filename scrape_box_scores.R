# libraries ----
library(tidyverse)
library(jsonlite)
`%<>%` <- magrittr::`%<>%`

# get data ----
yesterday <- Sys.Date() %>%
  { . - 1 } %>%
  format(., "%Y%m%d") %>%
  as.character

url <- glue::glue(
  "http://data.nba.com/json/cms/noseason/scoreboard/{yesterday}/games.json")
data <- jsonlite::fromJSON(url)
games <- data$sports_content$games
rm(data)

# reshape ----
box_score <- bind_cols(
  games$game$home[c("team_key", "score")] %>%
    rename(home = team_key, home_score = score),
  games$game$visitor[c("team_key", "score")] %>%
    rename(visitor = team_key, visitor_score = score)
) %>%
  select(home, visitor, home_score, visitor_score) %>%
  mutate_at(vars(ends_with("_score")), as.integer) %>%
  mutate(home_diff = home_score - visitor_score,
         visitor_diff = -home_diff)

box_score <- bind_rows(
  select(box_score, starts_with("home")) %>%
    rename(team = home, score = home_score, diff = home_diff),
  select(box_score, starts_with("visitor")) %>%
    rename(team = visitor, score = visitor_score, diff = visitor_diff)
) %>%
  mutate(date = yesterday)
rm(games)

# write to db ----
library(odbc)
library(DBI)
library(yaml)
cs <- yaml.load_file("nba.yaml")
con <- dbConnect(
  odbc::odbc(),
  .connection_string = paste0(cs$driver, cs$server, cs$port, cs$database, cs$uid, cs$pwd))

data <- dbWriteTable(con, "box_score", box_score, append = T)

# disconnect from db ----
dbDisconnect(con)
