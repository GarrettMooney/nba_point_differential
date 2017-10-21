# libraries ----
library(tidyverse)
library(jsonlite)
library(dbplyr)
`%<>%` <- magrittr::`%<>%`

# get data ----
today <- Sys.Date() %>% format(., "%Y%m%d") %>% as.character
yesterday <- today %>% as.integer() %>% {. - 1} %>% as.character

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

# write to postgresql ----
library(odbc)
library(DBI)
library(yaml)
cs <- yaml.load_file("nba.yaml")
con <- dbConnect(
  odbc::odbc(),
  .connection_string = paste0(cs$driver, cs$server, cs$port, cs$database, cs$uid, cs$pwd))

data <- dbWriteTable(con, "box_score", box_score, append = T)

# window function ----
result <- dbSendQuery(con, 
  "SELECT team, date, sum(diff) OVER (PARTITION BY team ORDER BY date) AS cum_diff
  from box_score
  order by team, date")
df <- dbFetch(result)
df %<>% mutate(cum_diff = as.integer(cum_diff))

# plot ----
p <- ggplot(df, aes(as.numeric(date), cum_diff, colour = team)) + 
  geom_line() + 
  geom_point() 
ggsave("nba_point_differential.pdf", p)

# disconnect from db ----
dbDisconnect(con)
