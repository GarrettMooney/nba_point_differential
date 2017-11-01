# libraries ----
library(tidyverse)
`%<>%` <- magrittr::`%<>%`

# connect to db ----
library(odbc)
library(DBI)
library(yaml)
cs <- yaml.load_file("nba.yaml")
con <- dbConnect(
  odbc::odbc(),
  .connection_string = paste0(cs$driver, cs$server, cs$port, cs$database, cs$uid, cs$pwd))

# window function ----
result <- dbSendQuery(con,
  "SELECT team, date, sum(diff) OVER (PARTITION BY team ORDER BY date) AS cum_diff
  from box_score
  order by team, date")
df <- dbFetch(result)
df %<>% mutate(cum_diff = as.integer(cum_diff))

# plot ----
# TODO: customize
p <- ggplot(df, aes(as.numeric(date), cum_diff, colour = team)) +
  geom_line() +
  geom_point()
ggsave("nba_point_differential.pdf", p)

# disconnect from db ----
dbDisconnect(con)
