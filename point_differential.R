# get the most up to date cumulative point differentials
suppressPackageStartupMessages(library(odbc))
suppressPackageStartupMessages(library(DBI))
suppressPackageStartupMessages(library(yaml))

cs <- yaml.load_file('nba.yaml')
con <- dbConnect(
  odbc::odbc(),
  .connection_string = paste0(cs$driver, cs$server, cs$port, cs$database, cs$uid, cs$pwd))

dbFetch(
  dbSendQuery(
    con,
    "SELECT distinct d.team, d.date, w.cum_diff 
    FROM (SELECT DISTINCT ON (team) team, date
          FROM box_score
          ORDER BY team, date DESC) d
    INNER JOIN (SELECT team, date, sum(diff) OVER (PARTITION BY team ORDER BY date) AS cum_diff 
           FROM box_score) w ON w.date = d.date 
    AND w.team = d.team
    ORDER BY cum_diff desc, date desc;"
  ))

suppressWarnings(dbDisconnect(con))
