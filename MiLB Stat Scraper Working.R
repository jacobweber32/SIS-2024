# Install required packages if not already installed
if (!requireNamespace("rvest", quietly = TRUE)) install.packages("rvest")
if (!requireNamespace("httr", quietly = TRUE)) install.packages("httr")
if (!requireNamespace("dplyr", quietly = TRUE)) install.packages("dplyr")
if (!requireNamespace("lubridate", quietly = TRUE)) install.packages("lubridate")
if (!requireNamespace("readr", quietly = TRUE)) install.packages("readr")

# Load required libraries
library(httr)
library(jsonlite)
library(dplyr)
library(lubridate)
library(readr)
library(purrr)

# Function to retrieve full boxscore data for a game
retrieve_full_boxscore <- function(date, level, game_id) {
  if (is.na(game_id) || game_id == "") {
    message("Error: Empty or invalid game ID for date ", date, " and level ", level)
    return(list(
      meta = list(
        date = date,
        level = level,
        game_id = game_id,
        status = "Invalid game ID"
      )
    ))
  }
  
  tryCatch({
    boxscore_url <- paste0("https://statsapi.mlb.com/api/v1/game/", game_id, "/boxscore")
    
    message("Retrieving boxscore for game:", game_id)
    message("URL:", boxscore_url)
    
    boxscore_response <- GET(boxscore_url)
    
    if (status_code(boxscore_response) == 200) {
      boxscore_content <- content(boxscore_response, "text")
      boxscore_data <- fromJSON(boxscore_content, simplifyVector = FALSE)
      
      # Add metadata to the boxscore data
      boxscore_data$meta <- list(
        date = date,
        level = level,
        game_id = game_id
      )
      
      message("Successfully retrieved boxscore for game", game_id)
      return(boxscore_data)
      
    } else {
      message("Failed to retrieve boxscore. Status code:", status_code(boxscore_response))
      return(list(
        meta = list(
          date = date,
          level = level,
          game_id = game_id,
          status = paste("Failed to retrieve boxscore. Status code:", status_code(boxscore_response))
        )
      ))
    }
    
  }, error = function(e) {
    message("Error processing game ", game_id, ": ", conditionMessage(e))
    return(list(
      meta = list(
        date = date,
        level = level,
        game_id = game_id,
        status = paste("Error:", conditionMessage(e))
      )
    ))
  })
}

# Function to extract player stats from a single boxscore
extract_player_stats <- function(boxscore) {
  meta <- boxscore$meta
  
  if (!is.null(meta$status)) {
    return(data.frame(
      date = meta$date,
      game_id = meta$game_id,
      level = meta$level,
      status = meta$status,
      stringsAsFactors = FALSE
    ))
  }
  
  extract_team_stats <- function(team_data, team_name) {
    players <- team_data$players
    
    map_df(players, function(player) {
      player_name <- player$person$fullName %||% NA
      position <- player$position$abbreviation %||% NA
      
      batting <- player$stats$batting %||% list()
      pitching <- player$stats$pitching %||% list()
      
      data.frame(
        date = meta$date,
        game_id = meta$game_id,
        level = meta$level,
        player_id = player$person$id,
        player_name = player_name,
        team = team_name,
        position = position,
        
        # Batting stats
        at_bats = batting$atBats %||% NA,
        runs = batting$runs %||% NA,
        hits = batting$hits %||% NA,
        doubles = batting$doubles %||% NA,
        triples = batting$triples %||% NA,
        home_runs = batting$homeRuns %||% NA,
        rbi = batting$rbi %||% NA,
        walks = batting$baseOnBalls %||% NA,
        strikeouts_batting = batting$strikeOuts %||% NA,
        stolen_bases = batting$stolenBases %||% NA,
        caught_stealing = batting$caughtStealing %||% NA,
        left_on_base = batting$leftOnBase %||% NA,
        ground_into_double_play = batting$groundIntoDoublePlay %||% NA,
        
        # Pitching stats
        innings_pitched = pitching$inningsPitched %||% NA,
        hits_allowed = pitching$hits %||% NA,
        runs_allowed = pitching$runs %||% NA,
        earned_runs = pitching$earnedRuns %||% NA,
        walks_allowed = pitching$baseOnBalls %||% NA,
        strikeouts_pitching = pitching$strikeOuts %||% NA,
        home_runs_allowed = pitching$homeRuns %||% NA,
        inherited_runners = pitching$inheritedRunners %||% NA,
        inherited_runners_scored = pitching$inheritedRunnersScored %||% NA,
        strike_percentage = (pitching$strikes %||% 0) / (pitching$pitches %||% 1) * 100,
        
        stringsAsFactors = FALSE
      )
    })
  }
  
  away_stats <- extract_team_stats(boxscore$teams$away, boxscore$teams$away$team$name)
  home_stats <- extract_team_stats(boxscore$teams$home, boxscore$teams$home$team$name)
  
  bind_rows(away_stats, home_stats)
}

# Read the CSV file
games <- read_csv("C:/Users/weber/OneDrive/Desktop/Baseball/SIS MiLB Assignments CSV.csv")

# Check if required columns are present
required_columns <- c("Date", "Level", "Game ID")
missing_columns <- setdiff(required_columns, names(games))
if (length(missing_columns) > 0) {
  stop("Error: The following required columns are missing from the CSV file: ", 
       paste(missing_columns, collapse = ", "))
}

# Check for missing or invalid Game IDs
invalid_games <- games %>% filter(is.na(`Game ID`) | `Game ID` == "")
if (nrow(invalid_games) > 0) {
  message("Warning: Found ", nrow(invalid_games), " games with missing or invalid Game IDs")
  print(invalid_games)
}

# Process all games
all_boxscores <- map(1:nrow(games), function(i) {
  game <- games[i, ]
  message("\nProcessing game", i, "of", nrow(games))
  Sys.sleep(1)  # Add a 1-second delay between requests to avoid rate limiting
  retrieve_full_boxscore(game$Date, game$Level, game$`Game ID`)
})

# Save the full boxscore data
saveRDS(all_boxscores, "milb_all_boxscores.rds")
message("\nAll boxscores saved to milb_all_boxscores.rds")

# Extract player stats from all boxscores
all_player_stats <- map_df(all_boxscores, extract_player_stats)

# Removing players that didn't put up any stats
all_player_stats <- all_player_stats %>%
  filter(!(is.na(at_bats) & position!= 'P')) %>%
  filter(!(position == 'P' & is.na(innings_pitched)))

# Save the extracted player stats to a CSV file
write_csv(all_player_stats, "milb_all_player_stats.csv")
message("\nAll player stats saved to milb_all_player_stats.csv")

# Aggregate player stats
aggregate_player_stats <- all_player_stats %>%
  group_by(player_id, player_name, level) %>%
  summarise(
    games_played = n_distinct(game_id),
    
    # Batting stats
    at_bats = sum(at_bats, na.rm = TRUE),
    runs = sum(runs, na.rm = TRUE),
    hits = sum(hits, na.rm = TRUE),
    doubles = sum(doubles, na.rm = TRUE),
    triples = sum(triples, na.rm = TRUE),
    home_runs = sum(home_runs, na.rm = TRUE),
    rbi = sum(rbi, na.rm = TRUE),
    walks = sum(walks, na.rm = TRUE),
    strikeouts_batting = sum(strikeouts_batting, na.rm = TRUE),
    stolen_bases = sum(stolen_bases, na.rm = TRUE),
    caught_stealing = sum(caught_stealing, na.rm = TRUE),
    left_on_base = sum(left_on_base, na.rm = TRUE),
    ground_into_double_play = sum(ground_into_double_play, na.rm = TRUE),
    
    # Pitching stats
    innings_pitched = sum(as.numeric(innings_pitched), na.rm = TRUE),
    hits_allowed = sum(hits_allowed, na.rm = TRUE),
    runs_allowed = sum(runs_allowed, na.rm = TRUE),
    earned_runs = sum(earned_runs, na.rm = TRUE),
    walks_allowed = sum(walks_allowed, na.rm = TRUE),
    strikeouts_pitching = sum(strikeouts_pitching, na.rm = TRUE),
    home_runs_allowed = sum(home_runs_allowed, na.rm = TRUE),
    inherited_runners = sum(inherited_runners, na.rm = TRUE),
    inherited_runners_scored = sum(inherited_runners_scored, na.rm = TRUE),
    
    # Calculate averages and percentages
    batting_average = hits / at_bats,
    on_base_percentage = (hits + walks) / (at_bats + walks),
    slugging_percentage = (hits + doubles + 2*triples + 3*home_runs) / at_bats,
    era = (earned_runs * 9) / innings_pitched,
    whip = (walks_allowed + hits_allowed) / innings_pitched,
    strike_percentage = mean(strike_percentage, na.rm = TRUE),
    
    .groups = 'drop'
  ) %>%
  mutate(
    ops = on_base_percentage + slugging_percentage,
    primary_position = case_when(
      innings_pitched > 0 ~ "Pitcher",
      TRUE ~ "Batter"
    )
  )

# Save the aggregated player stats to a CSV file
write_csv(aggregate_player_stats, "milb_aggregate_player_stats.csv")
message("\nAggregate player stats saved to milb_aggregate_player_stats.csv")
