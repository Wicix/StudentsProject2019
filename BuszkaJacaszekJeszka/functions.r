library(jsonlite)
library(lubridate)
library(dplyr)
library(tidyr)

##returns dataframe with data from spotify
streaming_history_df <- function(folder_path){
  file_paths<- list.files(folder_path,"StreamingHistory")
  read_files <- lapply(file_paths, jsonlite::fromJSON)
  bind_rows(read_files)
}



##returns streaming history with end time changed to date
#mutate(streaming_history, end_time = ymd_hm(end_time))

##returns streaming_history with end_time column changed from character to date
#mutate(streaming_history, s_played = dmilliseconds((s_played)))

names_change <- function(streaming_history, column_names = c("end_time", "artist_name", "track_name", "s_played")){
  names(streaming_history) <- column_names
  streaming_history
}
##returns streaming_history with added start_time column using end_time and ms_played
add_start_time <- function(streaming_history){
  
  start_time <- streaming_history[["end_time"]] - streaming_history[["s_played"]]
  streaming_history <- cbind(streaming_history, start_time)
  streaming_history
  
}

##returns streaming history with added "skipped" column [true or false]
add_skipped <- function(streaming_history){
  skipped <- (streaming_history[["s_played"]] < duration(10, "seconds"))
  streaming_history <- cbind(streaming_history, skipped)
}

##returns streaming history with added weekdays column
add_weekday <- function(streaming_history){
  weekday <- wday(streaming_history[["start_time"]], label = TRUE)
  streaming_history <- cbind(streaming_history, weekday)
}

#### creating and preparing dataframe
streaming_history_complete <- function(folder_path){
  streaming_history_df(folder_path) %>%
  names_change() %>% 
  mutate(end_time = ymd_hm(end_time)) %>% 
  mutate(s_played = dmilliseconds((s_played))) %>%
  add_start_time() %>%
  add_skipped() %>%
  add_weekday()
}
### creating dataframe with date, devices and country 
search_queries_df <- function(folder_path){
  file_path <-list.files(folder_path,"SearchQueries")
  select(jsonlite::fromJSON(file_path),1:3) %>%
  mutate(date = ymd(date))
}



### creating dataframe with names of playlists, lists with character of song names, and artist"
playlist_df_function <- function(folder_path) {
  song_names_function <- function(x)
    return(df[[2]][[x]][[1]][[1]])
  
  artist_names_function <- function(x)
    return(df[[2]][[x]][[1]][[2]])
  
  file_path <- list.files(folder_path, "Playlist")
  df <- jsonlite::fromJSON(file_path)
  df <- select(df[[1]], name, items)
  df <-
    transmute(
      df,
      name,
      "song_names" = sapply(1:nrow(df), song_names_function),
      "artist_names" = sapply(1:nrow(df), artist_names_function)
    )
}

# creating dataframe similar to Streaming_History_Complete, but this one has additional 
#column with lists of playlists that including that song
str_his_with_playlists <- function(folder_path) {
  playlist_df <- playlist_df_function(folder_path)
  str_his_comp <- streaming_history_complete(folder_path)
  in_which_playlists <- function(song_row) {
    in_playlist <- function(playlist_row) {
      position_in_playlist <- function(position_number) {
        if (((str_his_comp[song_row, 2] == playlist_df[playlist_row, 3][[1]][position_number])) &
            (str_his_comp[song_row, 3] == playlist_df[playlist_row, 2][[1]][position_number]))
          
          return(playlist_df[playlist_row, 1])
      }
      unlist(unique(lapply(
        1:length(playlist_df[playlist_row, 3][[1]]), position_in_playlist
      )))
    }
    if (is.null(unlist(sapply(1:length(playlist_df[, 1]), in_playlist))))
      return("It is not in any playlist")
    unlist(sapply(1:length(playlist_df[, 1]), in_playlist))
    
    
  }
  mutate(str_his_comp, "In Playlist" = lapply(1:nrow(str_his_comp), in_which_playlists))
  
}

how_many_skipped <- function(streaming_history, start_date, end_date, as_percentage = FALSE){
  filtered <- filter(streaming_history, start_time >= ymd(start_date), start_time <= ymd(end_date), skipped == TRUE)  
  if(as_percentage) {
    return (paste(round((nrow(filtered)/nrow(streaming_history)) * 100, digits = 3), "%", sep = ""))
  }
  
  return(nrow(filtered))
}

#how long you listened to spotify in given time period, as a duration or as a percentage 
how_long_listened <- function(streaming_history, start_date, end_date, as_percentage = FALSE){
  filtered <- filter(streaming_history, start_time >= ymd(start_date), start_time <= ymd(end_date))
  suma <- sum(filtered[["s_played"]])
  seconds_in_period <-as.numeric(difftime(end_date,start_date, units = "secs"))
  if (as_percentage) return(paste(round(suma/seconds_in_period * 100, digits = 3), "%", sep = ""))
  return (as.duration(suma))
}

#which songs were played the most times in given time period
most_played_track <- function(streaming_history, start_date, end_date, how_many = 10){
  df <-filter(streaming_history, start_time >= ymd(start_date), start_time <= ymd(end_date), skipped == FALSE) %>% 
    group_by(track_name) %>% 
    summarise(number = n())
  df <- df[order(-df[["number"]]),]
  df[1:how_many,]
}

#which songs were skipped the most times in given time period
most_skipped_track <- function(streaming_history, start_date, end_date, how_many = 10){
  df <-filter(streaming_history, start_time >= ymd(start_date), start_time <= ymd(end_date), skipped == TRUE) %>% 
    group_by(track_name) %>% 
    summarise(number = n())
  df <- df[order(-df[["number"]]),]
  df[1:how_many,]
}

#which artists were played the most times in given time period
most_played_artist <- function(streaming_history, start_date, end_date, how_many = 10){
  df <-filter(streaming_history, start_time >= ymd(start_date), start_time <= ymd(end_date), skipped == FALSE) %>% 
    group_by(artist_name) %>% 
    summarise(number = n())
  df <- df[order(-df[["number"]]),]
  df[1:how_many,]
}

#which atrists were skipped the most times in given time period
most_skipped_artist <- function(streaming_history, start_date, end_date, how_many = 10){
  df <-filter(streaming_history, start_time >= ymd(start_date), start_time <= ymd(end_date), skipped == TRUE) %>% 
    group_by(artist_name) %>% 
    summarise(number = n())
  df <- df[order(-df[["number"]]),]
  df[1:how_many,]
}


