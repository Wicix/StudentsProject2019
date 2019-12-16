library(jsonlite)
library(lubridate)
library(dplyr)
library(tidyr)

##returns dataframe with data from spotify
Streaming_History_df <- function(folder_path){
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
Streaming_History_Complete <- function(folder_path){
  Streaming_History_df(folder_path) %>%
  names_change() %>% 
  mutate(end_time = ymd_hm(end_time)) %>% 
  mutate(s_played = dmilliseconds((s_played))) %>%
  add_start_time() %>%
  add_skipped() %>%
  add_weekday()
}
### creating dataframe with date, devices and country 
Search_Queries_df <- function(folder_path){
  file_path <-list.files(folder_path,"SearchQueries")
  select(jsonlite::fromJSON(file_path),1:3) %>%
  mutate(date = ymd(date))
}



### creating dataframe with names of playlists, string containing song names separated 
#by ";;;" and artist names separated by ";;;"
Playlist_df_function <- function(folder_path){
  file_path <-list.files(folder_path,"Playlist")
  a <- jsonlite::fromJSON(file_path)
  b <- select(a[[1]], name, items)
  num_of_rows <- dim(b)[1]
  vec_num_of_rows <- 1:num_of_rows
  df <- data.frame(b$name,NA, NA)
  colnames(df) <- c("Playlist names","Song names","Artist names")
  for (i in vec_num_of_rows){
    df[i,2] <- paste(b[i,2][[1]]$track$trackName, collapse = ";;;")
    df[i,3] <- paste(b[i,2][[1]]$track$artistName, collapse = ";;;")
  df[,1] <- as.character(df[,1])
  }
  
  return(df)
}

# creating dataframe similar to Streaming_History_Complete, but this one has additional 
#column with lists of playlists that including that song
Playlist_df_Str_his <- function(folder_path){
  Str_his_df <- Streaming_History_Complete(folder_path)
  Playlist_df <- Playlist_df_function(folder_path)
  In_which_playlist <- select(data.frame(1:dim(Str_his_df)[1],NA),2)
  colnames(In_which_playlist) <- c("In_playlists")
  for (i in 1:dim(Playlist_df)[1]){
    splited_song_names <- strsplit(Playlist_df[i,2],";;;")
    
    splited_artist_names <- strsplit(Playlist_df[i,3],";;;")
    
    for (j in 1:dim(Str_his_df)[1]){
      for(k in 1:length(splited_artist_names[[1]])){
        if(Str_his_df[j,2]==splited_artist_names[[1]][k] & Str_his_df[j,3]==splited_song_names[[1]][k] )
        {
         if(is.na(In_which_playlist[j,1]==TRUE)){
           In_which_playlist[j,1] <- Playlist_df[i,1]
           break
         }
          else {In_which_playlist[j,1]<- paste(In_which_playlist[j,1], ";",Playlist_df[i,1])
            break
          }
        }
     }
    }
  }
  strsplit_playlist <- function(dataframe){
    dataframe$In_playlists <- strsplit(dataframe$In_playlists,";")
    dataframe
  }
  cbind(Str_his_df,In_which_playlist) %>%
  strsplit_playlist
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
