library(ggplot2)
library(dplyr)
library(lubridate)
###functions to be used on streaming history

##visualizes number of songs played in given time period at different hours 
number_of_songs_listened_by_hour <- function(streaming_history, start_date, end_date, 
                                             by_weekday = FALSE, dont_show_skipped = TRUE){
  filtered <- filter(streaming_history, start_time >= ymd(start_date), start_time <= ymd(end_date)) 
  if (dont_show_skipped) filtered <- filter(filtered, skipped == FALSE)
  vis <- ggplot(filtered, aes(x = hour(start_time)))+
    geom_bar()+
    scale_x_discrete(limits = 0:24)+
    xlab("Hour")+
    ylab("Songs listened")
  
  if (by_weekday){
    vis <- vis+
      facet_wrap(~weekday)
  }
  vis
}

##visualizes number of songs listened by weekday in given time period
number_of_songs_listened_by_weekday <- function(streaming_history, start_date, end_date, dont_show_skipped = TRUE){
  filtered <- filter(streaming_history, start_time >= ymd(start_date), start_time <= ymd(end_date)) 
  if (dont_show_skipped) filtered <- filter(filtered, skipped == FALSE)
  vis <- ggplot(filtered, aes(x = weekday))+
    geom_bar()+
    xlab("Weekday")+
    theme(axis.text.x = element_text(angle = 270))+
    ylab("Songs listened")
  vis
}

##visualizes number of songs skipped in given time period, has two vesiorns - bar and point

number_of_skipped_songs <- function(streaming_history, start_date, end_date, by = "day", type = "bar"){
  filtered <- filter(streaming_history, start_time >= ymd(start_date), start_time <= ymd(end_date), skipped == TRUE) %>% 
    mutate(end_time = floor_date(end_time, by))
  vis <- ggplot(filtered, aes(x = end_time ))+
    ylab("Songs skipped")+
    xlab("Date")
  if (type == "point") vis <- vis + geom_point(stat = "count")
  if (type == "bar") vis <- vis + geom_bar()
  vis
} 

### function to be used on search Queries:

##
platform_used <- function(search_queries, start_date, end_date){
  filter(search_queries, date >= ymd(start_date), date <= ymd(end_date)) %>% 
    ggplot(aes(x = platform, fill = platform))+
    geom_bar(show.legend = FALSE)+
    xlab("Platform used")+
    ylab("How many searches")+
    
    
}

##
platform_used_by_date<- function(search_queries, start_date, end_date){
  filter(search_queries, date >= ymd(start_date), date <= ymd(end_date)) %>%
    ggplot(aes(x = date, color = platform))+
    geom_point(stat = "count")+
    xlab("Date")+
    ylab("How many searches")
}

##
country_by_date <- function(search_queries, start_date, end_date){
  filter(search_queries, date >= ymd(start_date), date <= ymd(end_date)) %>%
    ggplot(aes(x = date, color = country))+
    geom_point(stat = "count" )+
    xlab("Date")+
    ylab("How many searches")
}