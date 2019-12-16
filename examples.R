streaming_history <- Playlist_df_Str_his(".")
search_queries <- Search_Queries_df(".")


how_long_listened(streaming_history, "2019-01-21", "2019-02-24")
how_long_listened(streaming_history, "2019-01-21", "2019-02-24", as_percentage = TRUE)

most_played_track(streaming_history, "2018-10-21", "2019-02-24", how_many = 15)
most_skipped_track(streaming_history, "2019-01-21", "2019-07-24")

most_played_artist(streaming_history, "2019-01-21", "2019-02-24")
most_skipped_artist(streaming_history, "2019-01-21", "2019-02-24", how_many = 5)

how_many_skipped(streaming_history, "2018-10-21", "2019-01-01")
how_many_skipped(streaming_history, "2019-09-01", "2019-10-29", as_percentage = TRUE)


number_of_skipped_songs(streaming_history, "2018-10-21", "2019-10-01")
number_of_skipped_songs(streaming_history, "2018-10-21", "2019-10-01", by = "month")

number_of_songs_listened_by_weekday(streaming_history, "2018-10-21", "2018-11-21")

number_of_songs_listened_by_hour(streaming_history, "2018-10-21", "2018-11-21")
number_of_songs_listened_by_hour(streaming_history, "2018-10-21", "2019-01-21", by_weekday = TRUE)


platform_used(search_queries, "2019-10-01", "2019-10-21")
platform_used_by_date(search_queries, "2019-01-01", "2019-10-21")

country_by_date(search_queries, "2019-10-01", "2019-10-21")