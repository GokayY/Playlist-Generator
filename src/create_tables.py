from DBConnection import DBConnection

dbc = DBConnection()
conn = dbc.get_connector()
cur = conn.cursor()

try:
    cur.execute("CREATE TABLE IF NOT EXISTS ALBUMS(album_id text NOT NULL PRIMARY KEY,\
                                                   album_name text, \
                                                   album_type text, \
                                                   release_date text,\
                                                   total_tracks int,\
                                                   type text)")

    cur.execute("CREATE TABLE IF NOT EXISTS ARTISTS(artist_id text NOT NULL PRIMARY KEY,\
                                                    artist_name text)")

    cur.execute("CREATE TABLE IF NOT EXISTS TRACKS(id text NOT NULL PRIMARY KEY,\
                                                   name text,\
                                                   track_number integer,\
                                                   uri text, \
                                                   album_id text REFERENCES albums (album_id), \
                                                   artist_id text REFERENCES artists (artist_id), \
                                                   danceability numeric, \
                                                   energy numeric, \
                                                   key integer, \
                                                   loudness numeric, \
                                                   mode integer,\
                                                   speechiness numeric,\
                                                   acousticness numeric, \
                                                   instrumentalness numeric,\
                                                   liveness numeric, \
                                                   valence numeric, \
                                                   tempo numeric)")

    cur.execute("commit")
except Exception as e:
    print("Failed: " + str(e))
    print("Rollbacked.")
    conn.rollback()

