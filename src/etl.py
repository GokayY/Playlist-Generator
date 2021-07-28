import pandas as pd
from DataCollector import DataCollector
from DBConnection import DBConnection

# Data Collection
dc = DataCollector()

# Database Connection
db = DBConnection()
conn = db.get_connector()
cur = conn.cursor()

album_data = dc.get_song_data_by_artist_name('Portishead')
track_data = dc.get_track_features(album_data)


def load_artist_data(dataframe):
    df = pd.DataFrame(columns=['artist_id', 'artist_name'])

    for row in dataframe.itertuples(index=True):
        df.at[row.Index, 'artist_id'] = row.artist_id
        df.at[row.Index, 'artist_name'] = row.artist_name

    df = df.drop_duplicates()
    try:
        db = DBConnection()
        conn = db.get_connector()
        cur = conn.cursor()

        for row in df.itertuples():
            sql = """INSERT INTO ARTISTS (artist_id, \
                                          artist_name)\
                    VALUES (%s, %s)
                    ON CONFLICT (artist_id) DO NOTHING"""
            cur.execute(sql, (row.artist_id, row.artist_name))

        conn.commit()
        conn.close()

    except Exception as e:
        print(e)
        print("Rollback")
        if conn:
            conn.rollback()


def load_album_data(dataframe):
    df = pd.DataFrame(columns=['album_type', 'album_id', 'album_name',
                               'release_date', 'total_tracks', 'type', ])
    for row in dataframe.itertuples(index=True):
        df.at[row.Index, 'album_id'] = row.album_id
        df.at[row.Index, 'album_name'] = row.album_name
        df.at[row.Index, 'album_type'] = row.album_type
        df.at[row.Index, 'release_date'] = row.release_date
        df.at[row.Index, 'total_tracks'] = row.total_tracks
        df.at[row.Index, 'type'] = row.type

    df = df.drop_duplicates()
    try:
        db = DBConnection()
        conn = db.get_connector()
        cur = conn.cursor()

        for row in df.itertuples():
            sql = """INSERT INTO ALBUMS (album_id,\
                                          album_name,\
                                          album_type,\
                                          release_date,\
                                          total_tracks,\
                                          type) \
                    VALUES (%s, %s, %s, %s, %s, %s) \
                    ON CONFLICT (album_id) DO NOTHING"""
            cur.execute(sql, (row.album_id, row.album_name, row.album_type, \
                              row.release_date, row.total_tracks, row.type))

        conn.commit()
        conn.close()
    except Exception as e:
        print(e)
        print("Rollback")
        if conn:
            conn.rollback()


def load_track_data(dataframe):
    pass
