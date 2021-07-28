import pandas as pd
from DBConnection import DBConnection
from DataCollector import DataCollector


class ETLPipeline:
    def __init__(self):
        self.db = DBConnection()
        self.dc = DataCollector()

    def process_data(self, artist):
        if type(artist) == str:

            album_data = self.dc.get_song_data_by_artist_name(artist)
            track_data = self.dc.get_track_features(album_data)

            print(artist)
            self.load_album_data(album_data)
            self.load_artist_data(album_data)
            self.load_track_data(track_data)
            print(artist + ' added into DB.')

        elif type(artist) == list:

            for index, art in enumerate(artist):
                print(art)
                album_data = self.dc.get_song_data_by_artist_name(art)
                track_data = self.dc.get_track_features(album_data)

                self.load_album_data(album_data)
                self.load_artist_data(album_data)
                self.load_track_data(track_data)
                print(str(index) + ". " + art + ' added into DB.')

        else:
            print("Please enter a valid artist or a list of artists.")

    def load_artist_data(self, dataframe):
        df = pd.DataFrame(columns=['artist_id', 'artist_name'])

        for row in dataframe.itertuples(index=True):
            df.at[row.Index, 'artist_id'] = row.artist_id
            df.at[row.Index, 'artist_name'] = row.artist_name

        df = df.drop_duplicates()
        try:
            conn = self.db.get_connector()
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

    def load_album_data(self, dataframe):
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
            conn = self.db.get_connector()
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

    def load_track_data(self, dataframe):
        try:
            for row in dataframe.itertuples(index=True):
                conn = self.db.get_connector()
                cur = conn.cursor()

                for row in dataframe.itertuples():
                    sql = "INSERT INTO TRACKS (id, name, track_number, uri, album_id, artist_id, \
                                                   danceability, energy, key, loudness, mode,speechiness, \
                                                   acousticness, instrumentalness, liveness, valence, tempo)\
                               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) \
                               ON CONFLICT (id) DO NOTHING"

                    cur.execute(sql, (row.id, row.name, row.track_number, row.uri, row.album_id, row.artist_id,
                                      row.danceability, row.energy, row.key, row.loudness, row.mode, row.speechiness,
                                      row.acousticness, row.instrumentalness, row.liveness, row.valence, row.tempo))

            conn.commit()
            conn.close()
        except Exception as e:
            print(e)
            print("Rollback")
            if conn:
                conn.rollback()


# Initial Data Entry
# artist_list = ['Pink Floyd', 'Portishead', 'Radiohead', 'Massive Attack', 'Tricky',
#                'Motorhead', 'Tool', 'Bjork', 'Hooverphonic', 'Moloko', 'Morphine',
#                'Bent', 'Faithless', 'Archive', 'Groove Armada', 'Testament', 'Incubus',
#                'Jimi Hendrix', 'Queens of the Stone Age', 'Alice in Chains', 'Low',
#                'A Perfect Circle', 'David Bowie', 'Pearl Jam', 'Nine Inch Nails',
#                'Black Sabbath', 'Placebo']
#
# etl = ETLPipeline()
# Test
# etl.process_data(321)
# etl.process_data("Portishead")
# etl.process_data(artist_list)
