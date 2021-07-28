import configparser
import pandas as pd
import requests


class DataCollector:
    def __init__(self):
        self._config = configparser.ConfigParser()
        self._config.read_file(open(r'C:\Users\gokay\Documents\GitHub\Spotify-Recommender\recommender\src\access.cfg'))
        self._KEY = self._config.get('SPOTIFY', 'client_id')
        self._SECRET = self._config.get('SPOTIFY', 'client_secret')

        self._auth_response = requests.post("https://accounts.spotify.com/api/token", \
                                            {'grant_type': 'client_credentials',
                                             'client_id': self._KEY,
                                             'client_secret': self._SECRET})

        self._auth_response_data = self._auth_response.json()
        self._access_token = self._auth_response_data['access_token']
        self._header = {'Authorization': f'Bearer {self._access_token}'}

    def get_song_data_by_artist_name(self, artist_name):
        """
        Helper function to get albums of a given artist.

        :param artist_name: str
        :return:
        1) pandas dataframe - dataframe: Album Information
        """

        # Albums
        # Base URLs for Artist and Album requests
        artists_base = "https://api.spotify.com/v1/"
        albums_base = "https://api.spotify.com/v1/artists/"

        # Requests for Artist and Albums
        response_1 = requests.get(artists_base + f'search?q="{artist_name}"&type=artist', headers=self._header)
        result_1 = response_1.json()
        artist_id = result_1['artists']['items'][0]['id']
        response_2 = requests.get(albums_base + f"{artist_id}/albums?limit=50", headers=self._header)
        result_2 = response_2.json()

        # Collecting Albums for the Artist
        dataframe = pd.DataFrame(result_2['items'])
        dataframe['uri'] = dataframe['uri'].apply(lambda x: x.split(":")[-1])
        dataframe = dataframe[(dataframe['album_group'] == 'album') | (dataframe['album_group'] == 'single')]
        dataframe = dataframe.rename(columns={'id': 'album_id', 'name': 'album_name'})

        # Adding Artist Name Column
        artist_name = []
        for row in dataframe.itertuples():
            artist_name.append(row.artists[0]['name'])

        dataframe['artist_name'] = artist_name

        # Adding Artist Id Column
        artist_id = []
        for row in dataframe.itertuples():
            artist_id.append(row.artists[0]['id'])
        dataframe['artist_id'] = artist_id

        # Cleaning DataFrame
        dataframe = dataframe.drop(['images', 'artists', 'album_group', 'available_markets',
                                    'release_date_precision', 'href', 'external_urls'], axis=1)

        dataframe = dataframe.drop_duplicates('album_name', keep='first')
        dataframe = dataframe.drop_duplicates('release_date', keep='first').reset_index(drop=True)

        return dataframe

    # Tracks
    # Collecting track data with album dataframe

    def get_track_features(self, dataframe):
        """
        Helper function to be used with get_song_data_by_artist_name function.
        Collects track information including audio features.

        :param pandas dataframe:
        :return:
        pandas dataframe - track_df: Track and audio features
        """

        tracks_df = pd.DataFrame()

        for row in dataframe.itertuples():
            tracks_base = f"https://api.spotify.com/v1/albums/{row.album_id}/tracks"
            r = requests.get(tracks_base, headers=self._header)
            r = r.json()
            _ = pd.DataFrame(r['items'])
            _['album_id'] = row.album_id
            _['artist_id'] = row.artist_id
            tracks_df = pd.concat([tracks_df, _], ignore_index=True)

        # Cleaning DataFrame
        tracks_df = tracks_df.drop(['artists', 'preview_url', 'external_urls', 'href', 'is_local',
                                    'available_markets', 'type', 'explicit', 'disc_number', 'duration_ms'],
                                   axis=1)

        # Audio Features Collection
        features_df = pd.DataFrame()
        for val in tracks_df['id']:
            features_base = f"https://api.spotify.com/v1/audio-features/{val}"
            r = requests.get(features_base, headers=self._header)
            r = r.json()
            features_df = pd.concat([features_df, pd.DataFrame(r, index=[0])], ignore_index=True)

        features_df = features_df.drop(['type', 'uri', 'track_href',
                                        'analysis_url', 'duration_ms', 'time_signature'],
                                       axis=1)
        tracks_df = pd.merge(tracks_df, features_df, on='id', how='inner').reset_index(drop=True)

        return tracks_df
