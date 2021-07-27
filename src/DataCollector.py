import os
import configparser
import json
import requests
import pandas as pd
import spotipy as sp

from spotipy.oauth2 import SpotifyClientCredentials

class DataCollector:
    config = configparser.ConfigParser()
    config.read_file(open(r'access.ini'))
    KEY = config.get('SPOTIFY','client_id')
    SECRET = config.get('SPOTIFY','client_secret')

    AUTH_URL = "https://accounts.spotify.com/api/token"
    auth_response = requests.post(AUTH_URL, {'grant_type': 'client_credentials',
                                             'client_id': KEY,
                                             'client_secret': SECRET,})

    auth_response_data = auth_response.json()
    access_token = auth_response_data['access_token']
    headers = {'Authorization': 'Bearer {token}'.format(token=access_token)}

    def get_song_data_by_artist_name(artist_name):
        """
        Helper function to get albums of a given artist.

        :param artist_name: str
        :return:
        1) pandas dataframe - dataframe: Album Information
        """
        ## Albums
        # Base URLs for Artist and Album requests
        artists_base = "https://api.spotify.com/v1/"
        albums_base = "https://api.spotify.com/v1/artists/"

        # Requests for Artist and Albums
        try:
            r = requests.get(artists_base + f'search?q="{artist_name}"&type=artist', headers = headers)
            r = r.json()
            artist_id =  r['artists']['items'][0]['id']
            r = requests.get(albums_base +f"{artist_id}/albums?limit=50", headers=headers)
            r = r.json()
        except Exception as e:
            print(e)

        # Collecting Albums for the Artist
        dataframe = pd.DataFrame(r['items'])
        dataframe['uri'] = dataframe['uri'].apply(lambda x: x.split(":")[-1])
        dataframe = dataframe[(dataframe['album_group'] == 'album') | (dataframe['album_group'] == 'single')]
        dataframe = dataframe.rename(columns={'id':'album_id', 'name':'album_name'})

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
        dataframe = dataframe.drop(['images','artists', 'album_group', 'available_markets',
                                    'href','external_urls'], axis = 1)
        dataframe = dataframe.drop_duplicates('album_name', keep='first')
        dataframe = dataframe.drop_duplicates('release_date', keep='first')

        return dataframe

    ## Tracks
    # Collecting track data with album dataframe
    def get_track_features(dataframe):
        """
        Helper function to be used with get_song_data_by_artist_name function.
        Collects track information including audio features.

        :param pandas dataframe:
        :return:
        pandas dataframe - track_df: Track and audio features
        """

        tracks_df = pd.DataFrame()
        for i in dataframe['uri']:
            tracks_base = f"https://api.spotify.com/v1/albums/{i}/tracks"
            r = requests.get(tracks_base, headers = headers)
            r = r.json()
            tracks_df = pd.concat([tracks_df, pd.DataFrame(r['items'])], ignore_index=True)

        # Exploding the nested dict and Adding artist_name and artist_id as column
        artist_name = []
        for row in tracks_df.itertuples():
            artist_name.append(row[1][0]['name'])
        tracks_df['artist_name'] = artist_name

        # Cleaning DataFrame
        tracks_df['track_id'] = tracks_df['uri'].apply(lambda x: x.split(":")[-1])
        tracks_df = tracks_df.drop(['track_id','artists','preview_url', 'external_urls','href', 'is_local',
                                    'available_markets','type', 'explicit','disc_number', 'duration_ms'],
                                   axis =1)

        # Audio Features Collection
        features_df = pd.DataFrame()
        for val in tracks_df['id']:
            features_base = f"https://api.spotify.com/v1/audio-features/{val}"
            r = requests.get(features_base, headers = headers)
            r = r.json()
            features_df = pd.concat([features_df, pd.DataFrame(r, index = [0])], ignore_index=True)
        features_df = features_df.drop(['type', 'uri', 'track_href',
                                        'analysis_url', 'duration_ms', 'time_signature'],
                                        axis = 1)
        tracks_df = pd.merge(tracks_df, features_df, on='id', how='inner')
        return tracks_df

