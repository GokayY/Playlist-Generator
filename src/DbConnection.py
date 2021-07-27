from data_collector import data_collector
import psycopg2
import pandas as pd
import numpy as np
import os
import glob

album_data = data_collector.get_song_data_by_artist_name('Radiohead')

track_data = data_collector.get_track_features(album_data)