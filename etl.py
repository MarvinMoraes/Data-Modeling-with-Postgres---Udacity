import os
import glob
import psycopg2
import datetime
import pandas as pd
from sql_queries import *


def insert_song_record(cur, df):
  
    song_data = df.loc[:,['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
def insert_artist_record(cur, df):
   
    artist_data = df.loc[:,['artist_id', 'artist_name', 'artist_location','artist_latitude','artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)

def process_song_file(cur, filepath):
     """Loads the song file data into the song and artist table"""
        
    # open song file
    df = pd.read_json(filepath, lines=True)
    # insert records into song and artist tables
    insert_song_record(cur, df)
    insert_artist_record(cur, df)

def insert_time_records(cur, df):
    time_data = (df['ts'], df['ts'].dt.hour, df['ts'].dt.day, df['ts'].dt.week, df['ts'].dt.month, df['ts'].dt.year, df['ts'].dt.weekday_name)
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    
    # Convert tuples to a dict so they can be converted to a DataFrame
    time_dict = dict(zip(column_labels, time_data)) 
    time_df = pd.DataFrame(time_dict)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))
        
def insert_user_records(cur, df):
    
    user_columns = ['userId', 'firstName', 'lastName', 'gender', 'level']
    user_df = df.loc[:, user_columns]
    
    # user_df = user_df.drop_duplicates(subset='userId')

    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)
        
def insert_songplay_records(cur, df):
 
    for index, row in df.iterrows():
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)

def process_log_file(cur, filepath):
 
    """Loads the log file data into the user and time tables. 
    Then, creates the songplay table using the ids of related tables"""
    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    df.ts = df['ts'].apply(lambda ts: datetime.datetime.fromtimestamp(ts/1000.0))
    
    # insert records into our users, songplays, and time tables
    insert_time_records(cur, df)
    insert_user_records(cur, df)
    insert_songplay_records(cur, df)

def process_data(cur, conn, filepath, func):
     """Walks the data directory for easy loading of files"""
        
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()