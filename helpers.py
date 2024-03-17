from tqdm.notebook import tqdm
import pandas as pd
import time

def artist_all_tracks(artist, spotify_client):
    
   # Each list in this list will be a track and its features
    tracks = []
        
    # Get the artist URI (a unique ID)
    artist_uri = spotify_client.search(artist)['tracks']['items'][0]['artists'][0]['uri']

    # Spotify has a lot of duplicate albums, but we'll cross-reference them with this list to avoid extra loops
    album_checker = []
    
    # The starting point of our loop of albums for those artists with more than 50
    n = 0
    
    # Note that here we include singles, compilations and collaborations in the albums to loop through
    while len(spotify_client.artist_albums(artist_uri, album_type = ['album', 'single'], limit=50, offset = n)['items']) > 0:
        
        # Avoid overloading Spotify with requests by assigning the list of album dictionaries to a variable
        dict_list = spotify_client.artist_albums(artist_uri, album_type = ['album', 'single'], limit=50, offset = n)['items']

        for i, album in enumerate(dict_list):

            # Add the featured artists for the album in question to the checklist
            check_this_album = [j['name'] for j in dict_list[i]['artists']]
            # And the album name
            check_this_album.append(dict_list[i]['name'])
            # And its date
            check_this_album.append(dict_list[i]['release_date'])

            # Only continue looping if that album isn't in the checklist
            if check_this_album not in album_checker:
    
                # Add this album to the checker
                album_checker.append(check_this_album)

                # for every song in an album, return data about the album, song title, and song uri
                for song in spotify_client.album_tracks(album['uri'])['items']:
                    tracks.extend([[artist, album['name'], album['uri'], album['release_date'], song['name'], song['uri']]])

                # For every song on the album, get its descriptors and features in a list and add to the tracklist
                # tracks.extend([[artist, album['name'], album['uri'], ['name'], album['release_date']] 
                #   + list(sp.audio_features(sp.album_tracks(album['uri'])['items'].values())) ])
        
        # Go through the next 50 albums (otherwise we'll get an infinite while loop)
        n += 50

    return tracks

def df_tracks(tracklist):

    df = pd.DataFrame(tracklist, columns=['artist',
     'album_name',
     'album_uri',
     'release_date',
     'track',
     'song_uri',
     ] )

    df.rename(columns={'uri':'song_uri'}, inplace=True)

    df.drop_duplicates(subset=['artist', 'track', 'release_date'], inplace=True)
    
    return df

def get_audio_features(df, spotify_client):
  # prevent error 429 by waiting 5 seconds before sending request
  time.sleep(5)    
  # Send 50 tracks per request
  batchsize = 50

  # feature lists
  acousticness = []
  instrumentalness = []
  mode = []
  time_signature = []
  energy =[]
  loudness =[]
  speechiness = []
  valence = []
  liveness =[]
  tempo = []
  danceability =[]
  key = []
  duration = []
  popularity = []

  # iterate through each track in batches of 50 songs
  for i in range(0,df.shape[0], batchsize):
    uris = []
    batch = df[i:i+batchsize].reset_index()
    # add each song uri to the uris array
    for track in range(0,batch.shape[0], 1):
      # list of up to 100 uris
      uris.append(batch.iloc[track]['song_uri'])

    # gets audio features of the 100 uris
    features = spotify_client.audio_features(uris)
    tracks= spotify_client.tracks(uris)
    
    for i in range(len(features)):
      if features[i] != None:
        popularity.append(tracks['tracks'][i]['popularity'])
        energy.append(features[i]['energy'])
        acousticness.append(features[i]['acousticness'])
        instrumentalness.append(features[i]['instrumentalness'])
        mode.append(features[i]['mode'])
        time_signature.append(features[i]['time_signature'])
        loudness.append(features[i]['loudness'])
        speechiness.append(features[i]['speechiness'])
        valence.append(features[i]['valence'])
        liveness.append(features[i]['liveness'])
        tempo.append(features[i]['tempo'])
        danceability.append(features[i]['danceability'])
        key.append(features[i]['key'])
        duration.append(features[i]['duration_ms'])
      else:
        popularity.append(0)
        energy.append(0)
        acousticness.append(0)
        instrumentalness.append(0)
        mode.append(0)
        time_signature.append(0)
        loudness.append(0)
        speechiness.append(0)
        valence.append(0)
        liveness.append(0)
        tempo.append(0)
        danceability.append(0)
        key.append(0)
        duration.append(0)

  # Add Columns to Dataframe
  df['popularity'] = popularity
  df['energy'] = energy
  df['acousticness'] = acousticness
  df['instrumentalness'] = instrumentalness
  df['mode'] = mode
  df['time_signature'] = time_signature
  df['loudness'] = loudness
  df['speechiness'] = speechiness
  df['valence'] = valence
  df['liveness'] = liveness
  df['tempo'] = tempo
  df['danceability'] = danceability
  df['key'] = key
  df['duration'] = duration

  # rearrange columns
  cols = ['track', 'album_name', 'release_date', 'album_uri', 'song_uri', 'popularity',
              'acousticness', 'danceability', 'energy', 'instrumentalness', 'liveness',
              'loudness', 'speechiness', 'tempo', 'time_signature', 'mode', 'key', 'valence', 'duration', ]
  df= df[cols]
  df['release_date'] = df['release_date'].str.split('-').str[0].tolist()
  df = df.rename(columns = {'release_date':'year'})

  return df

def get_artist_tracklist(artist, spotify_client):
    tracks = artist_all_tracks(artist, spotify_client)
    tracklist = df_tracks(tracks)
    features = get_audio_features(tracklist, spotify_client)
    return features
