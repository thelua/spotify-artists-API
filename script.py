import requests
import json
import sqlite3
import os
import csv
from spotify_bd import BancoDeDados 
from multiprocessing import Pool
from credenciais import client_id, client_secret
from datetime import datetime

def obter_token_spotify():
    auth_url = 'https://accounts.spotify.com/api/token'
    auth_data = {'grant_type': 'client_credentials'}

    try:
        auth_resposta = requests.post(auth_url, auth=(client_id, client_secret), data=auth_data)
        access_token = auth_resposta.json()['access_token']
        return access_token
    except requests.exceptions.RequestException as e:
        print(f"Erro ao obter o token de autenticação: {e}")
        return None

def buscar_artistas_no_spotify(nome_artista, access_token):
    API = "https://api.spotify.com/v1/"
    endpoint_procurar = API + 'search'
    params = {'q': nome_artista, 'type': 'artist'}
    headers = {'Authorization': f'Bearer {access_token}'}

    try:
        resposta = requests.get(endpoint_procurar, params=params, headers=headers)
        artist_data = resposta.json().get('artists', {}).get('items', [])
    except Exception as e:
        print(f"Erro na solicitação HTTP: {e}")
        return None

    return artist_data

def obter_dados_albums(artist_id, access_token):
    API = "https://api.spotify.com/v1/"
    endpoint_album = API + f'artists/{artist_id}/albums'
    params = {'country': 'BR'}
    headers = {'Authorization': f'Bearer {access_token}'}

    try:
        resposta = requests.get(endpoint_album, params=params, headers=headers)
        albums_data = resposta.json().get('items', [])
    except Exception as e:
        print(f"Erro na solicitação HTTP: {e}")
        return None

    return albums_data

def obter_dados_top_tracks(artist_id, access_token):
    API = "https://api.spotify.com/v1/"
    endpoint_top_tracks = API + f'artists/{artist_id}/top-tracks'
    top_tracks_params = {'country': 'BR'}
    headers = {'Authorization': f'Bearer {access_token}'}

    try:
        resposta = requests.get(endpoint_top_tracks, params=top_tracks_params, headers=headers)
        top_tracks_data = resposta.json().get('tracks', [])
    except Exception as e:
        print(f"Erro na solicitação HTTP: {e}")
        return None

    return top_tracks_data


def salvar_dados_albums(cursor, artist_db_id, albums_data, con):
    for album in albums_data:
        album_id = album['id']
        album_name = album['name']

        cursor.execute("INSERT INTO albuns (nome, artista_id, spotify_id, data_consulta) VALUES (?, ?, ?, ?)",
                       (album_name, artist_db_id, album_id, datetime.now().strftime("%d/%m/%Y")))
        con.commit()

def salvar_dados_top_tracks(cursor, artist_db_id, top_tracks_data, con):
    for track in top_tracks_data:
        track_id = track['id']
        track_name = track['name']
        album_name = track['album']['name']

        cursor.execute("INSERT INTO top_tracks (nome, artista_id, album_nome, spotify_id, data_consulta) VALUES (?, ?, ?, ?, ?)",
                       (track_name, artist_db_id, album_name, track_id, datetime.now().strftime("%d/%m/%Y")))
        con.commit()

def obter_dados_spotify(nome_artista):
    access_token = obter_token_spotify()

    if not access_token:
        return

    artist_data = buscar_artistas_no_spotify(nome_artista, access_token)

    if not artist_data:
        print(f"'{nome_artista}' não encontrado no Spotify.")
        return

    artist = artist_data[0]
    artist_id = artist.get('id', '')
    artista_nome = artist.get('name', '')
    artista_popularidade = artist.get('popularity', '')

    try:
        con = sqlite3.connect("spotify_banco.sqlite")
        cursor = con.cursor()
    except Exception as e:
        print(f"Ocorreu um erro: {e}")

    cursor.execute("SELECT id FROM artistas WHERE spotify_id=?", (artist_id,))
    artista_existente = cursor.fetchone()

    if artista_existente:
        artist_db_id = artista_existente[0]
        cursor.execute("UPDATE artistas SET popularidade=? WHERE id=?", (artista_popularidade, artist_db_id))
    else:
        cursor.execute("INSERT INTO artistas (nome, spotify_id, popularidade, data_consulta) VALUES (?, ?, ?, ?)", 
                       (artista_nome, artist_id, artista_popularidade, datetime.now().strftime("%d/%m/%Y")))
        artist_db_id = cursor.lastrowid

    con.commit()
        

    artista_diretorio = f"dados_spotify/{artista_nome}"
    os.makedirs(artista_diretorio, exist_ok=True)


    albums_data = obter_dados_albums(artist_id, access_token)
    if albums_data:
        salvar_dados_albums(cursor, artist_db_id, albums_data, con)


    top_tracks_data = obter_dados_top_tracks(artist_id, access_token)
    if top_tracks_data:
        salvar_dados_top_tracks(cursor, artist_db_id, top_tracks_data, con)

    gerar_csv(cursor, artist_db_id, artista_nome, artist_id, albums_data, top_tracks_data)
    con.close()


def gerar_csv(cursor, artist_db_id, artista_nome, artist_id, albums_data, top_tracks_data):
    artista_diretorio = f"dados_spotify/{artista_nome}"
    os.makedirs(artista_diretorio, exist_ok=True)
    con = sqlite3.connect("spotify_banco.sqlite")
    cursor = con.cursor()

    cursor.execute("SELECT popularidade, data_consulta FROM artistas WHERE id=?", (artist_db_id,))
    artista_popularidade, data_consulta = cursor.fetchone()
    
    data_atual = datetime.now().strftime("%d-%m-%Y")
    
    if data_consulta == data_atual:
        print(f"A consulta para {artista_nome} já foi realizada para a data atual.")
        return

    artista_csv_file = f"{artista_diretorio}/artista.csv"
    with open(artista_csv_file, 'w', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(['id', 'nome', 'spotify_id', 'popularidade', 'data_consulta'])
        csv_writer.writerow([artist_db_id, artista_nome, artist_id, artista_popularidade, data_atual])


    cursor.execute("UPDATE artistas SET data_consulta=? WHERE id=?", (data_atual, artist_db_id))
    con.commit()

    albums_csv_file = f"{artista_diretorio}/albums.csv"
    with open(albums_csv_file, 'w', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(['id', 'nome', 'artista_id', 'spotify_id', 'data_consulta'])
        for album in albums_data:
            cursor.execute("SELECT id, data_consulta FROM albuns WHERE spotify_id=?", (album['id'],))
            album_info = cursor.fetchone()
            album_id, data_consulta = album_info[0], album_info[1]
            csv_writer.writerow([album_id, album['name'], artist_db_id, album['id'], data_atual])

    top_tracks_csv_file = f"{artista_diretorio}/top_tracks.csv"
    with open(top_tracks_csv_file, 'w', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(['id', 'nome', 'artista_id', 'album_nome', 'spotify_id', 'data_consulta'])

        for track in top_tracks_data:
            track_id = track['id']
            track_name = track['name']
            album_name = track['album']['name']

            cursor.execute("SELECT album_nome, data_consulta FROM top_tracks WHERE spotify_id=?", (track_id,))
            track_info = cursor.fetchone()
            album_nome_db, data_consulta = track_info[0], track_info[1]

            if album_nome_db != album_name:
                cursor.execute("UPDATE top_tracks SET album_nome=? WHERE spotify_id=?", (album_name, track_id))
                con.commit()

            csv_writer.writerow([track_id, track_name, artist_db_id, album_name, track['id'], data_atual])

    print(f"Dados do artista '{artista_nome}' salvos com sucesso.")

def processar_artista(artista):
    print(f"Iniciando processamento do artista: {artista}")
    obter_dados_spotify(artista)

def main():
    bd = BancoDeDados()
    bd.criar_banco_dados()


    with open('artistas.json', 'r') as file:
        artistas = json.load(file)

    num_processos = 4
    with Pool(num_processos) as pool:
        pool.map(processar_artista, artistas)

if __name__ == "__main__":
    main()