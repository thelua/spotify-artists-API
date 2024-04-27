import sqlite3
from datetime import datetime

class BancoDeDados():

    def criar_banco_dados(self):
        con = sqlite3.connect("spotify_banco.sqlite")
        cursor = con.cursor()


        cursor.execute('''
            CREATE TABLE IF NOT EXISTS artistas (
                id INTEGER PRIMARY KEY,
                nome TEXT NOT NULL,
                spotify_id TEXT NOT NULL,
                popularidade INTENGER,
                data_consulta TEXT NOT NULL  
            )
        ''')


        cursor.execute('''
            CREATE TABLE IF NOT EXISTS albuns (
                id INTEGER PRIMARY KEY,
                nome TEXT NOT NULL,
                artista_id INTEGER NOT NULL,
                spotify_id TEXT NOT NULL,
                data_consulta TEXT NOT NULL,
                FOREIGN KEY (artista_id) REFERENCES artistas (id)
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS top_tracks (
                id INTEGER PRIMARY KEY,
                nome TEXT NOT NULL,
                artista_id INTEGER NOT NULL,
                spotify_id TEXT NOT NULL,
                album_nome TEXT NOT NULL,
                data_consulta TEXT NOT NULL,
                FOREIGN KEY (artista_id) REFERENCES artistas (id),
                FOREIGN KEY (album_nome) REFERENCES albuns (nome)
            )
        ''')

        con.commit()
        con.close()

    def obter_e_atualizar_data(self, tabela, id):
        con = sqlite3.connect("spotify_db.sqlite")
        cursor = con.cursor()

        cursor.execute(f"SELECT data_atualizacao FROM {tabela} WHERE id=?", (id,))
        data_atualizacao = cursor.fetchone()

        if data_atualizacao:
            cursor.execute(f"UPDATE {tabela} SET data_atualizacao=? WHERE id=?", (datetime.now().strftime("%d/%m/%Y"), id))
            con.commit()

        con.close()

        return data_atualizacao[0] if data_atualizacao else None