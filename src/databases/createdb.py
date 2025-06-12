import sqlite3
import os
from src.utils import DB_PATH, SCHEMA_PATH
def create_db():
   

    #chemin relatif vers la base de données et le schema
    DB_PATH 
    SCHEMA_PATH
    

    #connexion à la base de données
    connexion = sqlite3.connect(DB_PATH)
    cursor = connexion.cursor()


    # Vérifier que le fichier schema.sql existe
    if not os.path.isfile(SCHEMA_PATH):
        raise FileNotFoundError(f"Le fichier {SCHEMA_PATH} n'existe pas")
        return

    # Lire et exécuter le script SQL
    with open(SCHEMA_PATH, 'r', encoding='utf-8') as f:
        schema_script = f.read()
    cursor.executescript(schema_script)
    
    # Sauvegarder les modifications et fermer la connexion
    connexion.commit()
    connexion.close()
    print(f"Base de données '{DB_PATH}' créée avec succès !")
    
