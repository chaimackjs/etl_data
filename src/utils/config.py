import os

# Chemin de base du projet (répertoire racine)
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

# chemin des fichiers de la base de données et du schéma SQL

DB_PATH = os.path.join(BASE_DIR, 'databases','Nobels.db')
SCHEMA_PATH = os.path.join(BASE_DIR, 'databases','schema.sql')

# Configuration de l'API Nobel (exemple, à adapter selon la documentation réelle)
NOBEL_API_URL = "https://app.swaggerhub.com/apis/NobelMedia/NobelMasterData/2.1"

# Configuration du logging
LOG_LEVEL = "INFO"  # Possibles valeurs : "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"