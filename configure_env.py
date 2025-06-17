"""
Script pour configurer les variables d'environnement nécessaires au projet.
À exécuter avant de lancer les scripts ETL.

Ce script charge les variables d'environnement depuis le système ou le fichier .env
plutôt que de coder en dur les valeurs sensibles.
"""
import os
import sys
import subprocess
from dotenv import load_dotenv, find_dotenv

# Essayer de charger les variables d'environnement depuis .env s'il existe
dotenv_path = find_dotenv()
if dotenv_path:
    load_dotenv(dotenv_path)
    print(f"Variables d'environnement chargées depuis {dotenv_path}")
else:
    print("Aucun fichier .env trouvé, utilisation des variables d'environnement système")

# Configuration AWS - extraite des variables d'environnement
aws_config = {
    'KEY_ACCESS': os.environ.get('KEY_ACCESS', ''),
    'KEY_SECRET': os.environ.get('KEY_SECRET', ''),
    'DATA_LAKE_BUCKET': os.environ.get('DATA_LAKE_BUCKET'),
    'AWS_REGION': os.environ.get('AWS_REGION', 'eu-north-1'),
    'LAMBDA_ROLE_ARN': os.environ.get('LAMBDA_ROLE_ARN', '')
}

# Configuration PostgreSQL RDS - extraite des variables d'environnement
rds_config = {
    'RDS_HOST': os.environ.get('DB_HOST', ''),
    'RDS_PORT': os.environ.get('DB_PORT', '5432'),
    'RDS_DATABASE': os.environ.get('DB_NAME', ''),
    'RDS_USER': os.environ.get('DB_USER', ''),
    'RDS_PASSWORD': os.environ.get('DB_PASSWORD', '')
}

# Configuration API Pôle Emploi - extraite des variables d'environnement
pole_emploi_config = {
    'POLE_EMPLOI_CLIENT_ID': os.environ.get('POLE_EMPLOI_CLIENT_ID', ''),
    'POLE_EMPLOI_CLIENT_SECRET': os.environ.get('POLE_EMPLOI_CLIENT_SECRET', ''),
    'POLE_EMPLOI_SCOPE': os.environ.get('POLE_EMPLOI_SCOPE', 'api_offresdemploiv2 o2dsoffre'),
    'URL_POLE_EMPLOI': os.environ.get('URL_POLE_EMPLOI', '')
}

def set_env_vars():
    """Configure les variables d'environnement pour le projet."""
    # Définir les variables AWS
    for key, value in aws_config.items():
        os.environ[key] = value
        print(f"Variable définie: {key}")
    
    # Définir les variables RDS
    for key, value in rds_config.items():
        os.environ[key] = value
        print(f"Variable définie: {key}")
    
    # Définir les variables Pôle Emploi
    for key, value in pole_emploi_config.items():
        os.environ[key] = value
        print(f"Variable définie: {key}")
    
    # Pour compatibilité avec le code existant, ajouter des mappings 
    # avec les noms de variables attendus par certaines fonctions
    os.environ['DB_HOST'] = os.environ['RDS_HOST']
    os.environ['DB_PORT'] = os.environ['RDS_PORT']
    os.environ['DB_NAME'] = os.environ['RDS_DATABASE']
    os.environ['DB_USER'] = os.environ['RDS_USER']
    os.environ['DB_PASSWORD'] = os.environ['RDS_PASSWORD']
    
    print("\nToutes les variables d'environnement ont été configurées.")
    
    # Afficher le status des variables principales
    print("\nStatut des variables principales:")
    print(f"AWS ACCESS KEY: {'✓ Configurée' if os.environ.get('KEY_ACCESS') else '✗ Non configurée'}")
    print(f"AWS SECRET KEY: {'✓ Configurée' if os.environ.get('KEY_SECRET') else '✗ Non configurée'}")
    print(f"RDS HOST: {'✓ Configurée' if os.environ.get('RDS_HOST') else '✗ Non configurée'}")
    print(f"RDS DATABASE: {'✓ Configurée' if os.environ.get('RDS_DATABASE') else '✗ Non configurée'}")
    print(f"Pôle Emploi Client ID: {'✓ Configurée' if os.environ.get('POLE_EMPLOI_CLIENT_ID') else '✗ Non configurée'}")
    print(f"Pôle Emploi Client Secret: {'✓ Configurée' if os.environ.get('POLE_EMPLOI_CLIENT_SECRET') else '✗ Non configurée'}")

def main():
    """Fonction principale."""
    print("Configuration des variables d'environnement...")
    set_env_vars()
    
    # Demander à l'utilisateur s'il veut exécuter le script ETL
    run_etl = input("\nVoulez-vous exécuter le pipeline ETL maintenant ? (o/n): ").lower()
    if run_etl == 'o' or run_etl == 'oui':
        print("\nExécution du pipeline ETL...")
        try:
            # Exécuter le script ETL
            subprocess.run([sys.executable, "src/etl/api/france_travail_etl.py"], check=True)
        except Exception as e:
            print(f"\nErreur lors de l'exécution du pipeline ETL: {e}")
    else:
        print("\nPour exécuter le pipeline ETL plus tard, utilisez la commande:")
        print("python src/etl/api/france_travail_etl.py")
    
    print("\nPour vérifier la base de données, utilisez:")
    print("python src/etl/api/verify_db_load.py")


if __name__ == "__main__":
    main()
