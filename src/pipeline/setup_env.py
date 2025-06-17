#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script de configuration des variables d'environnement pour le pipeline ETL.
Permet de créer ou mettre à jour le fichier .env avec les paramètres nécessaires.
"""

import os
import argparse
import logging
from datetime import datetime
from dotenv import set_key, load_dotenv, find_dotenv

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration par défaut pour les variables d'environnement
default_env_vars = {
    # AWS Credentials
    'KEY_ACCESS': os.environ.get('KEY_ACCESS', ''),
    'KEY_SECRET': os.environ.get('KEY_SECRET', ''),
    'DATA_LAKE_BUCKET': os.environ.get('DATA_LAKE_BUCKET', 'data-lake-brut'),
    'LAMBDA_ROLE_ARN': os.environ.get('LAMBDA_ROLE_ARN', 'arn:aws:iam::194722407093:role/LambdaETLRole'),
    
    # RDS Configuration
    'DB_HOST': os.environ.get('DB_HOST', 'datas.c32ygg4oyapa.eu-north-1.rds.amazonaws.com'),
    'DB_PORT': os.environ.get('DB_PORT', '5432'),
    'DB_NAME': os.environ.get('DB_NAME', 'postgres'),
    'DB_USER': os.environ.get('DB_USER', 'postgres'),
    'DB_PASSWORD': os.environ.get('DB_PASSWORD', ''),
    
    # France Travail API Configuration
    'POLE_EMPLOI_CLIENT_ID': os.environ.get('POLE_EMPLOI_CLIENT_ID', 'PAR_dataanalyse_7a35816bfdfa9a889ff2c8c3786217cbd397e99f7f1d1056bb3568f17ec115c0'),
    'POLE_EMPLOI_CLIENT_SECRET': os.environ.get('POLE_EMPLOI_CLIENT_SECRET', ''),
    'POLE_EMPLOI_SCOPE': os.environ.get('POLE_EMPLOI_SCOPE', 'api_offresdemploiv2 o2dsoffre'),
    'URL_POLE_EMPLOI': os.environ.get('URL_POLE_EMPLOI', 'https://francetravail.io/connexion/oauth2/access_token?realm=%2Fpartenaire')
}

def setup_environment(env_file='.env', overwrite=False, interactive=True):
    """
    Configure les variables d'environnement dans un fichier .env.
    
    Args:
        env_file (str): Chemin du fichier .env
        overwrite (bool): Si True, écrase les valeurs existantes
        interactive (bool): Si True, demande confirmation pour chaque variable
        
    Returns:
        bool: True si la configuration a réussi, False sinon
    """
    try:
        # Charger le fichier .env existant s'il existe
        dotenv_path = find_dotenv(env_file)
        if not dotenv_path:
            # Si le fichier n'existe pas, le créer dans le répertoire courant
            dotenv_path = os.path.join(os.getcwd(), env_file)
            with open(dotenv_path, 'w') as f:
                f.write("# Variables d'environnement pour le pipeline ETL\n")
                f.write(f"# Créé le {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        load_dotenv(dotenv_path)
        
        logger.info(f"Configuration du fichier d'environnement: {dotenv_path}")
        
        # Configurer chaque variable d'environnement
        for key, default_value in DEFAULT_ENV_VARS.items():
            current_value = os.getenv(key)
            
            # Déterminer si nous devons demander/modifier la valeur
            should_set = False
            new_value = default_value
            
            if interactive:
                if current_value:
                    user_input = input(f"{key} [actuel: {current_value}] (Entrer pour conserver, nouvelle valeur pour modifier): ")
                    if user_input:
                        new_value = user_input
                        should_set = True
                else:
                    user_input = input(f"{key} [défaut: {default_value}] (Entrer pour accepter, nouvelle valeur pour modifier): ")
                    if user_input:
                        new_value = user_input
                    should_set = True
            else:
                # En mode non interactif, définir uniquement si la valeur n'existe pas ou si overwrite est True
                if overwrite or not current_value:
                    should_set = True
            
            # Mettre à jour la variable
            if should_set:
                set_key(dotenv_path, key, new_value)
                logger.info(f"Variable définie: {key}")
        
        logger.info(f"Configuration des variables d'environnement terminée dans {dotenv_path}")
        return True
        
    except Exception as e:
        logger.error(f"Erreur lors de la configuration des variables d'environnement: {e}")
        return False

def test_environment():
    """
    Teste que les variables d'environnement nécessaires sont définies.
    
    Returns:
        bool: True si toutes les variables nécessaires sont définies, False sinon
    """
    load_dotenv()
    
    logger.info("=== Test des variables d'environnement ===")
    
    missing_vars = []
    for key in DEFAULT_ENV_VARS.keys():
        value = os.getenv(key)
        if value:
            logger.info(f"{key}: Défini")
        else:
            logger.warning(f"{key}: Non défini")
            missing_vars.append(key)
    
    if missing_vars:
        logger.warning(f"Variables manquantes: {', '.join(missing_vars)}")
        return False
    else:
        logger.info("Toutes les variables d'environnement nécessaires sont définies")
        return True

if __name__ == "__main__":
    # Définir les arguments de la ligne de commande
    parser = argparse.ArgumentParser(description="Configuration des variables d'environnement pour le pipeline ETL")
    parser.add_argument('--env-file', type=str, default='.env', help="Chemin du fichier .env")
    parser.add_argument('--overwrite', action='store_true', help="Écraser les valeurs existantes")
    parser.add_argument('--non-interactive', action='store_true', help="Mode non interactif")
    parser.add_argument('--test', action='store_true', help="Tester uniquement les variables d'environnement")
    
    args = parser.parse_args()
    
    if args.test:
        test_environment()
    else:
        setup_environment(args.env_file, args.overwrite, not args.non_interactive)
