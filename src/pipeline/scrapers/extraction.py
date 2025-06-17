#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Module d'extraction des données Welcome to the Jungle depuis AWS S3.
"""

import os
import json
import boto3
import logging
from datetime import datetime
import pandas as pd

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Charger les variables d'environnement
from src.pipeline.api.dotenv_utils import load_dotenv
load_dotenv()

def extract_welcome_jungle_data(bucket_name=None, specific_file=None, all_files=True, force_download=False):
    """
    Extrait les données Welcome to the Jungle depuis AWS S3.
    
    Args:
        bucket_name (str): Nom du bucket S3 (optionnel, utilise la variable d'environnement par défaut)
        specific_file (str): Chemin spécifique du fichier à extraire (optionnel)
        all_files (bool): Si True, extrait tous les fichiers disponibles au lieu du plus récent uniquement
        force_download (bool): Si True, force le téléchargement même si les fichiers existent déjà localement
        
    Returns:
        pandas.DataFrame: DataFrame contenant les données extraites
        str: Chemin du fichier local où les données ont été sauvegardées
    """
    try:
        # Utiliser le bucket spécifié ou celui de la variable d'environnement
        if not bucket_name:
            bucket_name = os.environ.get('DATA_LAKE_BUCKET')
            
        logger.info(f"Extraction des données Welcome to the Jungle depuis le bucket S3: {bucket_name}")
        
        # Connexion à S3 avec les identifiants par défaut si les variables d'environnement ne sont pas disponibles
        aws_access_key = os.environ.get('KEY_ACCESS')
        aws_secret_key = os.environ.get('KEY_SECRET')
        aws_region = os.environ.get('AWS_REGION', 'eu-north-1')
        
        logger.info(f"Utilisation des identifiants AWS: {aws_access_key[:5]}...")
        
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=aws_region
        )
        
        # Lister les objets dans le bucket/préfixe pour Welcome to the Jungle
        prefix = 'raw/welcome_jungle/'
        
        if specific_file:
            # Si un fichier spécifique est demandé, l'utiliser directement
            s3_objects = [{'Key': f"{prefix}{specific_file}"}]
            logger.info(f"Extraction du fichier spécifique: {specific_file}")
        else:
            # Sinon, lister tous les fichiers disponibles
            response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
            
            if 'Contents' not in response:
                logger.warning(f"Aucun fichier trouvé dans {bucket_name}/{prefix}")
                return pd.DataFrame(), None
                
            s3_objects = response['Contents']
            logger.info(f"Nombre de fichiers trouvés: {len(s3_objects)}")
            
            # Trier par date de dernière modification (le plus récent d'abord)
            s3_objects = sorted(s3_objects, key=lambda x: x['LastModified'], reverse=True)
            
            # Si all_files est False, prendre uniquement le fichier le plus récent
            if not all_files and s3_objects:
                logger.info(f"Utilisation du fichier le plus récent: {s3_objects[0]['Key']}")
                s3_objects = [s3_objects[0]]
            else:
                logger.info(f"Extraction de tous les {len(s3_objects)} fichiers disponibles")
        
        # Créer le répertoire de destination s'il n'existe pas
        local_dir = os.path.join('data', 'raw', 'welcome_jungle')
        os.makedirs(local_dir, exist_ok=True)
        
        all_dfs = []
        successful_files = []
        
        for s3_object in s3_objects:
            s3_key = s3_object['Key']
            
            # Vérifier si le fichier est valide (ignorer les dossiers ou fichiers vides)
            if s3_key.endswith('/') or not s3_key.endswith('.json'):
                logger.warning(f"Ignorer l'objet non-JSON ou dossier: {s3_key}")
                continue
                
            filename = os.path.basename(s3_key)
            local_file_path = os.path.join(local_dir, filename)
            
            try:
                # Vérifier si le fichier existe déjà localement
                if os.path.exists(local_file_path) and not force_download:
                    logger.info(f"Fichier {filename} déjà présent localement, utilisation du fichier existant")
                else:
                    logger.info(f"Téléchargement du fichier {s3_key} vers {local_file_path}")
                    s3_client.download_file(bucket_name, s3_key, local_file_path)
                
                # Charger les données JSON dans un DataFrame
                with open(local_file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                # Convertir en DataFrame en fonction de la structure des données
                try:
                    # Si les données sont une liste d'objets
                    if isinstance(data, list):
                        df = pd.DataFrame(data)
                    # Si les données sont un dictionnaire avec une clé contenant une liste
                    elif isinstance(data, dict):
                        # Chercher une clé qui contient une liste d'objets
                        for key, value in data.items():
                            if isinstance(value, list) and value:
                                logger.info(f"Utilisation de la clé '{key}' contenant {len(value)} éléments")
                                df = pd.DataFrame(value)
                                break
                        else:
                            # Si aucune liste n'est trouvée, utiliser le dictionnaire lui-même
                            df = pd.DataFrame([data])
                    else:
                        logger.warning(f"Structure de données non reconnue dans {filename}")
                        df = pd.DataFrame()
                        
                    if not df.empty:
                        all_dfs.append(df)
                        successful_files.append(local_file_path)
                        logger.info(f"Fichier {filename} chargé avec succès: {len(df)} enregistrements")
                    else:
                        logger.warning(f"Le fichier {filename} ne contient pas de données valides")
                except Exception as e:
                    logger.error(f"Erreur lors de la conversion des données en DataFrame: {e}")
                    # Sauvegarde de la structure des données pour débogage
                    debug_file = os.path.join(local_dir, f"debug_{filename}.txt")
                    with open(debug_file, 'w', encoding='utf-8') as f:
                        if isinstance(data, dict):
                            f.write(f"Type: dict avec {len(data)} clés\n")
                            f.write(f"Clés: {list(data.keys())}\n")
                        elif isinstance(data, list):
                            f.write(f"Type: list avec {len(data)} éléments\n")
                            if data and isinstance(data[0], dict):
                                f.write(f"Premier élément clés: {list(data[0].keys())}\n")
                        else:
                            f.write(f"Type: {type(data)}\n")
                    logger.info(f"Informations de débogage sauvegardées dans {debug_file}")
                    continue
            except Exception as e:
                logger.error(f"Erreur lors du traitement du fichier {s3_key}: {e}")
        
        # Concaténer les DataFrames
        if all_dfs:
            df = pd.concat(all_dfs, ignore_index=True)
            logger.info(f"Données extraites avec succès: {len(df)} enregistrements au total de {len(all_dfs)} fichiers")
            return df, successful_files[0] if successful_files else None
        else:
            logger.error("Aucun fichier n'a pu être traité")
            return pd.DataFrame(), None
        
    except Exception as e:
        logger.error(f"Erreur lors de l'extraction des données Welcome to the Jungle: {e}")
        return pd.DataFrame(), None

def main():
    """
    Fonction principale pour tester l'extraction.
    """
    # Extraire toutes les données Welcome to the Jungle disponibles
    df, file_path = extract_welcome_jungle_data(all_files=True)
    if not df.empty:
        logger.info(f"Extraction réussie: {len(df)} offres d'emploi")
        logger.info(f"Données sauvegardées dans: {file_path}")
        logger.info(f"Colonnes disponibles: {df.columns.tolist()}")
    else:
        logger.error("Extraction échouée")

if __name__ == "__main__":
    main()