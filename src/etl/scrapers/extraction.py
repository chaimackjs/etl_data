#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Module d'extraction de données pour le pipeline ETL.
Gère l'extraction des données depuis différentes sources (API, scraping, fichiers).
"""

import os
import json
import logging
import boto3
from datetime import datetime
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

def extract_from_s3(bucket_name, prefix):
    """
    Extrait des données depuis un bucket S3.
    
    Args:
        bucket_name (str): Nom du bucket S3
        prefix (str): Préfixe des fichiers à extraire
    
    Returns:
        list: Liste des données extraites
    """
    try:
        # Charger les identifiants AWS
        aws_access_key = os.getenv('KEY_ACCESS')
        aws_secret_key = os.getenv('KEY_SECRET')
        
        if not aws_access_key or not aws_secret_key:
            logger.error("Identifiants AWS manquants. Vérifiez les variables d'environnement KEY_ACCESS et KEY_SECRET.")
            return []
        
        # Créer le client S3
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key
        )
        
        # Lister les fichiers dans le bucket
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        
        if 'Contents' not in response:
            logger.warning(f"Aucun fichier trouvé dans s3://{bucket_name}/{prefix}")
            return []
        
        # Extraire les données de chaque fichier
        all_data = []
        for obj in response['Contents']:
            # Ignorer les dossiers
            if obj['Key'].endswith('/'):
                continue
            
            # Télécharger le fichier
            file_obj = s3_client.get_object(Bucket=bucket_name, Key=obj['Key'])
            file_content = file_obj['Body'].read().decode('utf-8')
            
            # Charger le contenu JSON
            try:
                data = json.loads(file_content)
                if isinstance(data, list):
                    all_data.extend(data)
                else:
                    all_data.append(data)
            except json.JSONDecodeError:
                logger.error(f"Erreur de décodage JSON pour le fichier s3://{bucket_name}/{obj['Key']}")
        
        logger.info(f"{len(all_data)} éléments extraits depuis s3://{bucket_name}/{prefix}")
        return all_data
    
    except Exception as e:
        logger.error(f"Erreur lors de l'extraction depuis S3: {e}")
        return []

def extract_from_local(directory):
    """
    Extrait des données depuis des fichiers locaux.
    
    Args:
        directory (str): Répertoire contenant les fichiers à extraire
    
    Returns:
        list: Liste des données extraites
    """
    try:
        # Vérifier si le répertoire existe
        if not os.path.exists(directory):
            logger.error(f"Le répertoire {directory} n'existe pas")
            return []
        
        # Lister les fichiers JSON dans le répertoire
        all_data = []
        for filename in os.listdir(directory):
            if not filename.endswith('.json'):
                continue
            
            # Charger le contenu JSON
            file_path = os.path.join(directory, filename)
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        all_data.extend(data)
                    else:
                        all_data.append(data)
            except json.JSONDecodeError:
                logger.error(f"Erreur de décodage JSON pour le fichier {file_path}")
        
        logger.info(f"{len(all_data)} éléments extraits depuis {directory}")
        return all_data
    
    except Exception as e:
        logger.error(f"Erreur lors de l'extraction depuis les fichiers locaux: {e}")
        return []

def extract_welcome_jungle_data():
    """
    Extrait les données Welcome to the Jungle depuis S3 ou en local.
    
    Returns:
        list: Liste des offres d'emploi extraites
    """
    # Essayer d'abord d'extraire depuis S3
    bucket_name = os.getenv('data_lake_bucket', 'data-lake-brut')
    prefix = 'raw/welcome_jungle/'
    
    s3_data = extract_from_s3(bucket_name, prefix)
    if s3_data:
        return s3_data
    
    # Si l'extraction depuis S3 échoue, essayer en local
    local_dir = 'raw/welcome_jungle'
    return extract_from_local(local_dir)

def extract_pole_emploi_data():
    """
    Extrait les données Pôle Emploi depuis S3 ou en local.
    
    Returns:
        list: Liste des offres d'emploi extraites
    """
    # Essayer d'abord d'extraire depuis S3
    bucket_name = os.getenv('data_lake_bucket', 'data-lake-brut')
    prefix = 'raw/pole_emploi/'
    
    s3_data = extract_from_s3(bucket_name, prefix)
    if s3_data:
        return s3_data
    
    # Si l'extraction depuis S3 échoue, essayer en local
    local_dir = 'raw/pole_emploi'
    return extract_from_local(local_dir)

if __name__ == "__main__":
    # Test d'extraction
    welcome_jungle_data = extract_welcome_jungle_data()
    print(f"Nombre d'offres Welcome to the Jungle extraites: {len(welcome_jungle_data)}")
    
    pole_emploi_data = extract_pole_emploi_data()
    print(f"Nombre d'offres Pôle Emploi extraites: {len(pole_emploi_data)}")
