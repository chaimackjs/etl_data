#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Module d'extraction des données pour l'API France Travail.
Récupère et organise les données brutes depuis les fichiers JSON collectés
localement et sur AWS S3.
"""

import os
import json
import glob
import tempfile
import boto3
import logging
import pandas as pd
import sys
from datetime import datetime

# Ajouter le chemin du projet au PYTHONPATH
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Importer l'utilitaire de chargement des variables d'environnement
from etl.api.dotenv_utils import load_dotenv

# Charger les variables d'environnement
load_dotenv()

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/etl_extraction_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def get_s3_client():
    """
    Crée et retourne un client S3 avec les identifiants AWS depuis les variables d'environnement.
    Inclut un timeout pour éviter les blocages en cas de problème de connexion.
    
    Returns:
        boto3.client: Client S3 configuré ou None en cas d'erreur
    """
    try:
        # Récupérer les identifiants depuis les variables d'environnement
        access_key = os.environ.get('KEY_ACCESS')
        secret_key = os.environ.get('KEY_SECRET')
        bucket_name = os.environ.get('DATA_LAKE_BUCKET')
        
        # Vérifier que les identifiants sont disponibles
        if not access_key or not secret_key:
            logger.error("Variables d'environnement AWS manquantes (KEY_ACCESS, KEY_SECRET)")
            logger.info("Assurez-vous que ces variables sont définies dans votre environnement")
            return None
        
        import botocore.config
        # Configurer boto3 avec un timeout
        config = botocore.config.Config(
            connect_timeout=5,  # 5 secondes de timeout pour la connexion
            read_timeout=10,    # 10 secondes de timeout pour les opérations de lecture
            retries={'max_attempts': 2}  # Maximum 2 tentatives
        )
        
        # Créer le client S3 avec les identifiants des variables d'environnement
        s3_client = boto3.client(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name='eu-north-1',  # Région Nord de l'Europe
            config=config
        )
        
        # Tester la connexion avec un timeout
        from botocore.exceptions import ClientError
        try:
            s3_client.head_bucket(Bucket=bucket_name)
            logger.info("Connexion S3 établie avec succès")
            return s3_client
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '403':
                logger.error("Accès refusé au bucket S3 (erreur 403)")
            elif error_code == '404':
                logger.error("Le bucket S3 n'existe pas (erreur 404)")
            else:
                logger.error(f"Erreur lors de l'accès au bucket: {error_code}")
            return None
        
    except Exception as e:
        logger.error(f"Erreur lors de la création du client S3: {e}")
        return None

def list_s3_files(bucket_name=os.environ.get('data_lake_bucket', 'data-lake-brut'), prefix='raw/france_travail/'):
    """
    Liste tous les fichiers JSON disponibles dans le bucket S3.
    
    Args:
        bucket_name (str): Nom du bucket S3
        prefix (str): Préfixe pour filtrer les objets
        
    Returns:
        list: Liste des clés S3 des fichiers correspondants
    """
    s3_files = []
    s3_client = get_s3_client()
    
    if not s3_client:
        logger.warning("Client S3 non disponible, impossible de lister les fichiers S3")
        return s3_files
    
    try:
        # Lister tous les objets dans le bucket avec le préfixe
        paginator = s3_client.get_paginator('list_objects_v2')
        response_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
        
        for response in response_iterator:
            if 'Contents' in response:
                # Filtrer pour ne garder que les fichiers .json
                files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.json')]
                s3_files.extend(files)
        
        logger.info(f"Trouvé {len(s3_files)} fichiers dans S3 sous {prefix}")
        return s3_files
    
    except Exception as e:
        logger.error(f"Erreur lors de la recherche des fichiers sur S3: {e}")
        return []

def download_s3_file(bucket_name, file_key, local_dir=None):
    """
    Télécharge un fichier depuis S3 et le stocke temporairement.
    
    Args:
        bucket_name (str): Nom du bucket S3
        file_key (str): Clé du fichier S3
        local_dir (str): Répertoire où stocker le fichier téléchargé
        
    Returns:
        str: Chemin local du fichier téléchargé ou None si erreur
    """
    s3_client = get_s3_client()
    
    if not s3_client:
        logger.warning(f"Client S3 non disponible, impossible de télécharger {file_key}")
        return None
    
    try:
        # Déterminer le chemin de sortie
        if local_dir:
            os.makedirs(local_dir, exist_ok=True)
            target_file = os.path.join(local_dir, os.path.basename(file_key))
        else:
            # Créer un fichier temporaire si aucun répertoire n'est spécifié
            target_dir = tempfile.mkdtemp()
            target_file = os.path.join(target_dir, os.path.basename(file_key))
        
        # Télécharger le fichier
        logger.debug(f"Téléchargement de s3://{bucket_name}/{file_key} vers {target_file}")
        s3_client.download_file(bucket_name, file_key, target_file)
        return target_file
    
    except Exception as e:
        logger.error(f"Erreur lors du téléchargement de {file_key}: {e}")
        return None

def list_raw_data_files(data_dir="data/raw/france_travail", pattern="*.json"):
    """
    Liste tous les fichiers JSON collectés dans le répertoire des données brutes.
    
    Args:
        data_dir (str): Répertoire contenant les fichiers bruts
        pattern (str): Motif pour la sélection des fichiers
        
    Returns:
        list: Liste des chemins de fichiers correspondants
    """
    search_pattern = os.path.join(data_dir, pattern)
    files = glob.glob(search_pattern)
    logger.info(f"Trouvé {len(files)} fichiers locaux correspondant au motif {search_pattern}")
    return files

def read_json_file(file_path):
    """
    Lit un fichier JSON et retourne son contenu.
    
    Args:
        file_path (str): Chemin du fichier à lire
        
    Returns:
        dict: Contenu du fichier JSON
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        logger.debug(f"Fichier lu avec succès: {file_path}")
        return data
    except Exception as e:
        logger.error(f"Erreur lors de la lecture du fichier {file_path}: {e}")
        return None

def extract_job_offers(file_path):
    """
    Extrait les offres d'emploi d'un fichier JSON France Travail.
    
    Args:
        file_path (str): Chemin du fichier à traiter
        
    Returns:
        list: Liste des offres d'emploi extraites
    """
    data = read_json_file(file_path)
    if not data:
        return []
    
    # Extraire les résultats (offres d'emploi) du fichier JSON
    offers = data.get('resultats', [])
    logger.info(f"Extrait {len(offers)} offres du fichier {os.path.basename(file_path)}")
    return offers

def extract_jobs_to_dataframe(file_list=None):
    """
    Extrait toutes les offres d'emploi des fichiers spécifiés et les convertit en DataFrame.
    
    Args:
        file_list (list): Liste de chemins de fichiers à traiter
        
    Returns:
        pandas.DataFrame: DataFrame contenant toutes les offres d'emploi
    """
    # Si aucune liste de fichiers n'est fournie, obtenir tous les fichiers
    if not file_list:
        file_list = list_raw_data_files()
    
    if not file_list:
        logger.warning("Aucun fichier à traiter")
        return None
    
    # Collecter toutes les offres d'emploi
    all_offers = []
    job_ids = set()  # Pour éviter les doublons
    
    for file_path in file_list:
        offers = extract_job_offers(file_path)
        for offer in offers:
            # Vérifier si l'offre existe déjà (éviter les doublons)
            offer_id = offer.get('id')
            if offer_id and offer_id not in job_ids:
                job_ids.add(offer_id)
                all_offers.append(offer)
    
    # Convertir en DataFrame
    if all_offers:
        df = pd.DataFrame(all_offers)
        logger.info(f"Extrait un total de {len(df)} offres d'emploi uniques")
        return df
    else:
        logger.warning("Aucune offre d'emploi trouvée dans les fichiers traités")
        return None

def extract_by_date_range(start_date, end_date=None, include_s3=True):
    """
    Extrait les offres d'emploi collectées entre deux dates en combinant
    les sources locales et S3.
    
    Args:
        start_date (str): Date de début au format YYYYMMDD
        end_date (str): Date de fin au format YYYYMMDD, default=aujourd'hui
        include_s3 (bool): Si True, inclut les données de S3
        
    Returns:
        pandas.DataFrame: DataFrame contenant les offres d'emploi de la période
    """
    if not end_date:
        end_date = datetime.now().strftime("%Y%m%d")
    
    logger.info(f"Extraction des offres pour la période du {start_date} au {end_date}")
    
    # 1. Trouver les fichiers locaux correspondant à la plage de dates
    data_dir = "data/raw/france_travail"
    local_files = list_raw_data_files(data_dir)
    
    # Filtrer les fichiers locaux selon la plage de dates
    filtered_local_files = []
    for file_path in local_files:
        file_name = os.path.basename(file_path)
        
        # Extraire la date du nom de fichier (format: france_travail_*_YYYYMMDD_*)
        date_parts = file_name.split('_')
        for i, part in enumerate(date_parts):
            if len(part) == 8 and part.isdigit():
                file_date = part
                if start_date <= file_date <= end_date:
                    filtered_local_files.append(file_path)
                break
    
    logger.info(f"Trouvé {len(filtered_local_files)} fichiers locaux dans la période du {start_date} au {end_date}")
    
    # 2. Trouver les fichiers S3 correspondant à la plage de dates
    downloaded_s3_files = []
    if include_s3:
        # Liste des fichiers dans S3
        s3_files = list_s3_files()
        
        # Filtrer par plage de dates
        filtered_s3_files = []
        for s3_key in s3_files:
            file_name = os.path.basename(s3_key)
            
            # Extraire la date du nom de fichier
            date_parts = file_name.split('_')
            for i, part in enumerate(date_parts):
                if len(part) == 8 and part.isdigit():
                    file_date = part
                    if start_date <= file_date <= end_date:
                        filtered_s3_files.append(s3_key)
                    break
        
        logger.info(f"Trouvé {len(filtered_s3_files)} fichiers dans S3 pour la période spécifiée")
        
        # Télécharger les fichiers S3 manquants localement
        # Pour éviter les doublons, vérifier si le fichier existe déjà localement
        local_filenames = [os.path.basename(f) for f in filtered_local_files]
        
        for s3_key in filtered_s3_files:
            file_name = os.path.basename(s3_key)
            
            # Si le fichier n'existe pas déjà localement, le télécharger
            if file_name not in local_filenames:
                temp_dir = "data/temp/s3_downloads"
                downloaded_file = download_s3_file('data-lake-brut', s3_key, temp_dir)
                if downloaded_file:
                    downloaded_s3_files.append(downloaded_file)
                    logger.info(f"Fichier téléchargé depuis S3: {file_name}")
    
    # 3. Combiner les listes de fichiers et extraire les données
    all_files = filtered_local_files + downloaded_s3_files
    
    logger.info(f"Extraction à partir de {len(all_files)} fichiers au total")
    return extract_jobs_to_dataframe(all_files)

if __name__ == "__main__":
    # Test de la fonction d'extraction
    today = datetime.now().strftime("%Y%m%d")
    
    # Tester la connexion S3 et lister les fichiers
    print("=== Test de connexion S3 ===")
    s3_files = list_s3_files()
    print(f"Fichiers trouvés dans S3: {len(s3_files)}")
    
    # Extraire les données sur une période de 7 jours pour avoir plus de chances de trouver des fichiers
    import datetime as dt
    one_week_ago = (dt.datetime.now() - dt.timedelta(days=7)).strftime("%Y%m%d")
    print(f"\n=== Extraction des données du {one_week_ago} au {today} ===")
    
    df = extract_by_date_range(one_week_ago, today, include_s3=True)
    
    if df is not None:
        print(f"\nRésultats de l'extraction:")
        print(f"- Colonnes disponibles: {df.columns.tolist()}")
        print(f"- Nombre d'offres: {len(df)}")
        print(f"- Premières offres: ")
        if 'intitule' in df.columns:
            print(df[['intitule']].head())
    else:
        print("\nAucune donnée extraite sur la période.")
        
    print("\nTest d'extraction terminé.")
        
    # Supprimer les fichiers temporaires téléchargés
    import shutil
    if os.path.exists("data/temp/s3_downloads"):
        shutil.rmtree("data/temp/s3_downloads")
        print("Fichiers temporaires nettoyés.")

