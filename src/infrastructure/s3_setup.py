#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Module de configuration et d'interaction avec AWS S3.
"""

import os
import logging
import boto3
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

def create_s3_bucket(session, bucket_name, region='eu-north-1'):
    """
    Crée un bucket S3 s'il n'existe pas déjà.
    
    Args:
        session (boto3.Session): Session AWS
        bucket_name (str): Nom du bucket
        region (str): Région AWS
    
    Returns:
        bool: True si le bucket a été créé ou existe déjà, False sinon
    """
    s3 = session.client('s3')
    
    try:
        # Vérifier si le bucket existe déjà
        s3.head_bucket(Bucket=bucket_name)
        logger.info(f"Le bucket {bucket_name} existe déjà")
        return True
    except:
        # Créer le bucket
        try:
            s3.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={'LocationConstraint': region}
            )
            logger.info(f"Bucket {bucket_name} créé avec succès")
            return True
        except Exception as e:
            logger.error(f"Erreur lors de la création du bucket S3: {e}")
            return False

def create_s3_folders(session, bucket_name):
    """
    Crée la structure de dossiers dans le bucket S3.
    
    Args:
        session (boto3.Session): Session AWS
        bucket_name (str): Nom du bucket
    
    Returns:
        bool: True si les dossiers ont été créés avec succès, False sinon
    """
    s3 = session.client('s3')
    
    # Définir la structure de dossiers
    folders = [
        'raw/',
        'processed/',
        'curated/',
        'raw/welcome_jungle/',
        'raw/pole_emploi/',
        'processed/jobs/',
        'processed/skills/',
        'curated/analytics/'
    ]
    
    try:
        # Créer chaque dossier
        for folder in folders:
            s3.put_object(Bucket=bucket_name, Key=folder)
        
        logger.info(f"Structure de dossiers créée avec succès dans le bucket {bucket_name}")
        return True
    except Exception as e:
        logger.error(f"Erreur lors de la création des dossiers dans le bucket S3: {e}")
        return False

def upload_file_to_s3(session, file_path, bucket_name, s3_path=None):
    """
    Télécharge un fichier vers le bucket S3.
    
    Args:
        session (boto3.Session): Session AWS
        file_path (str): Chemin local du fichier à télécharger
        bucket_name (str): Nom du bucket
        s3_path (str, optional): Chemin dans S3. Si None, utilise le nom du fichier.
    
    Returns:
        bool: True si le fichier a été téléchargé avec succès, False sinon
    """
    s3 = session.client('s3')
    
    # Si s3_path n'est pas spécifié, utiliser le nom du fichier
    if s3_path is None:
        s3_path = os.path.basename(file_path)
    
    try:
        s3.upload_file(file_path, bucket_name, s3_path)
        logger.info(f"Fichier {file_path} téléchargé avec succès vers s3://{bucket_name}/{s3_path}")
        return True
    except Exception as e:
        logger.error(f"Erreur lors du téléchargement du fichier vers S3: {e}")
        return False

def download_file_from_s3(session, bucket_name, s3_path, local_path):
    """
    Télécharge un fichier depuis le bucket S3.
    
    Args:
        session (boto3.Session): Session AWS
        bucket_name (str): Nom du bucket
        s3_path (str): Chemin du fichier dans S3
        local_path (str): Chemin local où sauvegarder le fichier
    
    Returns:
        bool: True si le fichier a été téléchargé avec succès, False sinon
    """
    s3 = session.client('s3')
    
    try:
        # Créer le dossier local si nécessaire
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        
        s3.download_file(bucket_name, s3_path, local_path)
        logger.info(f"Fichier s3://{bucket_name}/{s3_path} téléchargé avec succès vers {local_path}")
        return True
    except Exception as e:
        logger.error(f"Erreur lors du téléchargement du fichier depuis S3: {e}")
        return False

def list_s3_files(session, bucket_name, prefix=''):
    """
    Liste les fichiers dans un bucket S3.
    
    Args:
        session (boto3.Session): Session AWS
        bucket_name (str): Nom du bucket
        prefix (str, optional): Préfixe pour filtrer les résultats
    
    Returns:
        list: Liste des fichiers dans le bucket
    """
    s3 = session.client('s3')
    
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        
        if 'Contents' in response:
            files = [obj['Key'] for obj in response['Contents']]
            logger.info(f"{len(files)} fichiers trouvés dans s3://{bucket_name}/{prefix}")
            return files
        else:
            logger.info(f"Aucun fichier trouvé dans s3://{bucket_name}/{prefix}")
            return []
    except Exception as e:
        logger.error(f"Erreur lors de la liste des fichiers dans S3: {e}")
        return []

def create_aws_session():
    """
    Crée une session AWS avec les identifiants des variables d'environnement.
    
    Returns:
        boto3.Session: Session AWS
    """
    try:
        # Récupérer les identifiants AWS
        aws_access_key = os.getenv('KEY_ACCESS')
        aws_secret_key = os.getenv('KEY_SECRET')
        
        if not aws_access_key or not aws_secret_key:
            logger.error("Identifiants AWS manquants. Vérifiez les variables d'environnement KEY_ACCESS et KEY_SECRET.")
            return None
        
        # Créer la session
        session = boto3.Session(
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key
        )
        
        logger.info("Session AWS créée avec succès")
        return session
    except Exception as e:
        logger.error(f"Erreur lors de la création de la session AWS: {e}")
        return None

def setup_s3():
    """
    Configure S3 pour le projet.
    
    Returns:
        bool: True si la configuration a réussi, False sinon
    """
    # Créer la session AWS
    session = create_aws_session()
    if not session:
        return False
    
    # Récupérer le nom du bucket
    bucket_name = os.getenv('data_lake_bucket', 'data-lake-brut')
    
    # Créer le bucket
    if not create_s3_bucket(session, bucket_name):
        return False
    
    # Créer la structure de dossiers
    if not create_s3_folders(session, bucket_name):
        return False
    
    logger.info(f"Configuration S3 terminée avec succès pour le bucket {bucket_name}")
    return True

if __name__ == "__main__":
    # Test de configuration S3
    setup_s3()
