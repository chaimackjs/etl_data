#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Pipeline ETL complet pour le traitement des données d'offres d'emploi.
Ce script exécute l'ensemble du processus ETL : extraction, transformation et chargement.
"""

import os
import logging
import argparse
from datetime import datetime
from dotenv import load_dotenv

# Importer les modules ETL
from src.etl.extraction import extract_welcome_jungle_data, extract_pole_emploi_data
from src.etl.transformation import transform_to_dataframe
from src.etl.loading import load_to_s3, load_jobs_to_rds, load_skills_to_rds, save_to_local

# Charger les variables d'environnement
load_dotenv()

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/etl_pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_welcome_jungle_pipeline(search_terms=None, max_pages=3, save_local=True, upload_s3=True, load_rds=True):
    """
    Exécute le pipeline ETL pour les données de Welcome to the Jungle.
    
    Args:
        search_terms (list): Liste des termes de recherche
        max_pages (int): Nombre maximum de pages à scraper par terme
        save_local (bool): Sauvegarder les données en local
        upload_s3 (bool): Uploader les données vers S3
        load_rds (bool): Charger les données dans RDS
    
    Returns:
        bool: True si le pipeline a réussi, False sinon
    """
    try:
        # Termes de recherche par défaut
        if search_terms is None:
            search_terms = [
                "data scientist", "data engineer", "data analyst", 
                "machine learning", "python", "aws", "cloud"
            ]
        
        logger.info(f"Démarrage du pipeline ETL pour Welcome to the Jungle avec les termes: {search_terms}")
        
        # Extraction
        logger.info("Étape 1: Extraction des données")
        jobs_data = extract_welcome_jungle_data(search_terms=search_terms, max_pages=max_pages)
        
        if not jobs_data:
            logger.warning("Aucune donnée extraite de Welcome to the Jungle")
            return False
        
        logger.info(f"{len(jobs_data)} offres d'emploi extraites de Welcome to the Jungle")
        
        # Transformation
        logger.info("Étape 2: Transformation des données")
        jobs_df = transform_to_dataframe(jobs_data)
        
        # Création du timestamp pour les noms de fichiers
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Chargement
        logger.info("Étape 3: Chargement des données")
        success = True
        
        # Sauvegarde locale
        if save_local:
            local_path = f"data/processed/welcome_jungle/processed_jobs_{timestamp}.json"
            if save_to_local(jobs_df, local_path):
                logger.info(f"Données sauvegardées en local: {local_path}")
            else:
                logger.error("Échec de la sauvegarde locale")
                success = False
        
        # Upload vers S3
        if upload_s3:
            bucket_name = os.getenv('data_lake_bucket', 'data-lake-brut')
            s3_path = f"processed/welcome_jungle/processed_jobs_{timestamp}.json"
            if load_to_s3(jobs_df, bucket_name, s3_path):
                logger.info(f"Données uploadées vers S3: s3://{bucket_name}/{s3_path}")
            else:
                logger.error("Échec de l'upload vers S3")
                success = False
        
        # Chargement dans RDS
        if load_rds:
            try:
                if load_jobs_to_rds(jobs_df):
                    logger.info("Offres d'emploi chargées dans RDS")
                else:
                    logger.error("Échec du chargement des offres dans RDS")
                    success = False
                
                if load_skills_to_rds(jobs_df):
                    logger.info("Compétences chargées dans RDS")
                else:
                    logger.error("Échec du chargement des compétences dans RDS")
                    success = False
            except Exception as e:
                logger.error(f"Erreur lors du chargement dans RDS: {e}")
                success = False
        
        if success:
            logger.info("Pipeline ETL Welcome to the Jungle exécuté avec succès")
        else:
            logger.warning("Pipeline ETL Welcome to the Jungle terminé avec des avertissements ou erreurs")
        
        return success
    
    except Exception as e:
        logger.error(f"Erreur lors de l'exécution du pipeline ETL Welcome to the Jungle: {e}", exc_info=True)
        return False

def run_pole_emploi_pipeline(save_local=True, upload_s3=True, load_rds=True):
    """
    Exécute le pipeline ETL pour les données de Pôle Emploi.
    
    Args:
        save_local (bool): Sauvegarder les données en local
        upload_s3 (bool): Uploader les données vers S3
        load_rds (bool): Charger les données dans RDS
    
    Returns:
        bool: True si le pipeline a réussi, False sinon
    """
    try:
        logger.info("Démarrage du pipeline ETL pour Pôle Emploi")
        
        # Extraction
        logger.info("Étape 1: Extraction des données")
        jobs_data = extract_pole_emploi_data()
        
        if not jobs_data:
            logger.warning("Aucune donnée extraite de Pôle Emploi")
            return False
        
        logger.info(f"{len(jobs_data)} offres d'emploi extraites de Pôle Emploi")
        
        # Transformation
        logger.info("Étape 2: Transformation des données")
        jobs_df = transform_to_dataframe(jobs_data)
        
        # Création du timestamp pour les noms de fichiers
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Chargement
        logger.info("Étape 3: Chargement des données")
        success = True
        
        # Sauvegarde locale
        if save_local:
            local_path = f"data/processed/pole_emploi/processed_jobs_{timestamp}.json"
            if save_to_local(jobs_df, local_path):
                logger.info(f"Données sauvegardées en local: {local_path}")
            else:
                logger.error("Échec de la sauvegarde locale")
                success = False
        
        # Upload vers S3
        if upload_s3:
            bucket_name = os.getenv('data_lake_bucket', 'data-lake-brut')
            s3_path = f"processed/pole_emploi/processed_jobs_{timestamp}.json"
            if load_to_s3(jobs_df, bucket_name, s3_path):
                logger.info(f"Données uploadées vers S3: s3://{bucket_name}/{s3_path}")
            else:
                logger.error("Échec de l'upload vers S3")
                success = False
        
        # Chargement dans RDS
        if load_rds:
            try:
                if load_jobs_to_rds(jobs_df):
                    logger.info("Offres d'emploi chargées dans RDS")
                else:
                    logger.error("Échec du chargement des offres dans RDS")
                    success = False
                
                if load_skills_to_rds(jobs_df):
                    logger.info("Compétences chargées dans RDS")
                else:
                    logger.error("Échec du chargement des compétences dans RDS")
                    success = False
            except Exception as e:
                logger.error(f"Erreur lors du chargement dans RDS: {e}")
                success = False
        
        if success:
            logger.info("Pipeline ETL Pôle Emploi exécuté avec succès")
        else:
            logger.warning("Pipeline ETL Pôle Emploi terminé avec des avertissements ou erreurs")
        
        return success
    
    except Exception as e:
        logger.error(f"Erreur lors de l'exécution du pipeline ETL Pôle Emploi: {e}", exc_info=True)
        return False

def main():
    """
    Fonction principale pour exécuter le pipeline ETL.
    """
    # Créer le parser d'arguments
    parser = argparse.ArgumentParser(description='Pipeline ETL pour les offres d\'emploi')
    parser.add_argument('--source', choices=['welcome_jungle', 'pole_emploi', 'all'], default='all',
                        help='Source des données (welcome_jungle, pole_emploi, all)')
    parser.add_argument('--search-terms', nargs='+',
                        help='Termes de recherche pour Welcome to the Jungle')
    parser.add_argument('--max-pages', type=int, default=3,
                        help='Nombre maximum de pages à scraper par terme pour Welcome to the Jungle')
    parser.add_argument('--no-local', action='store_true',
                        help='Ne pas sauvegarder les données en local')
    parser.add_argument('--no-s3', action='store_true',
                        help='Ne pas uploader les données vers S3')
    parser.add_argument('--no-rds', action='store_true',
                        help='Ne pas charger les données dans RDS')
    
    # Parser les arguments
    args = parser.parse_args()
    
    # Créer le dossier de logs s'il n'existe pas
    os.makedirs('logs', exist_ok=True)
    
    # Exécuter le pipeline selon la source
    success = True
    
    if args.source in ['welcome_jungle', 'all']:
        wj_success = run_welcome_jungle_pipeline(
            search_terms=args.search_terms,
            max_pages=args.max_pages,
            save_local=not args.no_local,
            upload_s3=not args.no_s3,
            load_rds=not args.no_rds
        )
        success = success and wj_success
    
    if args.source in ['pole_emploi', 'all']:
        pe_success = run_pole_emploi_pipeline(
            save_local=not args.no_local,
            upload_s3=not args.no_s3,
            load_rds=not args.no_rds
        )
        success = success and pe_success
    
    # Retourner le code de sortie
    return 0 if success else 1

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)
