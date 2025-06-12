#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script principal d'exécution du pipeline ETL.
Ce script orchestre l'extraction, la transformation et le chargement
des données provenant de toutes les sources (France Travail et Welcome to the Jungle).
"""

import os
import sys
import argparse
import logging
from datetime import datetime, timedelta

# Configurer le logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/etl_pipeline_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Ajouter le chemin du projet au PYTHONPATH
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from etl.api.dotenv_utils import load_dotenv

# Charger les variables d'environnement
load_dotenv()

def run_france_travail_pipeline(start_date=None, end_date=None):
    """
    Exécute le pipeline ETL pour les données France Travail.
    
    Args:
        start_date (str): Date de début au format YYYYMMDD
        end_date (str): Date de fin au format YYYYMMDD
        
    Returns:
        int: Nombre d'enregistrements traités
    """
    from etl.api.loading import execute_etl_pipeline
    
    logger.info(f"Démarrage du pipeline France Travail ({start_date} - {end_date})")
    records = execute_etl_pipeline(start_date, end_date)
    logger.info(f"Pipeline France Travail terminé: {records} enregistrements traités")
    
    return records

def run_welcome_jungle_pipeline():
    """
    Exécute le pipeline ETL pour les données Welcome to the Jungle.
    
    Returns:
        int: Nombre d'enregistrements traités
    """
    from etl.scrapers.etl_pipeline import execute_etl_pipeline
    
    logger.info("Démarrage du pipeline Welcome to the Jungle")
    records = execute_etl_pipeline()
    logger.info(f"Pipeline Welcome to the Jungle terminé: {records} enregistrements traités")
    
    return records

def test_database_connection():
    """
    Teste la connexion à la base de données et affiche les informations de diagnostic.
    
    Returns:
        bool: True si la connexion est établie, False sinon
    """
    from etl.api.loading import get_db_connection
    
    logger.info("Test de connexion à la base de données...")
    engine = get_db_connection()
    
    if engine is not None:
        logger.info("Connexion à la base de données établie avec succès")
        return True
    else:
        logger.error("Échec de connexion à la base de données")
        
        # Afficher des informations de diagnostic
        db_host = os.getenv('DB_HOST')
        db_port = os.getenv('DB_PORT')
        db_name = os.getenv('DB_NAME')
        logger.info(f"Informations de connexion: Host={db_host}, Port={db_port}, DB={db_name}")
        
        # Suggestions pour résoudre le problème
        logger.info("Suggestions de dépannage:")
        logger.info("1. Vérifiez que l'instance RDS est en cours d'exécution")
        logger.info("2. Assurez-vous que le groupe de sécurité autorise les connexions depuis votre adresse IP")
        logger.info("3. Vérifiez que la base de données est configurée pour autoriser l'accès public")
        logger.info("4. Confirmez que les identifiants sont corrects")
        
        return False

def run_full_etl_pipeline(sources, start_date=None, end_date=None):
    """
    Exécute le pipeline ETL complet pour toutes les sources spécifiées.
    
    Args:
        sources (list): Liste des sources à traiter ('all', 'france_travail', 'welcome_jungle')
        start_date (str): Date de début au format YYYYMMDD
        end_date (str): Date de fin au format YYYYMMDD
    
    Returns:
        dict: Résultats par source
    """
    # Si aucune date n'est spécifiée, utiliser la date d'aujourd'hui
    if not end_date:
        end_date = datetime.now().strftime("%Y%m%d")
    
    # Si seule la date de fin est spécifiée, utiliser une semaine avant comme date de début
    if not start_date:
        start_date = (datetime.now() - timedelta(days=7)).strftime("%Y%m%d")
    
    logger.info(f"Démarrage du pipeline ETL complet pour la période {start_date} - {end_date}")
    
    # Tester la connexion à la base de données avant de commencer
    if not test_database_connection():
        logger.warning("Le pipeline continuera mais le chargement risque d'échouer")
    
    results = {}
    
    # Traiter France Travail
    if 'all' in sources or 'france_travail' in sources:
        try:
            results['france_travail'] = run_france_travail_pipeline(start_date, end_date)
        except Exception as e:
            logger.error(f"Erreur dans le pipeline France Travail: {e}")
            results['france_travail'] = 0
    
    # Traiter Welcome to the Jungle
    if 'all' in sources or 'welcome_jungle' in sources:
        try:
            results['welcome_jungle'] = run_welcome_jungle_pipeline()
        except Exception as e:
            logger.error(f"Erreur dans le pipeline Welcome to the Jungle: {e}")
            results['welcome_jungle'] = 0
    
    # Afficher un résumé des résultats
    logger.info("=== Résumé du pipeline ETL ===")
    for source, count in results.items():
        logger.info(f"{source}: {count} enregistrements traités")
    
    return results

if __name__ == "__main__":
    # Définir les arguments de la ligne de commande
    parser = argparse.ArgumentParser(description="Exécution du pipeline ETL pour les offres d'emploi")
    parser.add_argument('--sources', type=str, nargs='+', default=['all'], 
                        choices=['all', 'france_travail', 'welcome_jungle'],
                        help="Sources de données à traiter")
    parser.add_argument('--start-date', type=str, help="Date de début au format YYYYMMDD")
    parser.add_argument('--end-date', type=str, help="Date de fin au format YYYYMMDD")
    parser.add_argument('--test-db', action='store_true', help="Tester uniquement la connexion à la base de données")
    
    args = parser.parse_args()
    
    # Si l'option test-db est spécifiée, tester uniquement la connexion
    if args.test_db:
        test_database_connection()
        sys.exit(0)
    
    # Exécuter le pipeline
    run_full_etl_pipeline(args.sources, args.start_date, args.end_date)
