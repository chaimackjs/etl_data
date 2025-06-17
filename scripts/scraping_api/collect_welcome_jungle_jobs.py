#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script spécifique pour récupérer les offres d'emploi depuis Welcome to the Jungle
et les stocker à la fois localement et sur S3.

Ce script optimise la collecte en utilisant Selenium de manière efficace
et respecte l'architecture du projet.
"""

import os
import sys
import time
import logging
import argparse
from datetime import datetime

# Ajouter le chemin du projet à PYTHONPATH
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.data_extraction.scrapers.welcome_jungle import scrape_welcome_jungle

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/collect_welcome_jungle_{}.log".format(datetime.now().strftime("%Y%m%d"))),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def parse_args():
    """Parse les arguments de la ligne de commande"""
    parser = argparse.ArgumentParser(
        description="Collecte les offres d'emploi depuis Welcome to the Jungle"
    )
    
    parser.add_argument(
        "--query",
        type=str,
        default="",
        help="Terme de recherche (vide pour toutes les offres)"
    )
    
    parser.add_argument(
        "--location",
        type=str,
        default="",
        help="Localisation (vide pour toutes les localisations)"
    )
    
    parser.add_argument(
        "--max-pages",
        type=int,
        default=10,
        help="Nombre maximum de pages à récupérer (défaut: 10)"
    )
    
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Exécuter le navigateur en mode headless"
    )
    
    parser.add_argument(
        "--no-s3",
        action="store_true",
        help="Ne pas télécharger les fichiers vers S3"
    )
    
    parser.add_argument(
        "--save-locally",
        action="store_true",
        help="Sauvegarder les données en local (désactivé par défaut)"
    )
    
    parser.add_argument(
        "--force",
        action="store_true",
        help="Forcer la collecte même si des données ont déjà été collectées aujourd'hui"
    )
    
    return parser.parse_args()

def is_data_already_collected(query="", location=""):
    """
    Vérifie si des données ont déjà été collectées aujourd'hui
    
    Args:
        query (str): Terme de recherche
        location (str): Localisation
        
    Returns:
        tuple: (bool, list) - Si des données existent déjà et la liste des fichiers existants
    """
    today_str = datetime.now().strftime("%Y%m%d")
    raw_dir = os.path.join("data", "raw", "welcome_jungle")
    
    if not os.path.exists(raw_dir):
        return False, []
    
    existing_files = []
    for file in os.listdir(raw_dir):
        if file.endswith(".json") and today_str in file:
            # Vérifier si le fichier correspond à la requête et à la localisation
            query_match = True
            location_match = True
            
            if query and query.replace(" ", "_") not in file:
                query_match = False
            
            if location and location.replace(" ", "_") not in file:
                location_match = False
            
            if query_match and location_match:
                existing_files.append(os.path.join(raw_dir, file))
    
    return len(existing_files) > 0, existing_files

def collect_welcome_jungle_jobs(query="", location="", max_pages=10, headless=True, upload_s3=True, save_locally=False, force_collect=False):
    """
    Collecte les offres d'emploi depuis Welcome to the Jungle
    
    Args:
        query (str): Terme de recherche
        location (str): Localisation
        max_pages (int): Nombre maximum de pages à récupérer
        headless (bool): Si True, exécute le navigateur en mode headless
        upload_s3 (bool): Si True, télécharge les données vers S3
        save_locally (bool): Si True, sauvegarde les données en local (désactivé par défaut)
        force_collect (bool): Si True, force la collecte même si des données existent déjà
        
    Returns:
        list: Liste des offres d'emploi collectées
    """
    # S'assurer que les répertoires de logs existent
    os.makedirs("logs", exist_ok=True)
    
    # Créer le répertoire local uniquement si on sauvegarde en local
    if save_locally:
        os.makedirs("data/raw/welcome_jungle", exist_ok=True)
    
    # Vérifier si des données ont déjà été collectées aujourd'hui
    if not force_collect:
        already_collected, existing_files = is_data_already_collected(query, location)
        if already_collected:
            logger.info(f"Des données ont déjà été collectées aujourd'hui ({len(existing_files)} fichiers).")
            logger.info("Utilisez l'option --force pour forcer une nouvelle collecte.")
            return []
    
    # Charger les variables d'environnement depuis le fichier .env
    from dotenv import load_dotenv
    load_dotenv()
    
    logger.info(f"Début de la collecte des offres d'emploi Welcome to the Jungle")
    query_msg = f"'{query}'" if query else "toutes les offres"
    location_msg = f"à '{location}'" if location else "toutes localisations"
    logger.info(f"Recherche de {query_msg} {location_msg} sur {max_pages} pages...")
    
    try:
        # Exécution du scraper
        jobs = scrape_welcome_jungle(
            query=query,
            location=location,
            max_pages=max_pages,
            headless=headless,
            upload_to_aws=upload_s3,
            save_locally=save_locally
        )
        
        logger.info(f"Collecte terminée avec succès: {len(jobs)} offres d'emploi trouvées")
        return jobs
        
    except Exception as e:
        logger.exception(f"Erreur lors de la collecte des offres: {e}")
        return []

def main():
    """Fonction principale"""
    start_time = datetime.now()
    logger.info(f"=== Démarrage de la collecte des offres d'emploi Welcome to the Jungle ===")
    logger.info(f"Date et heure de début: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Récupérer les arguments
    args = parse_args()
    
    try:
        # Vérifier si on doit forcer la collecte
        if args.force:
            logger.info("Option --force activée: collecte forcée même si des données existent déjà")
        
        # Collecter les offres
        jobs = collect_welcome_jungle_jobs(
            query=args.query,
            location=args.location,
            max_pages=args.max_pages,
            headless=args.headless,
            upload_s3=not args.no_s3,
            save_locally=args.save_locally,
            force_collect=args.force
        )
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        if not jobs:
            logger.info(f"=== Aucune nouvelle collecte effectuée - Données existantes utilisées ou erreur ===")
        else:
            logger.info(f"=== Fin de la collecte des offres d'emploi Welcome to the Jungle ===")
            logger.info(f"Date et heure de fin: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"Durée totale: {duration}")
            logger.info(f"Total des offres collectées: {len(jobs)}")
        
        return 0
        
    except Exception as e:
        logger.exception(f"Erreur lors de la collecte des offres: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
