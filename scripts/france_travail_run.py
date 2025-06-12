#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script spécifique pour récupérer un maximum d'offres d'emploi depuis l'API France Travail
sans filtres particuliers.

Ce script optimise la collecte en utilisant l'API de manière efficace
et stocke les résultats à la fois localement et sur S3.
"""

import os
import sys
import time
import logging
import argparse
from datetime import datetime

# Ajouter le chemin du projet à PYTHONPATH
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.data_collection.apis.pole_emploi import get_access_token, search_jobs, save_jobs_to_file, upload_to_s3, is_data_already_collected

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/collect_all_jobs_{}.log".format(datetime.now().strftime("%Y%m%d"))),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def parse_args():
    """Parse les arguments de la ligne de commande"""
    parser = argparse.ArgumentParser(
        description="Collecte toutes les offres d'emploi disponibles depuis l'API France Travail"
    )
    
    parser.add_argument(
        "--max-pages",
        type=int,
        default=100,
        help="Nombre maximum de pages à récupérer (défaut: 100)"
    )
    
    parser.add_argument(
        "--delay",
        type=float,
        default=1.0,
        help="Délai en secondes entre les requêtes (défaut: 1.0)"
    )
    
    parser.add_argument(
        "--no-s3",
        action="store_true",
        help="Ne pas télécharger les fichiers vers S3"
    )
    
    parser.add_argument(
        "--force",
        action="store_true",
        help="Forcer la collecte même si des données ont déjà été collectées aujourd'hui"
    )
    
    return parser.parse_args()

def collect_all_jobs(max_pages=100, delay=1.0, upload_s3=True, force_collect=False):
    """
    Collecte toutes les offres d'emploi disponibles sur France Travail
    
    Args:
        max_pages (int): Nombre maximum de pages à récupérer
        delay (float): Délai en secondes entre les requêtes
        upload_s3 (bool): Si True, télécharge les données vers S3
        force_collect (bool): Si True, force la collecte même si des données existent déjà
        
    Returns:
        tuple: (total_offres, saved_files)
    """
    # S'assurer que les répertoires existent
    os.makedirs("data/raw/france_travail", exist_ok=True)
    os.makedirs("logs", exist_ok=True)
    
    # Date du jour au format YYYYMMDD pour vérifier les données existantes
    today_str = datetime.now().strftime("%Y%m%d")
    
    # Vérifier si des données ont déjà été collectées aujourd'hui
    if not force_collect:
        already_collected, existing_files = is_data_already_collected(today_str)
        if already_collected:
            logger.info(f"Des données ont déjà été collectées aujourd'hui ({len(existing_files)} fichiers).")
            logger.info("Utilisez l'option --force pour forcer une nouvelle collecte.")
            return 0, existing_files
    
    # Obtenir un token d'accès
    logger.info("Demande d'un token d'accès à l'API France Travail")
    access_token = get_access_token()
    
    if not access_token:
        logger.error("Impossible d'obtenir un token d'accès. Arrêt de la collecte.")
        return 0, []
    
    # Récupérer le nom du bucket
    bucket_name = os.getenv('data_lake_bucket', 'data-lake-brut')
    
    # Variables pour la pagination
    total_offres = 0
    saved_files = []
    
    logger.info(f"Début de la collecte de toutes les offres d'emploi (max {max_pages} pages)")
    logger.info(f"Date: {today_str}")
    
    # Créer un ensensble pour stocker les IDs d'offres déjà collectées
    collected_ids = set()
    
    # Parcourir toutes les pages
    for page in range(1, max_pages + 1):
        logger.info(f"\n=== Collecte des offres - Page {page}/{max_pages} ===")
        
        # Rechercher les offres sans filtres
        results = search_jobs(
            access_token=access_token,
            page=page,
            per_page=150
        )
        
        if not results:
            logger.error(f"Aucun résultat pour la page {page}. Arrêt de la pagination.")
            break
            
        # Compter les offres
        resultats = results.get('resultats', [])
        nb_offres = len(resultats)
        
        # Si aucune offre n'est trouvée, arrêter
        if nb_offres == 0:
            logger.warning(f"Page {page}: Aucune offre trouvée. Fin de la collecte.")
            break
            
        total_offres += nb_offres
        logger.info(f"Page {page}: {nb_offres} offres trouvées. Total cumulé: {total_offres}")
        
        # Sauvegarder les résultats localement
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"data/raw/france_travail/france_travail_all_{timestamp}_p{page}.json"
        
        try:
            # Sauvegarder localement
            local_file_path = save_jobs_to_file(results, "all", page)
            
            if local_file_path:
                saved_files.append(local_file_path)
                logger.info(f"Données sauvegardées dans {local_file_path}")
                
                # Télécharger vers S3 si demandé
                if upload_s3:
                    s3_success = upload_to_s3(local_file_path, bucket_name)
                    if s3_success:
                        logger.info(f"Fichier téléchargé vers S3: {local_file_path}")
                    else:
                        logger.error(f"Erreur lors du téléchargement vers S3: {local_file_path}")
        except Exception as e:
            logger.error(f"Erreur lors de la sauvegarde des données: {e}")
        
        # Vérifier si on a atteint la fin des résultats
        if nb_offres < 150:
            logger.info(f"Moins de 150 offres récupérées, fin de la pagination.")
            break
            
        # Pause pour éviter de surcharger l'API
        logger.info(f"Pause de {delay} secondes avant la prochaine requête...")
        time.sleep(delay)
        
        # Tous les 10 pages, renouveler le token pour éviter qu'il n'expire
        if page % 10 == 0:
            logger.info("Renouvellement du token d'accès...")
            new_token = get_access_token()
            if new_token:
                access_token = new_token
                logger.info("Token d'accès renouvelé avec succès")
            else:
                logger.warning("Échec du renouvellement du token, utilisation de l'ancien token")
    
    logger.info(f"\n=== Collecte terminée ===")
    logger.info(f"Total des offres collectées: {total_offres}")
    logger.info(f"Nombre de fichiers sauvegardés: {len(saved_files)}")
    
    return total_offres, saved_files

def parse_args():
    """Parse les arguments de la ligne de commande"""
    parser = argparse.ArgumentParser(
        description="Collecte toutes les offres d'emploi disponibles depuis l'API France Travail"
    )
    
    parser.add_argument(
        "--max-pages",
        type=int,
        default=100,
        help="Nombre maximum de pages à récupérer (défaut: 100)"
    )
    
    parser.add_argument(
        "--delay",
        type=float,
        default=1.0,
        help="Délai en secondes entre les requêtes (défaut: 1.0)"
    )
    
    parser.add_argument(
        "--no-s3",
        action="store_true",
        help="Ne pas télécharger les fichiers vers S3"
    )
    
    parser.add_argument(
        "--force",
        action="store_true",
        help="Forcer la collecte même si des données ont déjà été collectées aujourd'hui"
    )
    
    return parser.parse_args()

def main():
    """Fonction principale"""
    start_time = datetime.now()
    logger.info(f"=== Démarrage de la collecte complète des offres d'emploi France Travail ===")
    logger.info(f"Date et heure de début: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Récupérer les arguments
    args = parse_args()
    
    try:
        # Vérifier si on doit forcer la collecte
        if args.force:
            logger.info("Option --force activée: collecte forcée même si des données existent déjà")
        
        # Collecter toutes les offres
        total, files = collect_all_jobs(
            max_pages=args.max_pages,
            delay=args.delay,
            upload_s3=not args.no_s3,
            force_collect=args.force
        )
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        # Si total est 0 mais qu'on a des fichiers, c'est qu'on a utilisé des données existantes
        if total == 0 and files:
            logger.info(f"=== Aucune nouvelle collecte effectuée - Données existantes utilisées ===")
            logger.info(f"Nombre de fichiers existants: {len(files)}")
        else:
            logger.info(f"=== Fin de la collecte des offres d'emploi France Travail ===")
            logger.info(f"Date et heure de fin: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"Durée totale: {duration}")
            logger.info(f"Total des offres collectées: {total}")
            logger.info(f"Nombre de fichiers sauvegardés: {len(files)}")
        
        return 0
        
    except Exception as e:
        logger.exception(f"Erreur lors de la collecte des offres: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
