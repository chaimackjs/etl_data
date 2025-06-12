"""
Module pour exécuter plusieurs recherches sur Welcome to the Jungle.
Ce module permet de lancer des recherches avec différentes combinaisons
de mots-clés et lieux, puis de fusionner les résultats.
"""

import os
import argparse
import logging
from datetime import datetime
from itertools import product

# Configuration du logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('wttj_multi_search')

def run_multiple_searches(keywords, locations, max_pages=None, headless=True):
    """
    Exécute plusieurs recherches avec différentes combinaisons de mots-clés et lieux.
    
    Args:
        keywords: Liste de mots-clés de recherche
        locations: Liste de lieux de recherche
        max_pages: Nombre maximum de pages à parcourir par recherche
        headless: Booléen indiquant si le navigateur doit être exécuté en mode headless
    
    Returns:
        tuple: (nombre total d'offres extraites, liste des chemins des fichiers JSON générés)
    """
    # Import ici pour éviter les imports circulaires
    from wttj_scraper import scrape_welcome_jungle
    
    json_files = []
    total_jobs = 0
    
    # Générer toutes les combinaisons possibles de mots-clés et lieux
    combinations = list(product(keywords, locations))
    
    logger.info(f"Démarrage de {len(combinations)} recherches avec les combinaisons suivantes:")
    for i, (keyword, location) in enumerate(combinations):
        logger.info(f"Recherche {i+1}/{len(combinations)}: Mot-clé='{keyword}', Lieu='{location}'")
        
        # Exécuter la recherche
        job_data, json_path = scrape_welcome_jungle(keyword, location, max_pages, headless)
        
        if json_path:
            json_files.append(json_path)
            total_jobs += len(job_data)
            logger.info(f"Recherche {i+1} terminée: {len(job_data)} offres extraites")
        else:
            logger.warning(f"Recherche {i+1} échouée ou aucune offre trouvée")
    
    logger.info(f"Toutes les recherches sont terminées. {total_jobs} offres extraites au total.")
    return total_jobs, json_files

def merge_search_results(json_files, output_file=None):
    """
    Fusionne les résultats de plusieurs recherches en un seul fichier JSON.
    
    Args:
        json_files: Liste des chemins des fichiers JSON à fusionner
        output_file: Nom du fichier de sortie (sans extension)
    
    Returns:
        str: Chemin complet du fichier fusionné
    """
    # Import ici pour éviter les imports circulaires
    from wttj_storage import merge_json_files, upload_to_s3
    
    if not json_files:
        logger.warning("Aucun fichier à fusionner")
        return None
    
    # Générer un nom de fichier par défaut si non spécifié
    if not output_file:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"welcome_jungle_merged_{timestamp}"
    
    # Fusionner les fichiers JSON
    merged_file = merge_json_files(json_files, output_file)
    
    if merged_file:
        # Upload le fichier fusionné vers S3
        upload_success = upload_to_s3(merged_file)
        if upload_success:
            logger.info(f"Fichier fusionné uploadé avec succès vers S3: {merged_file}")
        else:
            logger.warning(f"Impossible d'uploader le fichier fusionné vers S3: {merged_file}")
    
    return merged_file

def main():
    """
    Fonction principale pour exécuter des recherches multiples depuis la ligne de commande.
    """
    parser = argparse.ArgumentParser(description="Exécute plusieurs recherches sur Welcome to the Jungle")
    parser.add_argument("-k", "--keywords", nargs="+", required=True, help="Liste des mots-clés de recherche")
    parser.add_argument("-l", "--locations", nargs="+", required=True, help="Liste des lieux de recherche")
    parser.add_argument("-p", "--max-pages", type=int, default=None, help="Nombre maximum de pages à parcourir par recherche")
    parser.add_argument("--no-headless", action="store_true", help="Désactiver le mode headless (afficher le navigateur)")
    parser.add_argument("-o", "--output", help="Nom du fichier de sortie pour les résultats fusionnés")
    
    args = parser.parse_args()
    
    # Exécuter les recherches multiples
    total_jobs, json_files = run_multiple_searches(
        args.keywords,
        args.locations,
        args.max_pages,
        not args.no_headless
    )
    
    # Fusionner les résultats si des fichiers ont été générés
    if json_files:
        merged_file = merge_search_results(json_files, args.output)
        if merged_file:
            logger.info(f"Résultats fusionnés avec succès dans: {merged_file}")
        else:
            logger.error("Impossible de fusionner les résultats")
    else:
        logger.warning("Aucun fichier de résultats généré, fusion impossible")

if __name__ == "__main__":
    main()