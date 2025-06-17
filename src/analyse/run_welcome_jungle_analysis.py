#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script d'interface en ligne de commande pour exécuter l'analyse des offres d'emploi Welcome to the Jungle.
"""

import argparse
import logging
import os
from dotenv import load_dotenv
from welcome_jungle_analysis import generate_welcome_jungle_report, load_welcome_jungle_data_from_db, load_welcome_jungle_data_from_csv

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

def main():
    """
    Fonction principale pour exécuter l'analyse des offres d'emploi Welcome to the Jungle.
    """
    # Charger les variables d'environnement
    load_dotenv()
    
    # Parser les arguments de ligne de commande
    parser = argparse.ArgumentParser(description='Analyse des offres d\'emploi Welcome to the Jungle')
    parser.add_argument('--source', choices=['db', 'csv', 'all'], default='db',
                        help='Source des données à analyser (db: base de données, csv: fichiers CSV, all: essayer les deux)')
    parser.add_argument('--csv-dir', default='data/processed/welcome_jungle/',
                        help='Répertoire contenant les fichiers CSV (si --source=csv)')
    parser.add_argument('--open-html', action='store_true',
                        help='Ouvrir le rapport HTML dans le navigateur après génération')
    
    args = parser.parse_args()
    
    # Charger les données selon la source spécifiée
    df = None
    
    if args.source == 'db' or args.source == 'all':
        logger.info("Chargement des données depuis la base de données...")
        df = load_welcome_jungle_data_from_db()
    
    if (args.source == 'csv' or args.source == 'all') and (df is None or df.empty):
        logger.info(f"Chargement des données depuis les fichiers CSV dans {args.csv_dir}...")
        df = load_welcome_jungle_data_from_csv(args.csv_dir)
    
    if df is None or df.empty:
        logger.error("Impossible de charger les données d'offres d'emploi Welcome to the Jungle")
        return 1
    
    # Générer le rapport
    logger.info(f"Analyse de {len(df)} offres d'emploi Welcome to the Jungle...")
    results = generate_welcome_jungle_report(df)
    
    # Ouvrir le rapport HTML dans le navigateur si demandé
    if args.open_html and results:
        html_path = 'data/analysis/visualizations/welcome_jungle/dashboard.html'
        if os.path.exists(html_path):
            try:
                import webbrowser
                absolute_path = os.path.abspath(html_path)
                logger.info(f"Ouverture du rapport HTML: {absolute_path}")
                webbrowser.open('file://' + absolute_path)
            except Exception as e:
                logger.error(f"Erreur lors de l'ouverture du rapport HTML: {e}")
    
    return 0

if __name__ == "__main__":
    exit(main())
