#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script principal pour exécuter le pipeline ETL Welcome to the Jungle.
Ce script gère l'extraction, la transformation et le chargement des données
des offres d'emploi collectées depuis Welcome to the Jungle.
"""

import os
import sys
import argparse
import logging
from datetime import datetime

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Ajouter le chemin du projet à PYTHONPATH
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Import des modules ETL
from src.pipeline.scrapers.extraction import extract_welcome_jungle_data
from src.pipeline.scrapers.transformation import transform_welcome_jungle_data, save_transformed_data
from src.pipeline.scrapers.loading import run_welcome_jungle_etl

def run_pipeline(args):
    """
    Exécute le pipeline ETL Welcome to the Jungle en fonction des arguments.
    
    Args:
        args (argparse.Namespace): Arguments de ligne de commande
    """
    try:
        # Charger les variables d'environnement
        from src.pipeline.api.dotenv_utils import load_dotenv
        load_dotenv()
        
        # Déterminer les étapes à exécuter
        run_extraction = args.extract or args.all
        run_transformation = args.transform or args.all
        run_loading = args.load or args.all
        
        # Si aucune étape n'est spécifiée, exécuter tout le pipeline
        if not any([run_extraction, run_transformation, run_loading]):
            run_extraction = run_transformation = run_loading = True
        
        df = None
        local_file = None
        transformed_df = None
        
        # Étape d'extraction
        if run_extraction:
            logger.info("Début de l'étape d'extraction")
            # Extraire toutes les données si aucun fichier spécifique n'est fourni
            df, local_file = extract_welcome_jungle_data(specific_file=args.file, all_files=True, force_download=args.force_download)
            
            if df.empty:
                logger.error("Aucune donnée extraite")
                return False
                
            logger.info(f"Extraction terminée: {len(df)} enregistrements")
        
        # Étape de transformation
        if run_transformation:
            if df is None and local_file:
                # Si l'extraction n'a pas été exécutée mais qu'un fichier est spécifié
                logger.info(f"Chargement des données depuis le fichier local: {local_file}")
                import pandas as pd
                df = pd.read_json(local_file)
            
            if df is None:
                logger.error("Aucune donnée disponible pour la transformation")
                return False
            
            logger.info("Début de l'étape de transformation")
            transformed_df = transform_welcome_jungle_data(df)
            
            if transformed_df.empty:
                logger.error("Erreur lors de la transformation des données")
                return False
                
            logger.info(f"Transformation terminée: {len(transformed_df)} enregistrements")
            
            # Sauvegarde des données transformées
            output_file = save_transformed_data(transformed_df)
            logger.info(f"Données transformées sauvegardées dans: {output_file}")
        
        # Étape de chargement
        if run_loading:
            if args.all or (args.extract and args.transform and args.load):
                # Si tout le pipeline est demandé, utiliser la fonction complète
                logger.info("Exécution du pipeline ETL complet")
                # Utiliser tous les fichiers disponibles
                success = run_welcome_jungle_etl(force=args.force, specific_file=args.file)
                
                if success:
                    logger.info("Pipeline ETL Welcome to the Jungle terminé avec succès")
                    return True
                else:
                    logger.error("Erreur lors de l'exécution du pipeline ETL")
                    return False
            else:
                # Si seulement l'étape de chargement est demandée
                if transformed_df is None:
                    logger.error("Aucune donnée transformée disponible pour le chargement")
                    return False
                
                logger.info("Début de l'étape de chargement")
                from src.pipeline.scrapers.loading import get_db_connection, load_welcome_jungle_data
                
                engine = get_db_connection()
                if not engine:
                    logger.error("Impossible de se connecter à la base de données")
                    return False
                
                success = load_welcome_jungle_data(transformed_df, engine)
                
                if success:
                    logger.info("Chargement des données terminé avec succès")
                    return True
                else:
                    logger.error("Erreur lors du chargement des données")
                    return False
        
        return True
        
    except Exception as e:
        logger.error(f"Erreur lors de l'exécution du pipeline: {e}")
        return False

def main():
    """
    Fonction principale pour exécuter le pipeline ETL Welcome to the Jungle.
    """
    parser = argparse.ArgumentParser(description="Pipeline ETL Welcome to the Jungle")
    
    # Options pour les étapes du pipeline
    parser.add_argument("--extract", action="store_true", help="Exécuter uniquement l'étape d'extraction")
    parser.add_argument("--transform", action="store_true", help="Exécuter uniquement l'étape de transformation")
    parser.add_argument("--load", action="store_true", help="Exécuter uniquement l'étape de chargement")
    parser.add_argument("--all", action="store_true", help="Exécuter toutes les étapes du pipeline")
    
    # Options supplémentaires
    parser.add_argument("--force", action="store_true", help="Force le chargement des données même si elles existent déjà")
    parser.add_argument("--force-download", action="store_true", help="Force le téléchargement des fichiers même s'ils existent déjà localement")
    parser.add_argument("--file", type=str, help="Fichier spécifique à traiter (optionnel)")
    
    args = parser.parse_args()
    
    success = run_pipeline(args)
    
    if success:
        logger.info("Exécution du pipeline ETL Welcome to the Jungle terminée avec succès")
    else:
        logger.error("Erreur lors de l'exécution du pipeline ETL Welcome to the Jungle")

if __name__ == "__main__":
    main()
