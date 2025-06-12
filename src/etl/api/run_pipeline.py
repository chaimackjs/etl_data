#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script principal pour exécuter le pipeline ETL complet pour l'API France Travail.
Ce script orchestre l'extraction, la transformation et le chargement des offres d'emploi.
"""

import os
import sys
import logging
import argparse
from datetime import datetime, timedelta

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

def parse_arguments():
    """
    Parse les arguments de la ligne de commande.
    """
    parser = argparse.ArgumentParser(description="Pipeline ETL pour les offres d'emploi France Travail")
    parser.add_argument(
        '--start-date', 
        type=str, 
        help="Date de début au format YYYYMMDD (défaut: aujourd'hui)",
        default=datetime.now().strftime("%Y%m%d")
    )
    parser.add_argument(
        '--end-date', 
        type=str, 
        help="Date de fin au format YYYYMMDD (défaut: aujourd'hui)"
    )
    parser.add_argument(
        '--skip-db', 
        action='store_true',
        help="Ne pas charger les données dans la base de données"
    )
    parser.add_argument(
        '--output-csv', 
        action='store_true',
        help="Générer un fichier CSV avec les données transformées"
    )
    parser.add_argument(
        '--verbose', 
        action='store_true',
        help="Afficher les messages de debug détaillés"
    )
    return parser.parse_args()

def check_prerequisites():
    """
    Vérifie que les prérequis sont remplis pour exécuter le pipeline.
    
    Returns:
        bool: True si tous les prérequis sont remplis, False sinon
    """
    # Vérifier que les répertoires nécessaires existent
    dirs_to_check = ['data/raw/france_travail', 'data/processed', 'logs']
    for dir_path in dirs_to_check:
        if not os.path.exists(dir_path):
            os.makedirs(dir_path, exist_ok=True)
            logger.info(f"Répertoire créé: {dir_path}")
    
    # Vérifier les variables d'environnement AWS si on doit se connecter à la base de données
    if not args.skip_db:
        aws_vars = ['DB_HOST', 'DB_NAME', 'DB_USER', 'DB_PASSWORD']
        missing_vars = [var for var in aws_vars if not os.getenv(var)]
        
        # Si les variables ne sont pas définies, utiliser les valeurs par défaut
        if missing_vars:
            logger.warning(f"Variables d'environnement manquantes: {missing_vars}")
            logger.info("Utilisation des valeurs par défaut pour la connexion à la base de données")
            
            # Définir les valeurs par défaut
            if not os.getenv('DB_HOST'):
                os.environ['DB_HOST'] = os.getenv('DB_HOST')
            if not os.getenv('DB_PORT'):
                os.environ['DB_PORT'] = os.getenv('DB_PORT')
            if not os.getenv('DB_NAME'):
                os.environ['DB_NAME'] = os.getenv('DB_NAME')
            if not os.getenv('DB_USER'):
                os.environ['DB_USER'] = os.getenv('DB_USER')
            if not os.getenv('DB_PASSWORD'):
                os.environ['DB_PASSWORD'] = os.getenv('DB_PASSWORD')
    
    return True

def main():
    """
    Fonction principale exécutant l'ensemble du pipeline ETL.
    """
    # Vérifier les prérequis
    if not check_prerequisites():
        logger.error("Impossible de continuer, prérequis non remplis")
        return 1
    
    # Ajouter le chemin du répertoire parent pour les imports
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    # Importer les modules nécessaires
    try:
        from api.extraction import extract_by_date_range
        from api.transformation import transform_job_dataframe, apply_keyword_analysis
        from api.loading import prepare_job_data_for_loading, get_db_connection, create_jobs_table, load_jobs_to_database
    except ImportError as e:
        logger.error(f"Erreur d'importation des modules: {e}")
        return 1
    
    # Début du pipeline
    logger.info(f"=== Début du pipeline ETL pour France Travail ===")
    logger.info(f"Période: {args.start_date} à {args.end_date or args.start_date}")
    
    start_time = datetime.now()
    
    try:
        # 1. Extraction
        logger.info("Étape 1: EXTRACTION - Récupération des offres d'emploi")
        raw_df = extract_by_date_range(args.start_date, args.end_date)
        
        if raw_df is None or len(raw_df) == 0:
            logger.warning("Aucune donnée extraite pour la période spécifiée")
            return 0
        
        logger.info(f"Extraction terminée: {len(raw_df)} offres d'emploi récupérées")
        
        # 2. Transformation
        logger.info("Étape 2: TRANSFORMATION - Nettoyage et enrichissement des données")
        transformed_df = transform_job_dataframe(raw_df)
        
        if transformed_df is not None:
            # Ajouter l'analyse des mots-clés
            logger.info("Application de l'analyse par mots-clés")
            transformed_df = apply_keyword_analysis(transformed_df)
            logger.info(f"Transformation terminée: {len(transformed_df)} offres d'emploi transformées")
        else:
            logger.error("Échec de la transformation des données")
            return 1
        
        # Sauvegarder en CSV si demandé
        if args.output_csv:
            csv_path = f"data/processed/france_travail_transformed_{args.start_date}.csv"
            transformed_df.to_csv(csv_path, index=False, encoding='utf-8')
            logger.info(f"Données transformées sauvegardées dans {csv_path}")
        
        # 3. Chargement (si non skip_db)
        if not args.skip_db:
            logger.info("Étape 3: CHARGEMENT - Préparation des données pour la base de données")
            load_ready_df = prepare_job_data_for_loading(transformed_df)
            
            if load_ready_df is None:
                logger.error("Échec de la préparation des données pour le chargement")
                return 1
            
            logger.info("Connexion à la base de données RDS...")
            engine = get_db_connection()
            
            if engine is None:
                logger.error("Impossible de se connecter à la base de données")
                logger.warning("Veuillez vérifier que l'instance RDS est accessible depuis votre réseau")
                logger.warning("Les données transformées ont été sauvegardées en CSV si --output-csv a été utilisé")
                return 1
            
            # Créer la table si nécessaire
            if create_jobs_table(engine):
                # Charger les données
                records_loaded = load_jobs_to_database(load_ready_df, engine)
                logger.info(f"Chargement terminé: {records_loaded} offres d'emploi insérées dans la base de données")
            else:
                logger.error("Échec de la création de la table dans la base de données")
                return 1
        else:
            logger.info("Chargement dans la base de données ignoré (--skip-db)")
        
        # Fin du pipeline
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info(f"=== Pipeline ETL terminé avec succès ===")
        logger.info(f"Durée totale: {duration}")
        
        return 0
        
    except Exception as e:
        logger.exception(f"Erreur lors de l'exécution du pipeline: {e}")
        return 1

if __name__ == "__main__":
    # Récupérer les arguments
    args = parse_arguments()
    
    # Configurer le niveau de log
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Exécuter le pipeline
    sys.exit(main())
