#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script principal d'ETL pour France Travail
Exécute le pipeline complet d'extraction, transformation et chargement des données
"""

import os
import sys
import logging
import argparse
import pandas as pd
from datetime import datetime, timedelta

# Ajouter le chemin du projet au PYTHONPATH
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Importer les modules ETL
from etl.api.extraction import extract_data
from etl.api.transformation import transform_job_dataframe, apply_keyword_analysis
from etl.api.loading import load_jobs_to_database, get_db_connection, create_jobs_table
from etl.api.skills_extraction import extract_skills_from_dataframe, get_skills_frequency, generate_skills_report
from etl.api.job_skills_loader import execute_skills_loading

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/etl_france_travail_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def parse_arguments():
    """
    Parse les arguments de ligne de commande pour le script ETL
    """
    parser = argparse.ArgumentParser(description="Pipeline ETL pour les données France Travail")
    parser.add_argument('--start-date', type=str, help="Date de début au format YYYYMMDD")
    parser.add_argument('--end-date', type=str, help="Date de fin au format YYYYMMDD")
    parser.add_argument('--include-s3', action='store_true', help="Inclure les données de S3")
    parser.add_argument('--output-dir', type=str, help="Répertoire de sortie pour les fichiers intermédiaires")
    parser.add_argument('--skip-load', action='store_true', help="Ignorer l'étape de chargement")
    parser.add_argument('--no-skills', action='store_true', help="Désactiver l'extraction et le chargement des compétences")
    return parser.parse_args()

def run_extraction(start_date=None, end_date=None, include_s3=True):
    """
    Exécute l'étape d'extraction du pipeline ETL
    
    Returns:
        pandas.DataFrame: Données extraites
    """
    logger.info("=== DÉBUT DE L'ÉTAPE D'EXTRACTION ===")
    
    # Si les dates ne sont pas fournies, utiliser les 7 derniers jours par défaut
    if not start_date:
        start_date = (datetime.now() - timedelta(days=7)).strftime("%Y%m%d")
    
    if not end_date:
        end_date = datetime.now().strftime("%Y%m%d")
    
    logger.info(f"Extraction des données du {start_date} au {end_date}")
    
    # Extraire les données
    raw_data = extract_data(
        start_date=start_date,
        end_date=end_date,
        include_s3=include_s3
    )
    
    if raw_data is None or raw_data.empty:
        logger.warning("Aucune donnée extraite pour la période spécifiée")
        return None
    
    logger.info(f"Extraction terminée: {len(raw_data)} offres d'emploi extraites")
    logger.info("=== FIN DE L'ÉTAPE D'EXTRACTION ===")
    
    return raw_data

def run_transformation(raw_data):
    """
    Exécute l'étape de transformation du pipeline ETL
    
    Args:
        raw_data (pandas.DataFrame): Données extraites
    
    Returns:
        pandas.DataFrame: Données transformées
    """
    if raw_data is None or raw_data.empty:
        logger.warning("Aucune donnée à transformer")
        return None
    
    logger.info("=== DÉBUT DE L'ÉTAPE DE TRANSFORMATION ===")
    
    # 1. Nettoyage et transformation de base
    transformed_data = transform_job_dataframe(raw_data)
    
    # 2. Enrichissement des données (analyse des compétences)
    transformed_data = apply_keyword_analysis(transformed_data)
    
    # 3. Préparation pour le chargement dans la base de données
    # IMPORTANT : Utiliser les noms de colonnes correspondant exactement à ceux de la table
    # D'après les logs, les colonnes de la table sont ['id', 'intitule', 'description_clean', 'entreprise_clean', ...]
    column_mapping = {
        # Mappings des colonnes - id reste id pour la clé primaire
        'id': 'id',  
        'intitule': 'intitule',
        'description_clean': 'description_clean',  # Déjà correct
        'dateCreation_iso': 'date_creation',
        'dateActualisation_iso': 'date_actualisation',
        'entreprise_nom': 'entreprise_clean',  # IMPORTANT: Cette colonne sera rename en entreprise_clean
        'typeContrat': 'type_contrat',
        'lieu': 'lieu_travail',  # Champ créé dans transform_job_dataframe
        'contract_type_std': 'contract_type_std',
        'experience_level': 'experience_level',
        'etl_timestamp': 'etl_timestamp'
    }
    
    # Sélectionner et renommer les colonnes pour correspondre au schéma de la BDD
    final_df = transformed_data.rename(columns=column_mapping)
    
    # Convertir la colonne entreprise_clean en texte JSON si c'est un dictionnaire
    if 'entreprise_clean' in final_df.columns:
        import json
        final_df['entreprise_clean'] = final_df['entreprise_clean'].apply(
            lambda x: json.dumps(x) if isinstance(x, dict) else x
        )
        logger.info("Colonne entreprise_clean convertie du format dict en chaîne JSON")
    
    # Ajouter la colonne source pour identifier l'origine des données
    final_df['source'] = 'FRANCE_TRAVAIL'
    
    # Sélectionner uniquement les colonnes pertinentes pour la base de données
    db_columns = [
        'id', 'intitule', 'description_clean', 'entreprise_clean', 'lieu_travail',
        'type_contrat', 'contract_type_std', 'experience_level', 'source', 'etl_timestamp',
        'date_creation', 'date_actualisation'
    ]
    
    # S'assurer que toutes les colonnes requises existent
    for col in db_columns:
        if col not in final_df.columns:
            final_df[col] = None
            
    # Convertir la colonne extracted_keywords en texte JSON si elle existe
    # et stocker le résultat directement dans keyword_count
    if 'extracted_keywords' in final_df.columns:
        try:
            import json
            final_df['keyword_count'] = final_df['extracted_keywords'].apply(
                lambda x: len(x) if isinstance(x, list) else 0
            )
            # On peut aussi stocker le JSON si nécessaire
            final_df['extracted_keywords_text'] = final_df['extracted_keywords'].apply(
                lambda x: json.dumps(x) if isinstance(x, list) else '[]'
            )
            logger.info("Colonne extracted_keywords traitée : comptage et conversion en JSON")
            
            # Ajout des colonnes spécifiques pour les compétences détectées
            for tech in ['python', 'java', 'javascript', 'sql', 'aws', 'machine_learning']:
                col_name = f'has_{tech}'
                if col_name not in final_df.columns:
                    final_df[col_name] = 0
                    
        except Exception as e:
            logger.warning(f"Erreur lors du traitement des keywords: {e}")
            final_df['keyword_count'] = 0
            for tech in ['python', 'java', 'javascript', 'sql', 'aws', 'machine_learning']:
                final_df[f'has_{tech}'] = 0
    else:
        final_df['keyword_count'] = 0
        for tech in ['python', 'java', 'javascript', 'sql', 'aws', 'machine_learning']:
            final_df[f'has_{tech}'] = 0
    
    # Ajouter les colonnes supplémentaires à celles à charger dans la BDD
    db_columns.append('keyword_count')
    for tech in ['python', 'java', 'javascript', 'sql', 'aws', 'machine_learning']:
        db_columns.append(f'has_{tech}')
    
    # Ajouter extracted_keywords_text si la colonne a été ajoutée à la table
    if 'extracted_keywords_text' in final_df.columns:
        db_columns.append('extracted_keywords_text')
    
    # Sélectionner uniquement les colonnes nécessaires pour la BDD
    final_df = final_df[db_columns]
    
    logger.info(f"Transformation terminée: {len(final_df)} offres d'emploi transformées")
    logger.info("=== FIN DE L'ÉTAPE DE TRANSFORMATION ===")
    
    return final_df

def save_intermediate_data(data, output_dir=None, filename=None):
    """
    Enregistre les données intermédiaires pour faciliter le débogage
    
    Args:
        data (pandas.DataFrame): DataFrame à enregistrer
        output_dir (str): Répertoire de sortie
        filename (str): Nom de fichier (sans extension)
    
    Returns:
        str: Chemin du fichier enregistré
    """
    if data is None or data.empty:
        return None
    
    if not output_dir:
        output_dir = "data/processed"
    
    if not filename:
        filename = f"france_travail_processed_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # Créer le répertoire s'il n'existe pas
    os.makedirs(output_dir, exist_ok=True)
    
    # Construire le chemin complet
    csv_path = os.path.join(output_dir, f"{filename}.csv")
    
    try:
        # Exporter en CSV
        data.to_csv(csv_path, index=False, encoding='utf-8')
        logger.info(f"Données intermédiaires exportées vers {csv_path}")
        return csv_path
    except Exception as e:
        logger.error(f"Erreur lors de l'export des données intermédiaires: {e}")
        return None

def run_loading(data, skip_load=False, skip_skills=False):
    """
    Exécute l'étape de chargement du pipeline ETL
    
    Args:
        data (pandas.DataFrame): Données transformées à charger
        skip_load (bool): Si True, ignore l'étape de chargement
        skip_skills (bool): Si True, ignore l'extraction et le chargement des compétences
        
    Returns:
        dict: Résultats du chargement (nombre d'enregistrements chargés, etc.)
    """
    if skip_load:
        logger.info("Étape de chargement ignorée")
        return {
            'jobs_loaded': 0,
            'skills_loaded': 0,
            'job_skills_loaded': 0
        }
    
    if data is None or data.empty:
        logger.warning("Aucune donnée à charger")
        return {
            'jobs_loaded': 0,
            'skills_loaded': 0,
            'job_skills_loaded': 0
        }
    
    logger.info("=== DÉBUT DE L'ÉTAPE DE CHARGEMENT ===")
    results = {
        'jobs_loaded': 0,
        'skills_loaded': 0,
        'job_skills_loaded': 0
    }
    
    # 1. Se connecter à la base de données
    engine = get_db_connection()
    
    if engine is None:
        logger.error("Impossible de se connecter à la base de données")
        return results
    
    # 2. Créer la table des offres d'emploi si nécessaire
    table_ok = create_jobs_table(engine)
    
    if not table_ok:
        logger.error("Impossible de créer la table des offres d'emploi")
        return results
    
    # 3. Charger les offres d'emploi
    jobs_loaded = load_jobs_to_database(data, engine)
    results['jobs_loaded'] = jobs_loaded
    
    logger.info(f"{jobs_loaded} offres d'emploi chargées dans la base de données")
    
    # 4. Extraction et chargement des compétences
    if not skip_skills:
        logger.info("Extraction et chargement des compétences...")
        
        # Extraire les compétences
        skills_count, job_skills_count = execute_skills_loading(data)
        
        results['skills_loaded'] = skills_count
        results['job_skills_loaded'] = job_skills_count
        
        logger.info(f"{skills_count} compétences uniques chargées")
        logger.info(f"{job_skills_count} relations job_skills chargées")
    else:
        logger.info("Extraction des compétences désactivée")
    
    logger.info("=== FIN DE L'ÉTAPE DE CHARGEMENT ===")
    return results

def run_pipeline(custom_args=None):
    """
    Exécute le pipeline ETL complet
    
    Args:
        custom_args (Namespace, optional): Arguments personnalisés pour le pipeline. 
                                          Si non fourni, utilise parse_arguments().
    
    Returns:
        dict: Résultats du pipeline
    """
    start_time = datetime.now()
    logger.info(f"Démarrage du pipeline ETL France Travail: {start_time}")
    
    # Analyser les arguments
    args = custom_args if custom_args is not None else parse_arguments()
    
    results = {
        'success': False,
        'extracted': 0,
        'transformed': 0,
        'jobs_loaded': 0,
        'skills_loaded': 0,
        'job_skills_loaded': 0,
        'duration_seconds': 0,
        'output_file': None
    }
    
    try:
        # Étape 1: Extraction
        raw_data = run_extraction(
            start_date=args.start_date,
            end_date=args.end_date,
            include_s3=args.include_s3
        )
        
        if raw_data is not None:
            results['extracted'] = len(raw_data)
        
        # Étape 2: Transformation
        transformed_data = run_transformation(raw_data)
        
        if transformed_data is not None:
            results['transformed'] = len(transformed_data)
        
        # Enregistrer les données transformées pour référence
        if transformed_data is not None:
            output_path = save_intermediate_data(
                transformed_data,
                output_dir=args.output_dir
            )
            logger.info(f"Données transformées enregistrées dans: {output_path}")
            results['output_file'] = output_path
        
        # Étape 3: Chargement
        loading_results = run_loading(
            transformed_data, 
            skip_load=args.skip_load,
            skip_skills=args.no_skills
        )
        
        results.update(loading_results)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        results['duration_seconds'] = duration
        logger.info(f"Pipeline ETL terminé en {duration:.2f} secondes")
        
        # Préparer le résumé du pipeline
        logger.info("=== RÉSUMÉ DU PIPELINE ETL ===")
        logger.info(f"Offres extraites: {results['extracted']}")
        logger.info(f"Offres transformées: {results['transformed']}")
        logger.info(f"Offres chargées: {results['jobs_loaded']}")
        
        if not args.no_skills:
            logger.info(f"Compétences chargées: {results['skills_loaded']}")
            logger.info(f"Relations job-skills créées: {results['job_skills_loaded']}")
        
        results['success'] = True
        
    except Exception as e:
        logger.error(f"Erreur lors de l'exécution du pipeline ETL: {e}", exc_info=True)
        return results
    
    return results

if __name__ == "__main__":
    # Exécuter le pipeline avec les arguments de ligne de commande
    results = run_pipeline()
    
    # Afficher un résumé des résultats
    print("\n=== RÉSUMÉ DU PIPELINE FRANCE TRAVAIL ===")
    print(f"Statut: {'✅ Succès' if results['success'] else '❌ Échec'}")
    print(f"Offres extraites: {results['extracted']}")
    print(f"Offres transformées: {results['transformed']}")
    print(f"Offres chargées: {results['jobs_loaded']}")
    print(f"Compétences chargées: {results['skills_loaded']}")
    print(f"Relations job-skills: {results['job_skills_loaded']}")
    print(f"Durée totale: {results['duration_seconds']:.2f} secondes")
    
    if results['output_file']:
        print(f"Données intermédiaires: {results['output_file']}")
    
    sys.exit(0 if results['success'] else 1)
