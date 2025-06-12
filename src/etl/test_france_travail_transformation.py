#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script de test pour la transformation des données France Travail.
Vérifie le processus de transformation des offres d'emploi.
"""

import os
import sys
import logging
import pandas as pd
from datetime import datetime, timedelta

# Configurer le logging
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/test_transformation_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Ajouter le chemin du projet au PYTHONPATH
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Importer les modules nécessaires
from etl.api.extraction import extract_by_date_range
from etl.api.dotenv_utils import load_dotenv
from etl.api.transformation import transform_job_dataframe, apply_keyword_analysis

def format_text_for_console(text, max_length=100):
    """Formate un texte pour l'affichage console en limitant sa longueur"""
    if not isinstance(text, str):
        return str(text)
    if len(text) > max_length:
        return text[:max_length] + "..."
    return text

def test_transformation_with_sample():
    """
    Teste la transformation avec un échantillon d'offres.
    """
    logger.info("=== Test de transformation avec un échantillon d'offres ===")
    
    # Au lieu de filtrer par date récente, utilisons tous les fichiers disponibles
    # et limitons le nombre d'offres
    logger.info("Recherche des fichiers disponibles dans S3...")
    # Liste des fichiers S3
    from etl.api.extraction import list_s3_files
    s3_files = list_s3_files()
    
    if not s3_files or len(s3_files) == 0:
        logger.error("Aucun fichier trouvé dans S3")
        return False
    
    logger.info(f"Trouvé {len(s3_files)} fichiers dans S3")
    # Prenons les 3 premiers fichiers pour notre test
    sample_files = s3_files[:3]
    logger.info(f"Utilisation de {len(sample_files)} fichiers pour le test: {[os.path.basename(f) for f in sample_files]}")
    
    # Téléchargeons et utilisons ces fichiers
    from etl.api.extraction import download_s3_file, extract_jobs_to_dataframe
    downloaded_files = []
    temp_dir = "data/temp/transformation_test"
    os.makedirs(temp_dir, exist_ok=True)
    
    for s3_key in sample_files:
        downloaded_file = download_s3_file('data-lake-brut', s3_key, temp_dir)
        if downloaded_file:
            downloaded_files.append(downloaded_file)
    
    if not downloaded_files:
        logger.error("Impossible de télécharger les fichiers de test depuis S3")
        return False
    
    logger.info(f"Traitement de {len(downloaded_files)} fichiers téléchargés")
    # Utilisons ces fichiers pour créer notre DataFrame
    raw_df = extract_jobs_to_dataframe(downloaded_files)
    
    if raw_df is None or raw_df.empty:
        logger.error("Aucune donnée extraite pour le test")
        return False
    
    # Échantillonnage si trop de données
    if len(raw_df) > 1000:
        logger.info(f"Échantillonnage de 1000 offres parmi {len(raw_df)} extraites")
        raw_df = raw_df.sample(1000, random_state=42)
    
    logger.info(f"Nombre d'offres à transformer: {len(raw_df)}")
    
    # Appliquer les transformations
    try:
        logger.info("Application de la transformation de base")
        transformed_df = transform_job_dataframe(raw_df)
        
        logger.info("Application de l'analyse par mots-clés")
        transformed_df = apply_keyword_analysis(transformed_df)
        
        # Vérification des colonnes attendues pour la base de données
        expected_columns = [
            'id', 'intitule', 'description_clean', 'entreprise_clean', 'lieu_travail',
            'type_contrat', 'contract_type_std', 'experience_level', 
            'min_salary', 'max_salary', 'salary_periodicity', 'currency',
            'etl_timestamp', 'has_python', 'has_java', 'has_javascript', 
            'has_sql', 'has_aws', 'has_machine_learning', 'keyword_count'
        ]
        
        missing_columns = [col for col in expected_columns if col not in transformed_df.columns]
        if missing_columns:
            logger.warning(f"Colonnes manquantes: {', '.join(missing_columns)}")
        else:
            logger.info("[OK] Toutes les colonnes attendues sont présentes")
        
        # Statistiques sur les données transformées
        logger.info("=== Statistiques sur les données transformées ===")
        logger.info(f"Nombre total d'offres transformées: {len(transformed_df)}")
        
        # Types de contrat standardisés
        contract_stats = transformed_df['contract_type_std'].value_counts()
        logger.info("Répartition des types de contrat standardisés:")
        for contract, count in contract_stats.items():
            logger.info(f"  - {contract}: {count} offres ({count/len(transformed_df)*100:.1f}%)")
        
        # Niveaux d'expérience
        if 'experience_level' in transformed_df.columns:
            exp_stats = transformed_df['experience_level'].value_counts()
            logger.info("Répartition des niveaux d'expérience:")
            for exp, count in exp_stats.items():
                logger.info(f"  - {exp}: {count} offres ({count/len(transformed_df)*100:.1f}%)")
        
        # Statistiques salariales
        salary_provided = transformed_df['min_salary'].notna().sum()
        logger.info(f"Offres avec information salariale: {salary_provided} ({salary_provided/len(transformed_df)*100:.1f}%)")
        
        if salary_provided > 0:
            mean_min_salary = transformed_df['min_salary'].mean()
            mean_max_salary = transformed_df['max_salary'].mean()
            logger.info(f"Salaire minimum moyen: {mean_min_salary:.2f}")
            logger.info(f"Salaire maximum moyen: {mean_max_salary:.2f}")
        
        # Compétences techniques
        tech_columns = [col for col in transformed_df.columns if col.startswith('has_')]
        logger.info("Répartition des compétences techniques:")
        for col in tech_columns:
            tech_name = col.replace('has_', '')
            count = transformed_df[col].sum()
            logger.info(f"  - {tech_name}: {count} offres ({count/len(transformed_df)*100:.1f}%)")
        
        # Examiner quelques exemples transformés
        logger.info("=== Exemples d'offres transformées ===")
        for i, (_, row) in enumerate(transformed_df.head(3).iterrows()):
            logger.info(f"Offre #{i+1}:")
            logger.info(f"  - ID: {row.get('id', 'N/A')}")
            logger.info(f"  - Titre: {row.get('intitule', 'N/A')}")
            logger.info(f"  - Entreprise: {format_text_for_console(row.get('entreprise_clean', 'N/A'))}")
            logger.info(f"  - Lieu: {row.get('lieu_travail', 'N/A')}")
            logger.info(f"  - Type de contrat: {row.get('type_contrat', 'N/A')} ({row.get('contract_type_std', 'N/A')})")
            logger.info(f"  - Niveau d'expérience: {row.get('experience_level', 'N/A')}")
            logger.info(f"  - Salaire: {row.get('min_salary', 'N/A')} - {row.get('max_salary', 'N/A')} {row.get('currency', '')} ({row.get('salary_periodicity', '')})")
            logger.info(f"  - Mots-clés trouvés: {row.get('keyword_count', 0)}")
            tech_present = [tech.replace('has_', '') for tech in tech_columns if row.get(tech, 0) == 1]
            logger.info(f"  - Technologies: {', '.join(tech_present) if tech_present else 'Aucune détectée'}")
            logger.info(f"  - Description: {format_text_for_console(row.get('description_clean', 'N/A'), 150)}")
            logger.info("  ---")
        
        logger.info("[SUCCÈS] Test de transformation réalisé avec succès")
        return True
        
    except Exception as e:
        logger.error(f"Erreur lors du test de transformation: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False

def main():
    """Fonction principale du script de test"""
    logger.info("=== DÉBUT DES TESTS DE TRANSFORMATION FRANCE TRAVAIL ===")
    
    # Charger les variables d'environnement
    load_dotenv()
    
    # Tester la transformation
    success = test_transformation_with_sample()
    
    if success:
        logger.info("[SUCCÈS] Tous les tests de transformation ont réussi!")
    else:
        logger.error("[ÉCHEC] Des erreurs sont survenues lors des tests de transformation")
    
    logger.info("=== FIN DES TESTS DE TRANSFORMATION FRANCE TRAVAIL ===")

if __name__ == "__main__":
    main()
