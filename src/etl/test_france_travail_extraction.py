#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script de test pour vérifier l'extraction des données France Travail depuis S3.
Ce script teste étape par étape le processus d'extraction pour s'assurer que
chaque composant fonctionne correctement.
"""

import os
import sys
import logging
import pandas as pd
from datetime import datetime
import dotenv

# Configurer le logging
os.makedirs('logs', exist_ok=True)
log_file = f"logs/test_extraction_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Ajouter le chemin du projet au PYTHONPATH
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Charger les variables d'environnement
dotenv.load_dotenv()

# Définir les variables AWS si elles ne sont pas déjà définies
if not os.environ.get('KEY_ACCESS'):
    os.environ['KEY_ACCESS'] = 'AKIAS2VS4EK2UIF56F5O'
if not os.environ.get('KEY_SECRET'):
    os.environ['KEY_SECRET'] = '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'
if not os.environ.get('data_lake_bucket'):
    os.environ['data_lake_bucket'] = 'data-lake-brut'

# Importer les fonctions du module d'extraction
from etl.api.extraction import (
    get_s3_client,
    list_s3_files,
    download_s3_file,
    list_raw_data_files,
    extract_jobs_to_dataframe,
    extract_by_date_range
)

def test_s3_connection():
    """
    Teste la connexion au bucket S3.
    """
    logger.info("=== Test de connexion à S3 ===")
    s3_client = get_s3_client()
    if s3_client:
        logger.info("[OK] Connexion S3 établie avec succès")
        return True
    else:
        logger.error("[ECHEC] Échec de la connexion S3")
        return False

def test_list_s3_files():
    """
    Teste la fonction de listage des fichiers S3.
    """
    logger.info("=== Test de listage des fichiers S3 ===")
    
    # Préfixe pour filtrer les fichiers France Travail
    prefix = 'raw/france_travail/'
    
    # Obtenir le nom du bucket depuis les variables d'environnement
    bucket_name = os.environ.get('data_lake_bucket', 'data-lake-brut')
    
    # Lister les fichiers dans le bucket S3
    s3_files = list_s3_files(bucket_name=bucket_name, prefix=prefix)
    
    logger.info(f"Nombre de fichiers trouvés dans S3: {len(s3_files)}")
    
    # Afficher les 5 premiers fichiers (s'ils existent)
    if s3_files:
        logger.info("Exemples de fichiers trouvés:")
        for i, file_key in enumerate(s3_files[:5]):
            logger.info(f"  - {file_key}")
        return True
    else:
        logger.warning("Aucun fichier trouvé dans le bucket S3")
        return False

def test_download_s3_file():
    """
    Teste le téléchargement d'un fichier depuis S3.
    """
    logger.info("=== Test de téléchargement d'un fichier S3 ===")
    
    # Obtenir la liste des fichiers S3
    bucket_name = os.environ.get('data_lake_bucket', 'data-lake-brut')
    s3_files = list_s3_files(bucket_name=bucket_name, prefix='raw/france_travail/')
    
    if not s3_files:
        logger.warning("Aucun fichier disponible pour le test de téléchargement")
        return False
    
    # Sélectionner le premier fichier pour le test
    test_file = s3_files[0]
    logger.info(f"Téléchargement du fichier: {test_file}")
    
    # Créer un répertoire temporaire pour le téléchargement
    temp_dir = "data/temp/s3_test_downloads"
    os.makedirs(temp_dir, exist_ok=True)
    
    # Télécharger le fichier
    local_path = download_s3_file(bucket_name, test_file, temp_dir)
    
    if local_path and os.path.exists(local_path):
        file_size = os.path.getsize(local_path)
        logger.info(f"[OK] Fichier téléchargé avec succès: {local_path} ({file_size} octets)")
        return True
    else:
        logger.error("[ECHEC] Échec du téléchargement du fichier")
        return False

def test_extract_jobs_from_file(file_path):
    """
    Teste l'extraction des offres d'emploi à partir d'un fichier JSON.
    
    Args:
        file_path (str): Chemin vers le fichier JSON à tester
    """
    logger.info(f"=== Test d'extraction depuis le fichier {os.path.basename(file_path)} ===")
    
    # Extraire les données à partir d'un seul fichier
    df = extract_jobs_to_dataframe([file_path])
    
    if df is not None and not df.empty:
        logger.info(f"[OK] Extraction réussie - {len(df)} offres extraites")
        logger.info(f"Colonnes disponibles: {df.columns.tolist()}")
        
        # Afficher quelques exemples d'offres
        if 'intitule' in df.columns:
            sample = df[['intitule']].head(3).values.tolist()
            logger.info(f"Exemples d'intitulés: {[item[0] for item in sample]}")
        return True
    else:
        logger.error("[ECHEC] Aucune offre extraite du fichier")
        return False

def analyze_job_data(df):
    """
    Analyse et affiche un aperçu détaillé des offres d'emploi.
    
    Args:
        df (pandas.DataFrame): DataFrame contenant les offres d'emploi
    """
    if df is None or df.empty:
        logger.warning("Aucune donnée à analyser")
        return
    
    logger.info("\n=== ANALYSE DES OFFRES D'EMPLOI ===")
    
    # 1. Statistiques générales
    logger.info(f"Nombre total d'offres: {len(df)}")
    logger.info(f"Colonnes disponibles: {df.columns.tolist()}")
    
    # 2. Types de contrats
    if 'typeContratLibelle' in df.columns:
        contrats = df['typeContratLibelle'].value_counts()
        logger.info("\nTypes de contrats:")
        for contrat, count in contrats.head(10).items():
            logger.info(f"  - {contrat}: {count} offres")
    
    # 3. Répartition géographique
    if 'lieuTravail' in df.columns and isinstance(df['lieuTravail'].iloc[0], dict):
        lieux = df['lieuTravail'].apply(lambda x: x.get('libelle', 'Non spécifié') if isinstance(x, dict) else 'Non spécifié')
        top_lieux = lieux.value_counts()
        logger.info("\nTop 10 des lieux de travail:")
        for lieu, count in top_lieux.head(10).items():
            logger.info(f"  - {lieu}: {count} offres")
    
    # 4. Analyse des compétences les plus demandées
    if 'competences' in df.columns:
        # Extraire les compétences (peut être une liste de dictionnaires)
        all_competences = []
        for comp_list in df['competences']:
            if isinstance(comp_list, list):
                for comp in comp_list:
                    if isinstance(comp, dict) and 'libelle' in comp:
                        all_competences.append(comp['libelle'])
        
        if all_competences:
            from collections import Counter
            comp_counter = Counter(all_competences)
            logger.info("\nTop 10 des compétences demandées:")
            for comp, count in comp_counter.most_common(10):
                logger.info(f"  - {comp}: {count} mentions")
    
    # 5. Exemple détaillé d'une offre
    logger.info("\nExemple détaillé d'une offre:")
    sample_job = df.iloc[0].to_dict()
    
    # Afficher les informations principales
    logger.info(f"ID: {sample_job.get('id', 'N/A')}")
    logger.info(f"Titre: {sample_job.get('intitule', 'N/A')}")
    
    if 'entreprise' in sample_job and isinstance(sample_job['entreprise'], dict):
        logger.info(f"Entreprise: {sample_job['entreprise'].get('nom', 'N/A')}")
    
    if 'lieuTravail' in sample_job and isinstance(sample_job['lieuTravail'], dict):
        logger.info(f"Lieu: {sample_job['lieuTravail'].get('libelle', 'N/A')}")
    
    logger.info(f"Type de contrat: {sample_job.get('typeContratLibelle', 'N/A')}")
    logger.info(f"Expérience: {sample_job.get('experienceLibelle', 'N/A')}")
    
    # Description (tronquée pour lisibilité)
    if 'description' in sample_job:
        description = sample_job['description']
        if isinstance(description, str) and len(description) > 200:
            description = description[:200] + '...'
        logger.info(f"Description: {description}")

def test_date_range_extraction():
    """
    Teste l'extraction des offres d'emploi sur une plage de dates.
    """
    from datetime import datetime, timedelta
    
    # Définir une plage de dates récente (7 derniers jours)
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=7)).strftime("%Y%m%d")
    
    logger.info(f"=== Test d'extraction sur la période du {start_date} au {end_date} ===")
    
    # Extraire les données
    df = extract_by_date_range(start_date, end_date, include_s3=True)
    
    if df is not None and not df.empty:
        logger.info(f"[OK] Extraction par date réussie - {len(df)} offres extraites")
        
        # Analyser les données extraites
        analyze_job_data(df)
        
        return True
    else:
        logger.warning("Aucune offre trouvée sur la période spécifiée")
        # Ce n'est pas nécessairement une erreur, il peut ne pas y avoir de données
        return True

def cleanup():
    """
    Nettoie les fichiers temporaires créés pendant les tests.
    """
    import shutil
    
    temp_dir = "data/temp/s3_test_downloads"
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
        logger.info(f"Nettoyage du répertoire temporaire: {temp_dir}")

def run_all_tests():
    """
    Exécute tous les tests de vérification de l'extraction.
    """
    logger.info("=== DÉBUT DES TESTS D'EXTRACTION FRANCE TRAVAIL ===")
    
    # Afficher les informations de configuration
    logger.info(f"Bucket S3: {os.environ.get('data_lake_bucket')}")
    logger.info(f"Clé AWS: {os.environ.get('KEY_ACCESS')[:4]}...{os.environ.get('KEY_ACCESS')[-4:]}")
    
    results = {}
    
    # Test 1: Connexion S3
    results['connexion_s3'] = test_s3_connection()
    
    # Test 2: Listage des fichiers S3
    results['listage_s3'] = test_list_s3_files()
    
    # Test 3: Téléchargement d'un fichier S3
    results['telechargement_s3'] = test_download_s3_file()
    
    # Si le téléchargement a réussi, tester l'extraction à partir du fichier téléchargé
    if results['telechargement_s3']:
        temp_dir = "data/temp/s3_test_downloads"
        if os.path.exists(temp_dir):
            test_files = [os.path.join(temp_dir, f) for f in os.listdir(temp_dir) if f.endswith('.json')]
            if test_files:
                results['extraction_fichier'] = test_extract_jobs_from_file(test_files[0])
    
    # Test 5: Extraction par plage de dates
    results['extraction_dates'] = test_date_range_extraction()
    
    # Résumé des tests
    logger.info("\n=== RÉSUMÉ DES TESTS ===")
    for test_name, result in results.items():
        status = "[OK] RÉUSSI" if result else "[ECHEC] ÉCHEC"
        logger.info(f"{test_name}: {status}")
    
    # Nettoyer les fichiers temporaires
    cleanup()
    
    logger.info("=== FIN DES TESTS D'EXTRACTION FRANCE TRAVAIL ===")
    
    # Vérifier si tous les tests ont réussi
    return all(results.values())

if __name__ == "__main__":
    # Exécuter tous les tests
    success = run_all_tests()
    
    if success:
        logger.info("[SUCCES] Tous les tests d'extraction ont réussi!")
        sys.exit(0)
    else:
        logger.error("[ECHEC] Certains tests d'extraction ont échoué. Consultez les logs pour plus de détails.")
        sys.exit(1)
