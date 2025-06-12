#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Module d'analyse des offres d'emploi.
Fournit des fonctions pour analyser les tendances et statistiques des offres d'emploi.
"""

import os
import json
import logging
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from collections import Counter
from datetime import datetime
from dotenv import load_dotenv
import boto3
import psycopg2
from wordcloud import WordCloud

# Charger les variables d'environnement
load_dotenv()

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

def load_jobs_from_rds():
    """
    Charge les offres d'emploi depuis la base de données RDS.
    
    Returns:
        pandas.DataFrame: DataFrame contenant les offres d'emploi
    """
    try:
        # Récupérer les paramètres de connexion
        host = os.getenv('DB_HOST')
        port = os.getenv('DB_PORT', '5432')
        dbname = os.getenv('DB_NAME')
        user = os.getenv('DB_USER')
        password = os.getenv('DB_PASSWORD')
        
        # Établir la connexion
        connection = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password
        )
        
        # Requête SQL pour récupérer les offres avec leurs compétences
        query = """
            SELECT j.id, j.title, j.company, j.location, j.contract_type, j.source, 
                   j.scraped_at, array_agg(s.name) as skills
            FROM jobs j
            LEFT JOIN job_skills js ON j.id = js.job_id
            LEFT JOIN skills s ON js.skill_id = s.id
            GROUP BY j.id, j.title, j.company, j.location, j.contract_type, j.source, j.scraped_at
        """
        
        # Exécuter la requête et charger les résultats dans un DataFrame
        df = pd.read_sql_query(query, connection)
        
        # Fermer la connexion
        connection.close()
        
        logger.info(f"Chargé {len(df)} offres d'emploi depuis RDS")
        return df
    
    except Exception as e:
        logger.error(f"Erreur lors du chargement des offres depuis RDS: {e}")
        return pd.DataFrame()

def load_jobs_from_s3(bucket_name=None, prefix='processed/'):
    """
    Charge les offres d'emploi depuis S3.
    
    Args:
        bucket_name (str): Nom du bucket S3
        prefix (str): Préfixe des fichiers à charger
    
    Returns:
        pandas.DataFrame: DataFrame contenant les offres d'emploi
    """
    try:
        # Charger les identifiants AWS
        aws_access_key = os.getenv('KEY_ACCESS')
        aws_secret_key = os.getenv('KEY_SECRET')
        
        if not aws_access_key or not aws_secret_key:
            logger.error("Identifiants AWS manquants. Vérifiez les variables d'environnement KEY_ACCESS et KEY_SECRET.")
            return pd.DataFrame()
        
        # Utiliser le bucket par défaut si non spécifié
        if not bucket_name:
            bucket_name = os.getenv('data_lake_bucket', 'data-lake-brut')
        
        # Créer le client S3
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key
        )
        
        # Lister les objets dans le bucket avec le préfixe spécifié
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=prefix
        )
        
        # Vérifier s'il y a des objets
        if 'Contents' not in response:
            logger.warning(f"Aucun objet trouvé dans s3://{bucket_name}/{prefix}")
            return pd.DataFrame()
        
        # Récupérer le fichier le plus récent
        objects = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)
        latest_file = objects[0]['Key']
        
        # Télécharger le fichier
        response = s3_client.get_object(
            Bucket=bucket_name,
            Key=latest_file
        )
        
        # Charger le contenu JSON
        content = response['Body'].read().decode('utf-8')
        jobs_data = json.loads(content)
        
        # Convertir en DataFrame
        df = pd.DataFrame(jobs_data)
        
        logger.info(f"Chargé {len(df)} offres d'emploi depuis S3: s3://{bucket_name}/{latest_file}")
        return df
    
    except Exception as e:
        logger.error(f"Erreur lors du chargement des offres depuis S3: {e}")
        return pd.DataFrame()

def load_jobs_from_local(directory='data/processed/'):
    """
    Charge les offres d'emploi depuis des fichiers locaux.
    
    Args:
        directory (str): Répertoire contenant les fichiers à charger
    
    Returns:
        pandas.DataFrame: DataFrame contenant les offres d'emploi
    """
    try:
        # Vérifier si le répertoire existe
        if not os.path.exists(directory):
            logger.warning(f"Le répertoire {directory} n'existe pas")
            return pd.DataFrame()
        
        # Trouver tous les fichiers JSON dans le répertoire et ses sous-répertoires
        all_files = []
        for root, _, files in os.walk(directory):
            for file in files:
                if file.endswith('.json'):
                    all_files.append(os.path.join(root, file))
        
        if not all_files:
            logger.warning(f"Aucun fichier JSON trouvé dans {directory}")
            return pd.DataFrame()
        
        # Trier les fichiers par date de modification (le plus récent en premier)
        all_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
        
        # Charger le fichier le plus récent
        latest_file = all_files[0]
        with open(latest_file, 'r', encoding='utf-8') as f:
            jobs_data = json.load(f)
        
        # Convertir en DataFrame
        df = pd.DataFrame(jobs_data)
        
        logger.info(f"Chargé {len(df)} offres d'emploi depuis {latest_file}")
        return df
    
    except Exception as e:
        logger.error(f"Erreur lors du chargement des offres depuis les fichiers locaux: {e}")
        return pd.DataFrame()

def analyze_contract_types(df):
    """
    Analyse la distribution des types de contrat.
    
    Args:
        df (pandas.DataFrame): DataFrame contenant les offres d'emploi
    
    Returns:
        dict: Statistiques sur les types de contrat
    """
    if df.empty or 'contract_type' not in df.columns:
        return {}
    
    # Compter les types de contrat
    contract_counts = df['contract_type'].value_counts()
    
    # Calculer les pourcentages
    contract_percentages = (contract_counts / len(df) * 100).round(2)
    
    # Créer un dictionnaire de résultats
    results = {
        'counts': contract_counts.to_dict(),
        'percentages': contract_percentages.to_dict(),
        'total': len(df)
    }
    
    # Créer un graphique
    plt.figure(figsize=(10, 6))
    sns.countplot(y='contract_type', data=df, order=contract_counts.index)
    plt.title('Distribution des Types de Contrat')
    plt.xlabel('Nombre d\'offres')
    plt.ylabel('Type de Contrat')
    plt.tight_layout()
    
    # Sauvegarder le graphique
    os.makedirs('reports/figures', exist_ok=True)
    plt.savefig('reports/figures/contract_types.png')
    plt.close()
    
    return results

def analyze_skills(df):
    """
    Analyse les compétences les plus demandées.
    
    Args:
        df (pandas.DataFrame): DataFrame contenant les offres d'emploi
    
    Returns:
        dict: Statistiques sur les compétences
    """
    if df.empty or 'skills' not in df.columns:
        return {}
    
    # Extraire toutes les compétences
    all_skills = []
    for skills_list in df['skills']:
        if isinstance(skills_list, list):
            all_skills.extend(skills_list)
    
    # Compter les occurrences
    skill_counts = Counter(all_skills)
    
    # Obtenir les 20 compétences les plus fréquentes
    top_skills = dict(skill_counts.most_common(20))
    
    # Créer un dictionnaire de résultats
    results = {
        'top_skills': top_skills,
        'unique_skills': len(skill_counts),
        'total_mentions': sum(skill_counts.values())
    }
    
    # Créer un graphique
    plt.figure(figsize=(12, 8))
    top_skills_df = pd.DataFrame(list(top_skills.items()), columns=['Skill', 'Count'])
    sns.barplot(x='Count', y='Skill', data=top_skills_df.sort_values('Count', ascending=False))
    plt.title('Top 20 des Compétences les Plus Demandées')
    plt.xlabel('Nombre d\'offres')
    plt.ylabel('Compétence')
    plt.tight_layout()
    
    # Sauvegarder le graphique
    os.makedirs('reports/figures', exist_ok=True)
    plt.savefig('reports/figures/top_skills.png')
    plt.close()
    
    # Créer un nuage de mots
    wordcloud = WordCloud(width=800, height=400, background_color='white').generate_from_frequencies(skill_counts)
    plt.figure(figsize=(10, 5))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis('off')
    plt.tight_layout()
    
    # Sauvegarder le nuage de mots
    plt.savefig('reports/figures/skills_wordcloud.png')
    plt.close()
    
    return results

def analyze_locations(df):
    """
    Analyse la distribution géographique des offres.
    
    Args:
        df (pandas.DataFrame): DataFrame contenant les offres d'emploi
    
    Returns:
        dict: Statistiques sur les localisations
    """
    if df.empty or 'location' not in df.columns:
        return {}
    
    # Nettoyer et standardiser les localisations
    df['location_clean'] = df['location'].str.extract(r'([A-Za-zÀ-ÿ\-]+)')
    
    # Compter les localisations
    location_counts = df['location_clean'].value_counts().head(15)
    
    # Calculer les pourcentages
    location_percentages = (location_counts / len(df) * 100).round(2)
    
    # Créer un dictionnaire de résultats
    results = {
        'top_locations': location_counts.to_dict(),
        'percentages': location_percentages.to_dict(),
        'unique_locations': df['location_clean'].nunique(),
        'total': len(df)
    }
    
    # Créer un graphique
    plt.figure(figsize=(12, 8))
    sns.countplot(y='location_clean', data=df, order=location_counts.index)
    plt.title('Top 15 des Localisations')
    plt.xlabel('Nombre d\'offres')
    plt.ylabel('Localisation')
    plt.tight_layout()
    
    # Sauvegarder le graphique
    os.makedirs('reports/figures', exist_ok=True)
    plt.savefig('reports/figures/top_locations.png')
    plt.close()
    
    return results

def analyze_sources(df):
    """
    Analyse la distribution des sources d'offres.
    
    Args:
        df (pandas.DataFrame): DataFrame contenant les offres d'emploi
    
    Returns:
        dict: Statistiques sur les sources
    """
    if df.empty or 'source' not in df.columns:
        return {}
    
    # Compter les sources
    source_counts = df['source'].value_counts()
    
    # Calculer les pourcentages
    source_percentages = (source_counts / len(df) * 100).round(2)
    
    # Créer un dictionnaire de résultats
    results = {
        'counts': source_counts.to_dict(),
        'percentages': source_percentages.to_dict(),
        'total': len(df)
    }
    
    # Créer un graphique
    plt.figure(figsize=(10, 6))
    plt.pie(source_counts, labels=source_counts.index, autopct='%1.1f%%', startangle=90)
    plt.axis('equal')
    plt.title('Distribution des Sources d\'Offres')
    plt.tight_layout()
    
    # Sauvegarder le graphique
    os.makedirs('reports/figures', exist_ok=True)
    plt.savefig('reports/figures/sources_pie.png')
    plt.close()
    
    return results

def generate_report(df=None):
    """
    Génère un rapport complet d'analyse des offres d'emploi.
    
    Args:
        df (pandas.DataFrame, optional): DataFrame contenant les offres d'emploi
    
    Returns:
        dict: Résultats de l'analyse
    """
    # Charger les données si non fournies
    if df is None:
        # Essayer d'abord RDS
        df = load_jobs_from_rds()
        
        # Si RDS échoue, essayer S3
        if df.empty:
            df = load_jobs_from_s3()
        
        # Si S3 échoue, essayer les fichiers locaux
        if df.empty:
            df = load_jobs_from_local()
    
    if df.empty:
        logger.error("Impossible de charger les données d'offres d'emploi")
        return {}
    
    # Créer le répertoire de rapports
    os.makedirs('reports', exist_ok=True)
    
    # Effectuer les analyses
    logger.info("Génération du rapport d'analyse...")
    
    results = {
        'metadata': {
            'total_jobs': len(df),
            'generated_at': datetime.now().isoformat(),
            'sources': df['source'].unique().tolist() if 'source' in df.columns else [],
            'date_range': {
                'min': df['scraped_at'].min() if 'scraped_at' in df.columns else None,
                'max': df['scraped_at'].max() if 'scraped_at' in df.columns else None
            }
        },
        'contract_types': analyze_contract_types(df),
        'skills': analyze_skills(df),
        'locations': analyze_locations(df),
        'sources': analyze_sources(df)
    }
    
    # Sauvegarder les résultats
    with open('reports/job_analysis_report.json', 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=4)
    
    logger.info("Rapport d'analyse généré avec succès: reports/job_analysis_report.json")
    logger.info("Graphiques sauvegardés dans: reports/figures/")
    
    return results

if __name__ == "__main__":
    # Générer le rapport
    generate_report()
