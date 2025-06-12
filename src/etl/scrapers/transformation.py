#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Module de transformation de données pour le pipeline ETL.
Gère le nettoyage, l'enrichissement et la standardisation des données.
"""

import re
import logging
import pandas as pd
from datetime import datetime
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
import nltk

# Télécharger les ressources NLTK nécessaires
try:
    nltk.data.find('tokenizers/punkt')
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('punkt')
    nltk.download('stopwords')

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

def clean_job_data(jobs_data):
    """
    Nettoie et standardise les données d'offres d'emploi.
    
    Args:
        jobs_data (list): Liste des offres d'emploi brutes
    
    Returns:
        list: Liste des offres d'emploi nettoyées
    """
    cleaned_jobs = []
    
    for job in jobs_data:
        # Créer une copie pour ne pas modifier l'original
        cleaned_job = job.copy() if isinstance(job, dict) else {}
        
        # Standardiser les champs obligatoires
        cleaned_job['title'] = clean_text(job.get('title', ''))
        cleaned_job['company'] = clean_text(job.get('company', ''))
        cleaned_job['location'] = clean_text(job.get('location', ''))
        cleaned_job['description'] = clean_text(job.get('description', ''))
        cleaned_job['url'] = job.get('url', '')
        
        # Standardiser le type de contrat
        contract_type = job.get('contract_type', '').upper()
        if contract_type:
            if 'CDI' in contract_type:
                cleaned_job['contract_type'] = 'CDI'
            elif 'CDD' in contract_type:
                cleaned_job['contract_type'] = 'CDD'
            elif 'STAGE' in contract_type:
                cleaned_job['contract_type'] = 'STAGE'
            elif 'ALTERNANCE' in contract_type or 'APPRENTISSAGE' in contract_type:
                cleaned_job['contract_type'] = 'ALTERNANCE'
            elif 'FREELANCE' in contract_type or 'INDEPENDANT' in contract_type:
                cleaned_job['contract_type'] = 'FREELANCE'
            elif 'INTERIM' in contract_type:
                cleaned_job['contract_type'] = 'INTERIM'
            else:
                cleaned_job['contract_type'] = 'AUTRE'
        else:
            cleaned_job['contract_type'] = 'NON SPECIFIE'
        
        # Ajouter la source
        if 'source' not in cleaned_job:
            if 'pole-emploi' in job.get('url', '').lower():
                cleaned_job['source'] = 'POLE_EMPLOI'
            elif 'welcometothejungle' in job.get('url', '').lower():
                cleaned_job['source'] = 'WELCOME_JUNGLE'
            else:
                cleaned_job['source'] = 'AUTRE'
        
        # Standardiser les dates
        if 'scraped_at' in job:
            try:
                # Convertir en format ISO si ce n'est pas déjà le cas
                if isinstance(job['scraped_at'], str):
                    cleaned_job['scraped_at'] = parse_date(job['scraped_at'])
                else:
                    cleaned_job['scraped_at'] = job['scraped_at']
            except:
                cleaned_job['scraped_at'] = datetime.now().isoformat()
        else:
            cleaned_job['scraped_at'] = datetime.now().isoformat()
        
        # Extraire les compétences
        cleaned_job['skills'] = extract_skills(job.get('description', ''))
        
        cleaned_jobs.append(cleaned_job)
    
    logger.info(f"{len(cleaned_jobs)} offres d'emploi nettoyées")
    return cleaned_jobs

def clean_text(text):
    """
    Nettoie un texte en supprimant les caractères spéciaux et les espaces multiples.
    
    Args:
        text (str): Texte à nettoyer
    
    Returns:
        str: Texte nettoyé
    """
    if not text:
        return ""
    
    # Convertir en chaîne de caractères si ce n'est pas déjà le cas
    if not isinstance(text, str):
        text = str(text)
    
    # Supprimer les balises HTML
    text = re.sub(r'<.*?>', ' ', text)
    
    # Remplacer les caractères spéciaux par des espaces
    text = re.sub(r'[^\w\s\.\/\-\,]', ' ', text)
    
    # Remplacer les espaces multiples par un seul espace
    text = re.sub(r'\s+', ' ', text)
    
    return text.strip()

def parse_date(date_str):
    """
    Convertit une chaîne de date en format ISO.
    
    Args:
        date_str (str): Chaîne de date
    
    Returns:
        str: Date au format ISO
    """
    try:
        # Essayer différents formats
        formats = [
            '%Y-%m-%dT%H:%M:%S',  # ISO
            '%Y-%m-%dT%H:%M:%S.%f',  # ISO avec microsecondes
            '%Y-%m-%d %H:%M:%S',  # Format standard
            '%d/%m/%Y %H:%M:%S',  # Format français
            '%d/%m/%Y',  # Format français court
            '%Y-%m-%d'  # Format court
        ]
        
        for fmt in formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                return dt.isoformat()
            except ValueError:
                continue
        
        # Si aucun format ne correspond, essayer de parser avec pandas
        dt = pd.to_datetime(date_str)
        return dt.isoformat()
    except:
        # En cas d'échec, retourner la date actuelle
        return datetime.now().isoformat()

def extract_skills(description):
    """
    Extrait les compétences d'une description d'offre d'emploi.
    
    Args:
        description (str): Description de l'offre
    
    Returns:
        list: Liste des compétences extraites
    """
    if not description:
        return []
    
    # Liste de compétences techniques courantes
    tech_skills = [
        'python', 'java', 'javascript', 'js', 'c\+\+', 'c#', 'ruby', 'php', 'swift', 'kotlin',
        'sql', 'nosql', 'mongodb', 'postgresql', 'mysql', 'oracle', 'cassandra', 'redis',
        'aws', 'azure', 'gcp', 'cloud', 'docker', 'kubernetes', 'k8s', 'terraform', 'ansible',
        'git', 'github', 'gitlab', 'bitbucket', 'jenkins', 'ci/cd', 'devops',
        'react', 'angular', 'vue', 'node', 'django', 'flask', 'spring', 'laravel', 'symfony',
        'html', 'css', 'sass', 'less', 'bootstrap', 'tailwind',
        'tensorflow', 'pytorch', 'scikit-learn', 'pandas', 'numpy', 'matplotlib',
        'hadoop', 'spark', 'kafka', 'airflow', 'nifi',
        'rest', 'graphql', 'soap', 'api', 'microservices', 'soa',
        'agile', 'scrum', 'kanban', 'jira', 'confluence',
        'linux', 'unix', 'windows', 'macos', 'bash', 'powershell',
        'mobile', 'android', 'ios', 'react native', 'flutter',
        'data science', 'machine learning', 'ml', 'deep learning', 'dl', 'ai', 'nlp',
        'data engineer', 'data analyst', 'bi', 'business intelligence', 'tableau', 'power bi',
        'seo', 'sem', 'google analytics', 'google ads', 'facebook ads',
        'ux', 'ui', 'user experience', 'user interface', 'figma', 'sketch', 'adobe xd',
        'photoshop', 'illustrator', 'indesign', 'after effects', 'premiere pro'
    ]
    
    # Construire le pattern regex pour la recherche
    pattern = r'\b(' + '|'.join(tech_skills) + r')\b'
    
    # Trouver toutes les occurrences
    matches = re.finditer(pattern, description.lower())
    skills = set(match.group(1) for match in matches)
    
    # Ajouter des compétences linguistiques
    languages = ['anglais', 'english', 'français', 'french', 'allemand', 'german',
                'espagnol', 'spanish', 'italien', 'italian', 'chinois', 'chinese',
                'japonais', 'japanese', 'russe', 'russian', 'arabe', 'arabic']
    
    for lang in languages:
        if re.search(r'\b' + lang + r'\b', description.lower()):
            skills.add(lang)
    
    return list(skills)

def transform_to_dataframe(jobs_data):
    """
    Transforme une liste d'offres d'emploi en DataFrame pandas.
    
    Args:
        jobs_data (list): Liste des offres d'emploi
    
    Returns:
        pandas.DataFrame: DataFrame contenant les offres d'emploi
    """
    # Nettoyer les données
    cleaned_jobs = clean_job_data(jobs_data)
    
    # Convertir en DataFrame
    df = pd.DataFrame(cleaned_jobs)
    
    # Ajouter une colonne d'identifiant unique si nécessaire
    if 'id' not in df.columns:
        df['id'] = range(1, len(df) + 1)
    
    logger.info(f"DataFrame créé avec {len(df)} lignes et {len(df.columns)} colonnes")
    return df

if __name__ == "__main__":
    # Test de transformation
    from extraction import extract_welcome_jungle_data
    
    # Extraire les données
    jobs_data = extract_welcome_jungle_data()
    
    # Transformer les données
    df = transform_to_dataframe(jobs_data)
    
    # Afficher les statistiques
    print(f"Nombre d'offres: {len(df)}")
    print(f"Colonnes: {df.columns.tolist()}")
    print(f"Types de contrat: {df['contract_type'].value_counts().to_dict()}")
    print(f"Sources: {df['source'].value_counts().to_dict()}")
    
    # Afficher les compétences les plus fréquentes
    all_skills = [skill for skills_list in df['skills'] for skill in skills_list]
    skills_counts = pd.Series(all_skills).value_counts()
    print(f"Compétences les plus fréquentes: {skills_counts.head(10).to_dict()}")
