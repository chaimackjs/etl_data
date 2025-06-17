#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Module d'extraction des compétences pour le pipeline ETL.
Extrait et classifie les compétences (skills) depuis les descriptions d'emploi.
"""

import re
import logging
import pandas as pd
import nltk
from datetime import datetime
from collections import Counter

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/etl_skills_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Télécharger les ressources NLTK nécessaires (à exécuter une fois)
try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt', quiet=True)
    
try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords', quiet=True)

# Chargement des stopwords français et anglais
from nltk.corpus import stopwords
STOPWORDS = set(stopwords.words('french') + stopwords.words('english'))

# Dictionnaire des compétences techniques par catégorie
TECH_SKILLS = {
    'LANGUAGE': [
        'python', 'java', 'javascript', 'typescript', 'c++', 'c#', 'php', 'ruby', 'go', 'rust', 'swift',
        'kotlin', 'scala', 'perl', 'bash', 'powershell', 'r', 'matlab', 'sql', 'dart', 'flutter'
    ],
    'FRAMEWORK_BACKEND': [
        'django', 'flask', 'fastapi', 'spring', 'express', 'nest.js', 'laravel', 'symfony', 
        'rails', 'node.js', 'struts', 'asp.net', '.net', 'hibernate', 'quarkus'
    ],
    'FRAMEWORK_FRONTEND': [
        'react', 'angular', 'vue', 'svelte', 'jquery', 'ember', 'backbone', 'bootstrap', 
        'tailwind', 'material-ui', 'redux', 'gatsby', 'next.js', 'nuxt.js'
    ],
    'DATABASE': [
        'mysql', 'postgresql', 'oracle', 'mongodb', 'cassandra', 'redis', 'sqlite', 'mariadb',
        'couchbase', 'dynamodb', 'neo4j', 'elastic', 'elasticsearch', 'sql server', 'nosql', 'graphql'
    ],
    'CLOUD': [
        'aws', 'azure', 'gcp', 'google cloud', 'cloud computing', 's3', 'ec2', 'lambda', 
        'kubernetes', 'docker', 'openshift', 'terraform', 'cloudformation', 'serverless'
    ],
    'DATA_SCIENCE': [
        'machine learning', 'deep learning', 'tensorflow', 'pytorch', 'scikit-learn',
        'data mining', 'nlp', 'computer vision', 'ai', 'artificial intelligence', 'data science',
        'tableau', 'power bi', 'hadoop', 'spark', 'big data', 'data warehouse', 'etl', 'pandas'
    ],
    'DEVOPS': [
        'jenkins', 'gitlab', 'github actions', 'travis ci', 'circleci', 'ansible', 'chef', 'puppet',
        'ci/cd', 'git', 'docker', 'kubernetes', 'prometheus', 'grafana', 'jira', 'terraform'
    ],
    'METHODOLOGIES': [
        'agile', 'scrum', 'kanban', 'lean', 'waterfall', 'devops', 'tdd', 'bdd', 'pair programming'
    ]
}

# Créer un dictionnaire inversé pour la recherche efficace
SKILLS_CATEGORIES = {}
for category, skills in TECH_SKILLS.items():
    for skill in skills:
        SKILLS_CATEGORIES[skill.lower()] = category

def extract_skills_from_text(text):
    """
    Extrait les compétences techniques à partir d'un texte.
    
    Args:
        text (str): Texte à analyser (description d'offre d'emploi)
        
    Returns:
        list: Liste des compétences détectées avec leurs catégories
    """
    if not isinstance(text, str) or not text:
        return []
    
    text = text.lower()
    found_skills = []
    
    # Rechercher les compétences dans le texte
    for skill, category in SKILLS_CATEGORIES.items():
        # Utiliser des expressions régulières pour trouver des mots entiers
        pattern = r'\b' + re.escape(skill) + r'\b'
        if re.search(pattern, text):
            found_skills.append({
                'name': skill,
                'category': category
            })
    
    return found_skills

def extract_skills_from_dataframe(df, text_column='description_clean'):
    """
    Extrait les compétences techniques à partir des descriptions des offres d'emploi.
    
    Args:
        df (pandas.DataFrame): DataFrame contenant les offres d'emploi
        text_column (str): Nom de la colonne contenant les descriptions
        
    Returns:
        tuple: (DataFrame des skills uniques, DataFrame de la relation job_skills)
    """
    if df is None or text_column not in df.columns:
        logger.error(f"Impossible d'extraire les compétences: DataFrame invalide ou colonne {text_column} manquante")
        return None, None
    
    logger.info(f"Extraction des compétences à partir de {len(df)} offres d'emploi")
    
    # Dictionnaire pour stocker les compétences par offre
    job_skills_data = []
    all_skills = []
    
    # Parcourir les offres d'emploi
    for _, row in df.iterrows():
        job_id = row.get('id')
        text = row.get(text_column)
        
        if not job_id or not text:
            continue
        
        # Extraire les compétences
        skills = extract_skills_from_text(text)
        
        # Enregistrer les compétences trouvées
        for skill in skills:
            all_skills.append(skill)
            job_skills_data.append({
                'job_id': job_id,
                'skill_name': skill['name']
            })
    
    # Créer un DataFrame unique de compétences
    if all_skills:
        # Convertir en DataFrame
        all_skills_df = pd.DataFrame(all_skills)
        
        # Supprimer les doublons
        skills_df = all_skills_df.drop_duplicates(subset=['name'])
        
        # Ajouter une colonne created_at
        skills_df['created_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
    else:
        logger.warning("Aucune compétence trouvée dans les offres d'emploi")
        skills_df = pd.DataFrame(columns=['name', 'category', 'created_at'])
    
    # Créer un DataFrame pour la table de liaison job_skills
    job_skills_df = pd.DataFrame(job_skills_data) if job_skills_data else pd.DataFrame(columns=['job_id', 'skill_name'])
    
    logger.info(f"Extraction terminée: {len(skills_df)} compétences uniques trouvées dans {len(job_skills_df)} relations")
    
    return skills_df, job_skills_df

def get_skills_frequency(job_skills_df, skills_df=None):
    """
    Analyse la fréquence des compétences dans les offres d'emploi.
    
    Args:
        job_skills_df (pandas.DataFrame): DataFrame de la relation job_skills
        skills_df (pandas.DataFrame): DataFrame des compétences (facultatif)
        
    Returns:
        pandas.DataFrame: DataFrame avec les statistiques de fréquence des compétences
    """
    if job_skills_df is None or job_skills_df.empty:
        return None
    
    # Compter les occurrences de chaque compétence
    skill_counts = Counter(job_skills_df['skill_name'])
    
    # Créer un DataFrame de fréquences
    freq_df = pd.DataFrame({
        'skill_name': list(skill_counts.keys()),
        'frequency': list(skill_counts.values())
    })
    
    # Trier par fréquence décroissante
    freq_df = freq_df.sort_values('frequency', ascending=False).reset_index(drop=True)
    
    # Ajouter la catégorie si skills_df est fourni
    if skills_df is not None and not skills_df.empty:
        # Créer un dictionnaire name -> category
        skill_categories = dict(zip(skills_df['name'], skills_df['category']))
        freq_df['category'] = freq_df['skill_name'].apply(lambda x: skill_categories.get(x))
    
    return freq_df

def generate_skills_report(skills_freq_df):
    """
    Génère un rapport d'analyse des compétences trouvées dans les offres d'emploi.
    
    Args:
        skills_freq_df (pandas.DataFrame): DataFrame avec les statistiques de fréquence des compétences
        
    Returns:
        dict: Rapport d'analyse des compétences
    """
    if skills_freq_df is None or skills_freq_df.empty:
        return {
            'total_skills': 0,
            'categories': {},
            'top_skills': []
        }
    
    # Nombre total de compétences uniques
    total_skills = len(skills_freq_df)
    
    # Les compétences les plus demandées (top 10)
    top_skills = skills_freq_df.head(10)[['skill_name', 'frequency']].to_dict('records')
    
    # Répartition par catégorie
    categories = {}
    if 'category' in skills_freq_df.columns:
        category_counts = skills_freq_df['category'].value_counts().to_dict()
        for cat, count in category_counts.items():
            categories[cat] = {
                'count': count,
                'percentage': round((count / total_skills) * 100, 2)
            }
    
    return {
        'total_skills': total_skills,
        'categories': categories,
        'top_skills': top_skills
    }

if __name__ == "__main__":
    # Test du module avec les données extraites
    from extraction import extract_by_date_range
    from transformation import transform_job_dataframe
    
    # Récupérer les données du jour
    today = datetime.now().strftime("%Y%m%d")
    raw_df = extract_by_date_range(today)
    
    if raw_df is not None:
        # Transformer les données
        transformed_df = transform_job_dataframe(raw_df)
        
        if transformed_df is not None:
            # Extraire les compétences
            skills_df, job_skills_df = extract_skills_from_dataframe(transformed_df)
            
            # Analyser la fréquence des compétences
            freq_df = get_skills_frequency(job_skills_df, skills_df)
            
            if freq_df is not None:
                # Afficher les compétences les plus demandées
                print("\nTop 10 compétences les plus demandées:")
                print(freq_df.head(10))
                
                # Générer et afficher le rapport
                report = generate_skills_report(freq_df)
                print("\nRapport d'analyse des compétences:")
                print(f"Nombre total de compétences uniques: {report['total_skills']}")
                print("Répartition par catégorie:")
                for cat, data in report['categories'].items():
                    print(f"  - {cat}: {data['count']} ({data['percentage']}%)")
