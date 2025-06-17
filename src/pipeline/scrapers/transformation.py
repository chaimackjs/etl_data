#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Module de transformation des données Welcome to the Jungle.
"""

import os
import json
import pandas as pd
import numpy as np
import re
import logging
from datetime import datetime

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Import du module d'extraction
from src.pipeline.scrapers.extraction import extract_welcome_jungle_data

def detect_technologies(text):
    """
    Détecte les technologies mentionnées dans un texte.
    
    Args:
        text (str): Texte à analyser
        
    Returns:
        dict: Dictionnaire des technologies détectées
    """
    if not text or not isinstance(text, str) or text == 'Description non disponible':
        return {}
    
    # Prétraitement du texte pour améliorer la détection
    text_lower = text.lower()
    
    # Remplacer les caractères spéciaux par des espaces pour éviter les faux positifs
    text_lower = re.sub(r'[\.,;:!\?\(\)\[\]\{\}]', ' ', text_lower)
    
    # Ajouter des espaces autour des symboles pour faciliter la détection
    text_lower = re.sub(r'([\+\-\*\/\=])', r' \1 ', text_lower)
    
    # Liste simplifiée des technologies à détecter avec leurs mots-clés associés
    # Nous utilisons des mots-clés plus génériques pour augmenter les chances de détection
    technologies = {
        # Langages de programmation
        'has_python': ['python'],
        'has_java': ['java'],
        'has_javascript': ['javascript', 'js', 'node', 'react', 'angular', 'vue'],
        'has_typescript': ['typescript', 'ts'],
        'has_php': ['php'],
        'has_ruby': ['ruby', 'rails'],
        'has_csharp': ['c#', 'csharp', '.net', 'dotnet'],
        'has_cpp': ['c++', 'cpp'],
        'has_c': ['langage c', 'programmation c'],
        'has_go': ['golang', 'go language'],
        
        # Bases de données
        'has_sql': ['sql', 'mysql', 'postgresql', 'postgres', 'oracle', 'database'],
        'has_nosql': ['nosql', 'mongodb', 'redis', 'elasticsearch'],
        
        # Cloud et infrastructure
        'has_aws': ['aws', 'amazon web', 's3', 'ec2', 'lambda'],
        'has_azure': ['azure', 'microsoft azure'],
        'has_gcp': ['gcp', 'google cloud'],
        'has_docker': ['docker', 'container'],
        'has_kubernetes': ['kubernetes', 'k8s'],
        
        # Front-end
        'has_html': ['html', 'html5'],
        'has_css': ['css', 'css3', 'sass', 'scss'],
        'has_react': ['react', 'reactjs', 'react.js'],
        'has_angular': ['angular', 'angularjs'],
        'has_vue': ['vue', 'vuejs', 'vue.js'],
        
        # Data Science et IA
        'has_datascience': ['data science', 'machine learning', 'intelligence artificielle', 'ai', 'ml'],
        
        # Autres technologies importantes
        'has_git': ['git', 'github', 'gitlab'],
        'has_agile': ['agile', 'scrum', 'kanban', 'jira']
    }
    
    # Résultat de la détection
    result = {}
    
    # Détecter chaque technologie
    for tech_key, keywords in technologies.items():
        # Vérifier la présence de chaque mot-clé
        for keyword in keywords:
            # Recherche simple pour maximiser les chances de détection
            if keyword in text_lower:
                result[tech_key] = True
                break
        else:
            # Si aucun mot-clé n'est trouvé, la technologie n'est pas présente
            result[tech_key] = False
    
    # Vérifier si au moins une technologie a été détectée
    if not any(result.values()):
        # Si aucune technologie n'a été détectée, essayer une détection plus souple
        for tech_key, keywords in technologies.items():
            for keyword in keywords:
                # Recherche plus souple en découpant le texte en mots
                words = text_lower.split()
                for word in words:
                    if keyword in word:
                        result[tech_key] = True
                        break
            if tech_key not in result:
                result[tech_key] = False
    
    return result

def normalize_contract_type(contract_type, title=None):
    """
    Normalise le type de contrat.
    
    Args:
        contract_type (str): Type de contrat brut
        title (str, optional): Titre de l'offre d'emploi pour aider à la normalisation
        
    Returns:
        str: Type de contrat normalisé (CDI, CDD, STAGE, ALTERNANCE, FREELANCE, OTHER)
    """
    # Préparation des variables
    contract_info = ''
    if contract_type and isinstance(contract_type, str) and contract_type != 'Type de contrat non disponible':
        contract_info = contract_type.lower()
    
    title_info = ''
    if title and isinstance(title, str):
        title_info = title.lower()
    
    combined_text = f"{contract_info} {title_info}".strip()
    if not combined_text:
        return 'OTHER'
    
    # Dictionnaire des mots-clés par type de contrat (enrichi)
    contract_keywords = {
        'CDI': ['cdi', 'indéterminé', 'permanent', 'indefinite', 'full time', 'temps plein', 'permanent contract',
               'contrat à durée indéterminée', 'open-ended', 'long-term', 'long term', 'full-time', 'fulltime',
               'permanent position', 'poste permanent', 'emploi permanent', 'permanent job', 'contrat permanent',
               'position permanente', 'poste fixe', 'poste à temps plein', 'emploi à temps plein', 'job permanent'],
        
        'CDD': ['cdd', 'déterminé', 'determined', 'fixed term', 'temporary', 'contrat à durée déterminée',
               'short-term', 'short term', 'contract duration', 'durée du contrat', 'interim', 'intérim',
               'contrat temporaire', 'temporary position', 'poste temporaire', 'emploi temporaire', 'temporary job',
               'contrat à terme', 'terme fixe', 'fixed duration', 'durée déterminée', 'durée limitée',
               'limited duration', 'limited term', 'terme limité', 'contrat limité', 'contrat à durée limitée'],
        
        'STAGE': ['stage', 'stagiaire', 'internship', 'intern', 'trainee', 'training period', 'période de formation',
                 'période d\'essai', 'probation', 'probationary', 'stage conventionné', 'stage étudiant',
                 'student internship', 'graduate internship', 'stage de fin d\'\u00e9tudes', 'stage de master',
                 'stage de licence', 'stage de bachelor', 'stage de formation', 'training internship',
                 'stage professionnel', 'professional internship', 'stage rémunéré', 'paid internship',
                 'stage non rémunéré', 'unpaid internship', 'summer internship', 'stage d\'\u00e9té'],
        
        'ALTERNANCE': ['alternance', 'apprentissage', 'apprenti', 'contrat pro', 'professionnalisation',
                      'work-study', 'work study', 'dual training', 'formation en alternance', 'formation alternée',
                      'contrat d\'apprentissage', 'contrat de professionnalisation', 'apprenticeship',
                      'contrat en alternance', 'alternant', 'apprentice', 'formation professionnelle',
                      'professional training', 'formation duale', 'dual education', 'étudiant alternant',
                      'alternating student', 'alternating training', 'formation par alternance',
                      'formation professionnelle alternée', 'formation professionnalisante'],
        
        'FREELANCE': ['freelance', 'indépendant', 'consultant', 'contractor', 'self-employed', 'auto-entrepreneur',
                     'free-lance', 'travailleur indépendant', 'prestataire', 'external', 'freelancer',
                     'independent contractor', 'contractuel', 'contractual', 'consultant externe',
                     'external consultant', 'consultant indépendant', 'independent consultant',
                     'travail indépendant', 'independent work', 'mission freelance', 'freelance mission',
                     'mission de consulting', 'consulting mission', 'prestation de service',
                     'service provision', 'prestation externe', 'external service']
    }
    
    # Vérifier chaque type de contrat
    for contract_type_std, keywords in contract_keywords.items():
        for keyword in keywords:
            if keyword in combined_text:
                return contract_type_std
    
    # Vérification supplémentaire pour les cas particuliers
    if any(term in combined_text for term in ['temps partiel', 'part time', 'part-time', 'mi-temps', 'half-time']):
        # Le temps partiel est souvent associé à un CDI, mais vérifions d'abord si c'est un stage
        if any(term in combined_text for term in ['stage', 'intern', 'stagiaire', 'internship']):
            return 'STAGE'
        # Sinon, c'est probablement un CDI
        return 'CDI'
    
    # Mots-clés spécifiques aux postes commerciaux (souvent freelance ou CDI)
    if any(term in combined_text for term in ['vdi', 'vendeur', 'commercial', 'sales', 'vente', 'business developer']):
        if any(term in combined_text for term in ['indépendant', 'commission', 'commission-based']):
            return 'FREELANCE'
        return 'CDI'  # Par défaut, les postes commerciaux sont souvent en CDI
    
    # Postes liés aux étudiants (souvent stages ou alternance)
    if any(term in combined_text for term in ['étudiant', 'student', 'formation', 'école', 'school', 'university', 'université', 'master', 'bachelor', 'licence', 'bac+']):
        # Vérifier si c'est spécifiquement une alternance
        if any(term in combined_text for term in ['alternance', 'apprentissage', 'apprenti', 'work-study']):
            return 'ALTERNANCE'
        # Sinon, c'est probablement un stage
        return 'STAGE'
    
    # Postes temporaires ou basés sur des projets (souvent CDD)
    if any(term in combined_text for term in ['temporaire', 'saisonnier', 'seasonal', 'mission', 'project-based', 'project based', 'projet', 'project', 'durée limitée', 'limited duration', 'limited time']):
        return 'CDD'
    
    # Postes de direction ou cadres supérieurs (souvent CDI)
    if any(term in combined_text for term in ['directeur', 'director', 'manager', 'responsable', 'head of', 'chief', 'senior', 'lead', 'executive']):
        return 'CDI'
    
    # Postes techniques ou d'ingénierie (souvent CDI)
    if any(term in combined_text for term in ['ingénieur', 'engineer', 'developer', 'développeur', 'technicien', 'technician', 'analyst', 'analyste']):
        # Vérifier si c'est spécifiquement un stage ou une alternance
        if any(term in combined_text for term in ['stage', 'intern', 'alternance', 'apprenti']):
            if 'alternance' in combined_text or 'apprenti' in combined_text:
                return 'ALTERNANCE'
            return 'STAGE'
        # Sinon, c'est probablement un CDI
        return 'CDI'
    
    # Par défaut, si le contrat est mentionné comme "contrat" sans précision, c'est souvent un CDI
    if 'contrat' in combined_text and not any(word in combined_text for word in ['cdd', 'stage', 'alternance', 'freelance']):
        return 'CDI'
    
    # Si le titre contient "offre d'emploi" sans autre précision, c'est souvent un CDI
    if 'offre d\'emploi' in combined_text or 'job offer' in combined_text or 'job opening' in combined_text:
        return 'CDI'
    
    return 'OTHER'

def extract_salary_info(salary_text):
    """
    Extrait les informations de salaire à partir d'un texte.
    
    Args:
        salary_text (str): Texte contenant les informations de salaire
        
    Returns:
        dict: Dictionnaire contenant min_salary, max_salary, currency et period
    """
    if not salary_text or not isinstance(salary_text, str):
        return {'min_salary': None, 'max_salary': None, 'salary_currency': None, 'salary_period': None}
    
    # Normalisation du texte
    salary_text = salary_text.lower().strip()
    
    # Cas spéciaux
    if salary_text in ['à négocier', 'negotiable', 'à discuter', 'selon profil', 'selon expérience', 'competitive']:
        return {'min_salary': None, 'max_salary': None, 'salary_currency': None, 'salary_period': 'NEGOTIABLE'}
    
    # Détection de la devise
    currency = None
    if '€' in salary_text or 'eur' in salary_text or 'euro' in salary_text:
        currency = 'EUR'
    elif '$' in salary_text or 'usd' in salary_text or 'dollar' in salary_text:
        currency = 'USD'
    elif '£' in salary_text or 'gbp' in salary_text or 'livre' in salary_text:
        currency = 'GBP'
    
    # Détection de la période
    period = None
    if any(word in salary_text for word in ['an', 'année', 'year', 'annual', 'annuel', '/an', '/year', 'par an', 'per year']):
        period = 'YEARLY'
    elif any(word in salary_text for word in ['mois', 'month', 'mensuel', 'monthly', '/mois', '/month', 'par mois', 'per month']):
        period = 'MONTHLY'
    elif any(word in salary_text for word in ['jour', 'day', 'daily', 'journalier', '/jour', '/day', 'par jour', 'per day']):
        period = 'DAILY'
    elif any(word in salary_text for word in ['heure', 'hour', 'hourly', 'horaire', '/heure', '/hour', 'par heure', 'per hour', '/40', '/35', '/39']):
        period = 'HOURLY'
    elif any(word in salary_text for word in ['semaine', 'week', 'hebdomadaire', 'weekly', '/semaine', '/week', 'par semaine', 'per week']):
        period = 'WEEKLY'
    
    # Extraction des valeurs numériques
    # Supprimer les symboles de devise pour faciliter l'extraction
    clean_text = salary_text.replace('€', '').replace('$', '').replace('£', '')
    
    # Traiter les formats spéciaux comme "37/40" (heures par semaine)
    hourly_match = re.search(r'(\d+)\s*[/]\s*(\d+)', clean_text)
    if hourly_match and (int(hourly_match.group(2)) in [35, 37, 38, 39, 40, 42]):
        return {'min_salary': None, 'max_salary': None, 'salary_currency': None, 'salary_period': 'HOURLY'}
    
    # Recherche de plages de salaire (ex: 30000 - 40000)
    range_match = re.search(r'(\d+[\s,.]?\d*)\s*[-–à]\s*(\d+[\s,.]?\d*)', clean_text)
    if range_match:
        min_salary_str = range_match.group(1).replace(' ', '').replace(',', '.')
        max_salary_str = range_match.group(2).replace(' ', '').replace(',', '.')
        try:
            min_salary = float(min_salary_str)
            max_salary = float(max_salary_str)
            
            # Convertir les valeurs en k (ex: 30k -> 30000)
            if 'k' in salary_text:
                if min_salary < 1000:
                    min_salary *= 1000
                if max_salary < 1000:
                    max_salary *= 1000
            
            # Déduction de la période si non spécifiée
            if not period:
                if min_salary < 100:  # Probablement un taux horaire
                    period = 'HOURLY'
                elif min_salary < 10000:  # Probablement un salaire mensuel
                    period = 'MONTHLY'
                else:  # Probablement un salaire annuel
                    period = 'YEARLY'
            
            return {'min_salary': min_salary, 'max_salary': max_salary, 'salary_currency': currency, 'salary_period': period}
        except ValueError:
            pass
    
    # Recherche d'une valeur unique (ex: 35000)
    single_match = re.search(r'(\d+[\s,.]?\d*)', clean_text)
    if single_match:
        salary_str = single_match.group(1).replace(' ', '').replace(',', '.')
        try:
            salary_value = float(salary_str)
            
            # Convertir les valeurs en k (ex: 30k -> 30000)
            if 'k' in salary_text and salary_value < 1000:
                salary_value *= 1000
            
            # Déduction de la période si non spécifiée
            if not period:
                if salary_value < 100:  # Probablement un taux horaire
                    period = 'HOURLY'
                elif salary_value < 10000:  # Probablement un salaire mensuel
                    period = 'MONTHLY'
                else:  # Probablement un salaire annuel
                    period = 'YEARLY'
            
            return {'min_salary': salary_value, 'max_salary': None, 'salary_currency': currency, 'salary_period': period}
        except ValueError:
            pass
    
    # Si on arrive ici, c'est qu'on n'a pas réussi à extraire de valeur numérique
    return {'min_salary': None, 'max_salary': None, 'salary_currency': currency, 'salary_period': period}

def normalize_location(location):
    """
    Normalise le lieu de travail.
    
    Args:
        location (str): Lieu de travail brut
        
    Returns:
        str: Lieu de travail normalisé
    """
    if not location or not isinstance(location, str) or location.lower() == 'lieu non disponible':
        return 'Télétravail / Remote'
    
    # Nettoyer le lieu de travail
    location = location.strip()
    
    # Supprimer les mentions inutiles
    location = re.sub(r'\(.*?\)', '', location).strip()
    
    # Normaliser les villes françaises courantes
    location_lower = location.lower()
    if 'paris' in location_lower:
        return 'Paris'
    elif 'lyon' in location_lower:
        return 'Lyon'
    elif 'marseille' in location_lower:
        return 'Marseille'
    elif 'toulouse' in location_lower:
        return 'Toulouse'
    elif 'bordeaux' in location_lower:
        return 'Bordeaux'
    elif 'lille' in location_lower:
        return 'Lille'
    elif 'nantes' in location_lower:
        return 'Nantes'
    elif 'strasbourg' in location_lower:
        return 'Strasbourg'
    elif 'montpellier' in location_lower:
        return 'Montpellier'
    elif 'nice' in location_lower:
        return 'Nice'
    
    return location

def extract_experience(experience, title=None):
    """
    Extrait l'expérience requise.
    
    Args:
        experience (str): Texte contenant l'expérience requise
        title (str): Titre de l'offre d'emploi (optionnel)
        
    Returns:
        tuple: (min_years, max_years, experience_level)
    """
    if not experience or not isinstance(experience, str) or experience.lower() == 'expérience non spécifiée':
        # Essayer d'extraire l'expérience du titre si disponible
        if title and isinstance(title, str):
            title_lower = title.lower()
            if 'junior' in title_lower:
                return 0, 2, 'JUNIOR'
            elif 'senior' in title_lower or 'expert' in title_lower:
                return 5, None, 'SENIOR'
            elif 'confirmé' in title_lower or 'confirmée' in title_lower or 'mid' in title_lower:
                return 2, 5, 'CONFIRMED'
        return None, None, 'Tous niveaux d\'expérience'
    
    experience_lower = experience.lower()
    
    # Détecter le niveau d'expérience
    if any(word in experience_lower for word in ['junior', 'débutant', 'debutant', 'entry']):  
        return 0, 2, 'JUNIOR'
    elif any(word in experience_lower for word in ['senior', 'expert', 'confirmé', 'confirmée', 'experienced']):  
        return 5, None, 'SENIOR'
    elif any(word in experience_lower for word in ['mid', 'intermédiaire', 'intermediaire', 'confirmé', 'confirmée']):  
        return 2, 5, 'CONFIRMED'
    
    # Rechercher des patterns d'années d'expérience
    patterns = [
        r'(\d+)[\s-]*(\d+)\s*an', # 2-5 ans
        r'(\d+)\+?\s*an',          # 5+ ans ou 5 ans
        r'minimum\s*(\d+)\s*an',   # minimum 3 ans
        r'au moins\s*(\d+)\s*an'   # au moins 3 ans
    ]
    
    for pattern in patterns:
        match = re.search(pattern, experience_lower)
        if match:
            groups = match.groups()
            if len(groups) == 2:
                # Plage d'années
                min_years = int(groups[0])
                max_years = int(groups[1])
                level = 'JUNIOR' if min_years < 2 else 'SENIOR' if min_years >= 5 else 'CONFIRMED'
                return min_years, max_years, level
            elif len(groups) == 1:
                # Années minimum
                min_years = int(groups[0])
                level = 'JUNIOR' if min_years < 2 else 'SENIOR' if min_years >= 5 else 'CONFIRMED'
                return min_years, None, level
    
    return None, None, 'Tous niveaux d\'expérience'

def transform_welcome_jungle_data(df):
    """
    Transforme les données Welcome to the Jungle.
    
    Args:
        df (pandas.DataFrame): DataFrame contenant les données brutes
        
    Returns:
        pandas.DataFrame: DataFrame contenant les données transformées
    """
    try:
        logger.info(f"Début de la transformation des données Welcome to the Jungle: {len(df)} enregistrements")
        
        # Copier le DataFrame pour éviter de modifier l'original
        transformed_df = df.copy()
        
        # Normaliser les types de contrat en utilisant aussi le titre
        transformed_df['contract_type_std'] = transformed_df.apply(
            lambda row: normalize_contract_type(row['contract_type'], row['title']), axis=1
        )
        
        # Normaliser les lieux de travail
        # Garder le nom de colonne 'location' pour correspondre au schéma de la table
        transformed_df['location'] = transformed_df['location'].apply(normalize_location)
        
        # Extraire les informations de salaire
        salary_info = transformed_df['salary'].apply(extract_salary_info)
        transformed_df['min_salary'] = salary_info.apply(lambda x: x.get('min_salary'))
        transformed_df['max_salary'] = salary_info.apply(lambda x: x.get('max_salary'))
        transformed_df['salary_currency'] = salary_info.apply(lambda x: x.get('salary_currency'))
        transformed_df['salary_period'] = salary_info.apply(lambda x: x.get('salary_period'))
        
        # Extraire les informations d'expérience
        experience_info = transformed_df.apply(
            lambda row: extract_experience(row['experience'], row['title']), axis=1
        )
        transformed_df['min_experience'] = experience_info.apply(lambda x: x[0])
        transformed_df['max_experience'] = experience_info.apply(lambda x: x[1])
        transformed_df['experience_level'] = experience_info.apply(lambda x: x[2])
        
        # Détecter les technologies mentionnées dans la description
        tech_columns = []
        for _, row in transformed_df.iterrows():
            tech_dict = detect_technologies(row['description'])
            for tech_col, has_tech in tech_dict.items():
                if tech_col not in transformed_df.columns:
                    transformed_df[tech_col] = False
                    tech_columns.append(tech_col)
                transformed_df.at[_, tech_col] = has_tech
        
        # Ajouter des colonnes supplémentaires
        transformed_df['source'] = 'welcome_jungle'
        transformed_df['processing_date'] = datetime.now().strftime('%Y-%m-%d')
        
        # Générer un identifiant unique pour chaque offre (colonne id)
        transformed_df['id'] = transformed_df.apply(
            lambda row: f"wj_{row['title'].replace(' ', '_').lower()}_{datetime.now().strftime('%Y%m%d')}_{hash(row['url']) % 10000}", 
            axis=1
        )
        
        # S'assurer que les colonnes correspondent au schéma de la table
        transformed_df['url'] = transformed_df['url']
        transformed_df['company_name'] = transformed_df['company']  # Renommer company en company_name
        transformed_df['city'] = transformed_df.apply(lambda row: row['location'].split(',')[0].strip() if ',' in row['location'] else row['location'], axis=1)
        transformed_df['country'] = 'France'  # Par défaut, on suppose que les offres sont en France
        transformed_df['contract_type'] = transformed_df['contract_type']  # Garder le type de contrat original
        transformed_df['description'] = transformed_df['description']
        transformed_df['category'] = transformed_df.get('category', 'Non spécifié')
        transformed_df['experience'] = transformed_df['experience']
        
        # Convertir la date de publication au format standard
        # Renommer publication_date en date pour correspondre au schéma de la table
        transformed_df['date'] = pd.to_datetime(transformed_df['publication_date']).dt.strftime('%Y-%m-%d')
        
        # Sélectionner les colonnes finales
        final_columns = [
            'id',
            'title',
            'company_name',
            'location',
            'city',
            'country',
            'contract_type',
            'contract_type_std',
            'date',
            'url',
            'description',
            'category',
            'experience',
            'min_experience',
            'max_experience',
            'experience_level',
            'salary',
            'min_salary',
            'max_salary',
            'salary_currency',
            'salary_period',
            'source',
            'processing_date'
        ]
        
        # Ajouter les colonnes de technologies
        final_columns.extend(tech_columns)
        
        # Sélectionner uniquement les colonnes qui existent dans le DataFrame
        existing_columns = [col for col in final_columns if col in transformed_df.columns]
        result_df = transformed_df[existing_columns]
        
        logger.info(f"Transformation terminée: {len(result_df)} enregistrements")
        
        return result_df
        
    except Exception as e:
        logger.error(f"Erreur lors de la transformation des données Welcome to the Jungle: {e}")
        return pd.DataFrame()

def save_transformed_data(df, output_dir=None):
    """
    Sauvegarde les données transformées dans un fichier CSV.
    
    Args:
        df (pandas.DataFrame): DataFrame contenant les données transformées
        output_dir (str): Répertoire de sortie (optionnel)
        
    Returns:
        str: Chemin du fichier CSV généré
    """
    try:
        if df.empty:
            logger.warning("DataFrame vide, aucune sauvegarde effectuée")
            return None
        
        # Définir le répertoire de sortie
        if not output_dir:
            output_dir = os.path.join('data', 'processed', 'welcome_jungle')
        
        # Créer le répertoire s'il n'existe pas
        os.makedirs(output_dir, exist_ok=True)
        
        # Générer le nom du fichier avec la date actuelle
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = os.path.join(output_dir, f"welcome_jungle_transformed_{timestamp}.csv")
        
        # Sauvegarder en CSV
        df.to_csv(output_file, index=False, encoding='utf-8')
        logger.info(f"Données transformées sauvegardées dans: {output_file}")
        
        return output_file
        
    except Exception as e:
        logger.error(f"Erreur lors de la sauvegarde des données transformées: {e}")
        return None

def main():
    """
    Fonction principale pour tester la transformation.
    """
    # Extraction des données
    df, _ = extract_welcome_jungle_data(specific_file="welcome_jungle_all_jobs_all_locations_20250614_175507.json")
    
    if not df.empty:
        # Transformation des données
        transformed_df = transform_welcome_jungle_data(df)
        
        if not transformed_df.empty:
            # Sauvegarde des données transformées
            output_file = save_transformed_data(transformed_df)
            
            if output_file:
                logger.info(f"Pipeline de transformation terminé avec succès")
                
                # Afficher quelques statistiques
                logger.info(f"Nombre total d'offres: {len(transformed_df)}")
                
                if 'contract_type_std' in transformed_df.columns:
                    contract_counts = transformed_df['contract_type_std'].value_counts()
                    logger.info("Distribution des types de contrat:")
                    for contract_type, count in contract_counts.items():
                        logger.info(f"  {contract_type}: {count} offres")
                
                # Statistiques sur les technologies
                tech_cols = [col for col in transformed_df.columns if col.startswith('has_')]
                if tech_cols:
                    tech_counts = {col.replace('has_', ''): transformed_df[col].sum() for col in tech_cols}
                    tech_counts = {k: v for k, v in sorted(tech_counts.items(), key=lambda item: item[1], reverse=True) if v > 0}
                    
                    if tech_counts:
                        logger.info("Technologies détectées:")
                        for tech, count in tech_counts.items():
                            logger.info(f"  {tech}: {count} offres")
            else:
                logger.error("Erreur lors de la sauvegarde des données transformées")
        else:
            logger.error("Erreur lors de la transformation des données")
    else:
        logger.error("Erreur lors de l'extraction des données")

if __name__ == "__main__":
    main()