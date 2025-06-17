#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Module d'analyse des offres d'emploi Welcome to the Jungle.
Fournit des fonctions pour analyser les tendances et statistiques des offres d'emploi.
"""

import os
import json
import logging
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from collections import Counter
from datetime import datetime, date
from dotenv import load_dotenv
import psycopg2
from wordcloud import WordCloud
import numpy as np

# Classe d'encodeur JSON personnalisée pour gérer les objets de type date
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        elif isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super(DateTimeEncoder, self).default(obj)

# Charger les variables d'environnement
load_dotenv()

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

# Configuration du style personnalisé pour les visualisations
plt.style.use('dark_background')

# Palette de couleurs personnalisée
custom_palette = ['#FF5733', '#33FF57', '#3357FF', '#F033FF', '#FF33A8', '#33FFF5', '#FFD133']
sns.set_palette(custom_palette)

def load_welcome_jungle_data_from_db():
    """
    Charge les offres d'emploi Welcome to the Jungle depuis la base de données PostgreSQL.
    
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
        
        # Requête SQL pour récupérer les offres Welcome to the Jungle
        query = """
            SELECT *
            FROM welcome_jungle_jobs
        """
        
        # Exécuter la requête et charger les résultats dans un DataFrame
        df = pd.read_sql_query(query, connection)
        
        # Fermer la connexion
        connection.close()
        
        logger.info(f"Chargé {len(df)} offres d'emploi Welcome to the Jungle depuis la base de données")
        return df
    
    except Exception as e:
        logger.error(f"Erreur lors du chargement des offres depuis la base de données: {e}")
        return pd.DataFrame()

def load_welcome_jungle_data_from_csv(directory='data/processed/welcome_jungle/'):
    """
    Charge les offres d'emploi Welcome to the Jungle depuis des fichiers CSV locaux.
    
    Args:
        directory (str): Répertoire contenant les fichiers CSV
    
    Returns:
        pandas.DataFrame: DataFrame contenant les offres d'emploi
    """
    try:
        # Vérifier si le répertoire existe
        if not os.path.exists(directory):
            logger.warning(f"Le répertoire {directory} n'existe pas")
            return pd.DataFrame()
        
        # Trouver tous les fichiers CSV dans le répertoire
        all_files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.csv')]
        
        if not all_files:
            logger.warning(f"Aucun fichier CSV trouvé dans {directory}")
            return pd.DataFrame()
        
        # Trier les fichiers par date de modification (le plus récent en premier)
        all_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
        
        # Charger le fichier le plus récent
        latest_file = all_files[0]
        df = pd.read_csv(latest_file)
        
        logger.info(f"Chargé {len(df)} offres d'emploi depuis {latest_file}")
        return df
    
    except Exception as e:
        logger.error(f"Erreur lors du chargement des offres depuis les fichiers CSV: {e}")
        return pd.DataFrame()

def analyze_contract_types(df):
    """
    Analyse la distribution des types de contrat.
    
    Args:
        df (pandas.DataFrame): DataFrame contenant les offres d'emploi
    
    Returns:
        dict: Statistiques sur les types de contrat
    """
    if df.empty or 'contract_type_std' not in df.columns:
        logger.warning("Données manquantes pour l'analyse des types de contrat")
        return {}
    
    # Compter les types de contrat
    contract_counts = df['contract_type_std'].value_counts()
    
    # Calculer les pourcentages
    contract_percentages = (contract_counts / len(df) * 100).round(2)
    
    # Créer un dictionnaire de résultats
    results = {
        'counts': contract_counts.to_dict(),
        'percentages': contract_percentages.to_dict(),
        'total': len(df)
    }
    
    # Créer un graphique personnalisé
    plt.figure(figsize=(12, 8), facecolor='#121212')
    ax = plt.gca()
    ax.set_facecolor('#1E1E1E')
    
    # Créer des barres horizontales avec des effets spéciaux
    bars = ax.barh(range(len(contract_counts)), contract_counts.values, height=0.7)
    
    # Définir les étiquettes de l'axe y
    ax.set_yticks(range(len(contract_counts)))
    ax.set_yticklabels(contract_counts.index, fontsize=12, fontweight='bold')
    
    # Personnaliser chaque barre avec des couleurs et des effets
    for i, bar in enumerate(bars):
        # Utiliser une couleur différente pour chaque barre
        bar.set_color(custom_palette[i % len(custom_palette)])
        bar.set_edgecolor('white')
        bar.set_linewidth(1.5)
        
        # Ajouter la valeur à la fin de chaque barre
        width = bar.get_width()
        label_y_pos = bar.get_y() + bar.get_height() / 2
        ax.text(width + 1, label_y_pos, f'{width}', va='center', fontweight='bold', color='white')
    
    # Personnaliser le titre et les étiquettes
    plt.title('Distribution des Types de Contrat - Welcome to the Jungle', fontsize=18, fontweight='bold', color='#33FFF5')
    plt.xlabel('Nombre d\'offres', fontsize=14, fontweight='bold', color='#FF5733')
    plt.ylabel('Type de Contrat', fontsize=14, fontweight='bold', color='#FF5733')
    
    # Ajouter une grille stylisée
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.tight_layout()
    
    # Sauvegarder le graphique
    os.makedirs('data/analysis/visualizations/welcome_jungle', exist_ok=True)
    plt.savefig('data/analysis/visualizations/welcome_jungle/contract_types.png')
    plt.close()
    
    return results

def analyze_locations(df):
    """
    Analyse la distribution géographique des offres d'emploi Welcome to the Jungle.
    
    Args:
        df: DataFrame contenant les données des offres d'emploi
        
    Returns:
        dict: Résultats de l'analyse des localisations
    """
    # Vérifier si la colonne location existe
    if 'location' not in df.columns:
        logger.warning("Colonne 'location' non trouvée dans les données")
        return {}
    
    # Remplacer les valeurs manquantes ou "Non spécifié" par "Télétravail / Remote"
    df_loc = df.copy()
    # Éviter l'utilisation de inplace=True pour éviter les avertissements pandas
    df_loc['location'] = df_loc['location'].fillna('Télétravail / Remote')
    df_loc['location'] = df_loc['location'].replace('Non spécifié', 'Télétravail / Remote')
    
    # Compter les occurrences de chaque lieu
    location_counts = df_loc['location'].value_counts()
    
    # Calculer les pourcentages
    location_percentages = (location_counts / len(df) * 100).round(2)
    
    # Prendre les 15 premiers lieux
    top_locations = location_counts.head(15)
    
    # Créer un graphique à barres horizontales personnalisé
    plt.figure(figsize=(14, 12), facecolor='#121212')
    
    # Créer un graphique à barres horizontal avec des couleurs dégradées
    ax = plt.gca()
    bars = ax.barh(range(len(top_locations)), top_locations.sort_values().values, height=0.7)
    
    # Ajouter des étiquettes de texte personnalisées
    ax.set_yticks(range(len(top_locations)))
    ax.set_yticklabels(top_locations.sort_values().index, fontsize=12, fontweight='bold')
    
    # Personnaliser les barres avec des couleurs dégradées et des effets
    for i, bar in enumerate(bars):
        # Utiliser une couleur différente pour chaque barre
        bar.set_color(custom_palette[i % len(custom_palette)])
        bar.set_edgecolor('white')
        bar.set_linewidth(1.5)
        
        # Ajouter la valeur à la fin de chaque barre
        width = bar.get_width()
        label_y_pos = bar.get_y() + bar.get_height() / 2
        ax.text(width + 1, label_y_pos, f'{width}', va='center', fontweight='bold', color='white')
    
    # Ajouter un titre et des étiquettes d'axe personnalisés
    plt.title('Top 15 des Localisations - Welcome to the Jungle', 
              fontsize=20, fontweight='bold', color='#33FFF5', pad=20)
    plt.xlabel('Nombre d\'offres', fontsize=16, fontweight='bold', color='#FF5733', labelpad=15)
    plt.ylabel('Localisation', fontsize=16, fontweight='bold', color='#FF5733', labelpad=15)
    
    # Ajouter une grille stylisée
    plt.grid(True, linestyle='--', alpha=0.3, axis='x')
    
    # Ajouter un fond dégradé pour le graphique
    ax.set_facecolor('#1E1E1E')
    
    # Ajouter une bordure au graphique
    for spine in ax.spines.values():
        spine.set_edgecolor('#444444')
        spine.set_linewidth(2)
    
    plt.tight_layout()
    
    # Créer un dictionnaire de résultats
    results = {
        'top_locations': top_locations.to_dict(),
        'percentages': location_percentages.head(15).to_dict(),
        'unique_locations': df_loc['location'].nunique(),
        'total': len(df)
    }
    
    # Sauvegarder le graphique
    os.makedirs('data/analysis/visualizations/welcome_jungle', exist_ok=True)
    plt.savefig('data/analysis/visualizations/welcome_jungle/top_locations.png')
    plt.close()
    
    return results

def analyze_salaries(df):
    """
    Analyse les salaires proposés dans les offres.
    
    Args:
        df (pandas.DataFrame): DataFrame contenant les offres d'emploi
    
    Returns:
        dict: Statistiques sur les salaires
    """
    if df.empty or 'min_salary' not in df.columns:
        logger.warning("Données manquantes pour l'analyse des salaires")
        return {}
    
    # Filtrer les offres avec des salaires
    salary_df = df[df['min_salary'].notna()].copy()
    
    if len(salary_df) == 0:
        logger.warning("Aucune offre avec des informations de salaire")
        return {}
    
    # Standardiser les salaires en fonction de la période
    # Convertir tous les salaires en salaire annuel
    period_multipliers = {
        'YEARLY': 1,
        'MONTHLY': 12,
        'WEEKLY': 52,
        'DAILY': 220,  # Environ 220 jours travaillés par an
        'HOURLY': 1600  # Environ 1600 heures travaillées par an
    }
    
    # Créer une colonne de salaire annualisé
    salary_df['annual_min_salary'] = salary_df.apply(
        lambda x: x['min_salary'] * period_multipliers.get(x['salary_period'], 1) 
        if pd.notna(x['min_salary']) and pd.notna(x['salary_period']) else np.nan, 
        axis=1
    )
    
    if 'max_salary' in salary_df.columns:
        salary_df['annual_max_salary'] = salary_df.apply(
            lambda x: x['max_salary'] * period_multipliers.get(x['salary_period'], 1) 
            if pd.notna(x['max_salary']) and pd.notna(x['salary_period']) else np.nan, 
            axis=1
        )
    
    # Statistiques de base
    stats = {
        'count': len(salary_df),
        'min': salary_df['annual_min_salary'].min(),
        'max': salary_df['annual_max_salary'].max() if 'annual_max_salary' in salary_df.columns else salary_df['annual_min_salary'].max(),
        'mean': salary_df['annual_min_salary'].mean(),
        'median': salary_df['annual_min_salary'].median(),
        'std': salary_df['annual_min_salary'].std(),
        'percentage_with_salary': len(salary_df) / len(df) * 100
    }
    
    # Analyse par type de contrat
    salary_by_contract = salary_df.groupby('contract_type_std')['annual_min_salary'].agg(['mean', 'median', 'count']).reset_index()
    
    # Créer un graphique de distribution des salaires personnalisé
    plt.figure(figsize=(14, 10), facecolor='#121212')
    ax = plt.gca()
    ax.set_facecolor('#1E1E1E')
    
    # Créer un histogramme avec des couleurs personnalisées et des effets
    n, bins, patches = plt.hist(salary_df['annual_min_salary'].dropna(), bins=30, 
                              alpha=0.7, edgecolor='white', linewidth=1.5)
    
    # Appliquer un dégradé de couleurs aux barres de l'histogramme
    bin_centers = 0.5 * (bins[:-1] + bins[1:])
    col = bin_centers - min(bin_centers)
    col /= max(col)
    
    for c, p in zip(col, patches):
        plt.setp(p, 'facecolor', plt.cm.viridis(c))
        plt.setp(p, 'edgecolor', 'white')
        plt.setp(p, 'linewidth', 1.5)
    
    # Ajouter une courbe KDE personnalisée en utilisant seaborn
    if len(salary_df['annual_min_salary'].dropna()) > 1:  # Vérifier qu'il y a assez de données
        sns.kdeplot(salary_df['annual_min_salary'].dropna(), color='#FF5733', 
                   linewidth=3, label='Distribution', ax=ax)
    
    # Ajouter des lignes verticales pour les statistiques clés
    plt.axvline(salary_df['annual_min_salary'].mean(), color='#33FFF5', 
                linestyle='--', linewidth=2, label=f'Moyenne: {int(salary_df["annual_min_salary"].mean()):,} €')
    plt.axvline(salary_df['annual_min_salary'].median(), color='#FF33A8', 
                linestyle='-.', linewidth=2, label=f'Médiane: {int(salary_df["annual_min_salary"].median()):,} €')
    
    # Ajouter un titre et des étiquettes d'axe personnalisés
    plt.title('Distribution des Salaires Annuels - Welcome to the Jungle', 
              fontsize=20, fontweight='bold', color='#33FFF5', pad=20)
    plt.xlabel('Salaire Annuel (€)', fontsize=16, fontweight='bold', color='#FF5733', labelpad=15)
    plt.ylabel('Nombre d\'offres', fontsize=16, fontweight='bold', color='#FF5733', labelpad=15)
    
    # Personnaliser les étiquettes des axes
    plt.xticks(fontsize=12, color='white')
    plt.yticks(fontsize=12, color='white')
    
    # Ajouter une légende personnalisée
    plt.legend(fontsize=12, frameon=True, facecolor='#1E1E1E', edgecolor='#444444', 
               loc='upper right', framealpha=0.9)
    
    # Ajouter une grille stylisée
    plt.grid(True, linestyle='--', alpha=0.3)
    
    # Ajouter une bordure au graphique
    for spine in ax.spines.values():
        spine.set_edgecolor('#444444')
        spine.set_linewidth(2)
    
    # Formater les étiquettes de l'axe x pour afficher les valeurs en milliers
    ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, loc: f"{int(x):,}"))
    
    plt.tight_layout()
    
    # Sauvegarder le graphique
    os.makedirs('data/analysis/visualizations/welcome_jungle', exist_ok=True)
    plt.savefig('data/analysis/visualizations/welcome_jungle/salary_distribution.png')
    plt.close()
    
    # Créer un graphique de salaire par type de contrat personnalisé
    plt.figure(figsize=(14, 10), facecolor='#121212')
    ax = plt.gca()
    ax.set_facecolor('#1E1E1E')
    
    # Trier les données par salaire moyen décroissant pour une meilleure visualisation
    salary_by_contract_sorted = salary_by_contract.sort_values('mean', ascending=False)
    
    # Créer des barres personnalisées
    bars = ax.bar(salary_by_contract_sorted['contract_type_std'], 
                 salary_by_contract_sorted['mean'], 
                 width=0.7, 
                 edgecolor='white', 
                 linewidth=1.5)
    
    # Personnaliser chaque barre avec des couleurs et des effets
    for i, bar in enumerate(bars):
        # Utiliser un dégradé de couleurs pour les barres
        bar_color = custom_palette[i % len(custom_palette)]
        bar.set_color(bar_color)
        
        # Ajouter la valeur au-dessus de chaque barre
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height + 1000,
                f'{int(height):,} €',
                ha='center', va='bottom', fontsize=12, fontweight='bold', color='white')
    
    # Ajouter des annotations pour le nombre d'offres
    for i, row in salary_by_contract_sorted.iterrows():
        count = row['count']
        x = i
        y = row['mean'] / 2  # Placer l'annotation au milieu de la barre
        ax.text(x, y, f'n={count}', ha='center', va='center', 
                color='black', fontweight='bold', fontsize=10)
    
    # Ajouter un titre et des étiquettes d'axe personnalisés
    plt.title('Salaire Moyen par Type de Contrat - Welcome to the Jungle', 
              fontsize=20, fontweight='bold', color='#33FFF5', pad=20)
    plt.xlabel('Type de Contrat', fontsize=16, fontweight='bold', color='#FF5733', labelpad=15)
    plt.ylabel('Salaire Annuel Moyen (€)', fontsize=16, fontweight='bold', color='#FF5733', labelpad=15)
    
    # Personnaliser les étiquettes des axes
    plt.xticks(rotation=45, fontsize=12, color='white', fontweight='bold')
    plt.yticks(fontsize=12, color='white')
    
    # Ajouter une grille stylisée
    plt.grid(True, linestyle='--', alpha=0.3, axis='y')
    
    # Ajouter une bordure au graphique
    for spine in ax.spines.values():
        spine.set_edgecolor('#444444')
        spine.set_linewidth(2)
    
    # Formater les étiquettes de l'axe y pour afficher les valeurs en milliers
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, loc: f"{int(x):,}"))
    
    plt.tight_layout()
    
    # Sauvegarder le graphique
    plt.savefig('data/analysis/visualizations/welcome_jungle/salary_by_contract.png')
    plt.close()
    
    return {
        'stats': stats,
        'by_contract': salary_by_contract.to_dict('records')
    }

def analyze_technologies(df):
    """
    Analyse les technologies mentionnées dans les offres.
    
    Args:
        df (pandas.DataFrame): DataFrame contenant les offres d'emploi
    
    Returns:
        dict: Statistiques sur les technologies
    """
    tech_columns = [
        'has_python', 'has_java', 'has_javascript', 'has_react', 'has_angular',
        'has_vue', 'has_node', 'has_php', 'has_sql', 'has_nosql',
        'has_mongodb', 'has_postgresql', 'has_mysql', 'has_aws', 'has_azure',
        'has_gcp', 'has_docker', 'has_kubernetes', 'has_devops', 'has_machine_learning',
        'has_data_science', 'has_ai'
    ]
    
    # Vérifier si les colonnes de technologies existent
    existing_tech_columns = [col for col in tech_columns if col in df.columns]
    
    # Si les colonnes de technologies n'existent pas, essayons de les détecter dans la description
    if not existing_tech_columns and 'description' in df.columns:
        logger.info("Détection des technologies dans les descriptions des offres...")
        # Créer des colonnes de technologies basées sur la description
        tech_keywords = {
            'has_python': ['python'],
            'has_java': ['java', 'j2ee'],
            'has_javascript': ['javascript', 'js', 'ecmascript'],
            'has_react': ['react', 'reactjs', 'react.js'],
            'has_angular': ['angular', 'angularjs', 'angular.js'],
            'has_vue': ['vue', 'vuejs', 'vue.js'],
            'has_node': ['node', 'nodejs', 'node.js'],
            'has_php': ['php'],
            'has_sql': ['sql', 'mysql', 'postgresql', 'oracle', 'sqlite'],
            'has_nosql': ['nosql', 'mongodb', 'dynamodb', 'cassandra', 'couchdb'],
            'has_mongodb': ['mongodb', 'mongo'],
            'has_postgresql': ['postgresql', 'postgres'],
            'has_mysql': ['mysql'],
            'has_aws': ['aws', 'amazon web services', 'ec2', 's3', 'lambda'],
            'has_azure': ['azure', 'microsoft azure'],
            'has_gcp': ['gcp', 'google cloud', 'cloud platform'],
            'has_docker': ['docker', 'conteneur', 'container'],
            'has_kubernetes': ['kubernetes', 'k8s'],
            'has_devops': ['devops', 'ci/cd', 'jenkins', 'gitlab ci'],
            'has_machine_learning': ['machine learning', 'ml', 'apprentissage automatique'],
            'has_data_science': ['data science', 'science des données', 'datascience'],
            'has_ai': ['ai', 'artificial intelligence', 'intelligence artificielle']
        }
        
        # Créer les colonnes de technologies
        for tech_col, keywords in tech_keywords.items():
            df[tech_col] = df['description'].str.lower().apply(
                lambda x: any(keyword in str(x).lower() for keyword in keywords) if pd.notna(x) else False
            )
        
        existing_tech_columns = list(tech_keywords.keys())
    
    if df.empty or not existing_tech_columns:
        logger.warning("Données manquantes pour l'analyse des technologies")
        return {}
    
    # Compter les occurrences de chaque technologie
    tech_counts = {}
    for col in existing_tech_columns:
        tech_name = col.replace('has_', '')
        tech_counts[tech_name] = df[col].sum()
    
    # Trier par nombre d'occurrences
    tech_counts = {k: v for k, v in sorted(tech_counts.items(), key=lambda item: item[1], reverse=True)}
    
    # Créer un graphique personnalisé avec un style futuriste
    plt.figure(figsize=(16, 12), facecolor='#121212')
    ax = plt.gca()
    ax.set_facecolor('#1E1E1E')
    
    # Préparer les données
    tech_df = pd.DataFrame(list(tech_counts.items()), columns=['Technology', 'Count'])
    tech_df = tech_df.sort_values('Count', ascending=True)  # Tri ascendant pour afficher les plus demandées en haut
    
    # Limiter aux 15 technologies les plus demandées pour une meilleure lisibilité
    if len(tech_df) > 15:
        tech_df = tech_df.tail(15)
    
    # Créer des barres horizontales avec des effets spéciaux
    bars = ax.barh(tech_df['Technology'], tech_df['Count'], height=0.7)
    
    # Personnaliser chaque barre avec des couleurs et des effets
    for i, bar in enumerate(bars):
        # Utiliser un dégradé de couleurs pour les barres
        bar_color = plt.cm.plasma(i / len(bars))
        bar.set_color(bar_color)
        bar.set_edgecolor('white')
        bar.set_linewidth(1.5)
        
        # Ajouter la valeur à la fin de chaque barre
        width = bar.get_width()
        label_y_pos = bar.get_y() + bar.get_height() / 2
        ax.text(width + 0.5, label_y_pos, f'{int(width)}', va='center', fontweight='bold', color='white', fontsize=12)
    
    # Ajouter un titre et des étiquettes d'axe personnalisés avec des polices et couleurs spéciales
    plt.title('Technologies les Plus Demandées - Welcome to the Jungle', 
              fontsize=22, fontweight='bold', color='#33FFF5', pad=20)
    plt.xlabel('Nombre d\'offres', fontsize=16, fontweight='bold', color='#FF5733', labelpad=15)
    plt.ylabel('Technologie', fontsize=16, fontweight='bold', color='#FF5733', labelpad=15)
    
    # Personnaliser les étiquettes des axes
    plt.xticks(fontsize=12, color='white')
    plt.yticks(fontsize=14, color='white', fontweight='bold')
    
    # Ajouter une grille stylisée uniquement pour l'axe x
    plt.grid(True, axis='x', linestyle='--', alpha=0.3, color='#666666')
    
    # Ajouter une bordure au graphique
    for spine in ax.spines.values():
        spine.set_edgecolor('#444444')
        spine.set_linewidth(2)
    
    # Ajouter un fond dégradé pour plus d'effet visuel
    from matplotlib.colors import LinearSegmentedColormap
    cmap = LinearSegmentedColormap.from_list('custom', ['#1E1E1E', '#0A0A0A'], N=100)
    ax.imshow([[0, 0], [1, 1]], 
              extent=[ax.get_xlim()[0], ax.get_xlim()[1]*1.2, 
                      ax.get_ylim()[0], ax.get_ylim()[1]], 
              cmap=cmap, aspect='auto', alpha=0.3, zorder=0)
    
    # Ajouter une annotation explicative
    total_offers = df.shape[0]
    ax.text(0.98, 0.02, 
            f'Analyse basée sur {total_offers} offres d\'emploi',
            transform=ax.transAxes, fontsize=10, color='#BBBBBB',
            ha='right', va='bottom', alpha=0.7)
    
    plt.tight_layout()
    
    # Sauvegarder le graphique
    os.makedirs('data/analysis/visualizations/welcome_jungle', exist_ok=True)
    plt.savefig('data/analysis/visualizations/welcome_jungle/technologies.png')
    plt.close()
    
    # Créer un nuage de mots personnalisé avec un style futuriste
    if sum(tech_counts.values()) > 0:  # Vérifier qu'il y a des technologies mentionnées
        # Créer une forme personnalisée pour le nuage de mots (cercle)
        x, y = np.ogrid[:300, :300]
        mask = (x - 150) ** 2 + (y - 150) ** 2 > 130 ** 2
        mask = 255 * mask.astype(int)
        
        # Créer un nuage de mots avec des paramètres personnalisés
        wordcloud = WordCloud(
            width=1000, 
            height=1000, 
            background_color='#121212',
            colormap='plasma',  # Utiliser une colormap vibrante
            mask=mask,
            contour_width=2,
            contour_color='#33FFF5',
            min_font_size=10,
            max_font_size=80,
            random_state=42,  # Pour la reproductibilité
            prefer_horizontal=0.7,  # Permettre plus de mots verticaux
            font_path=None  # Utiliser la police par défaut
        ).generate_from_frequencies(tech_counts)
        
        # Créer la figure avec un fond noir
        plt.figure(figsize=(12, 12), facecolor='#121212')
        plt.imshow(wordcloud, interpolation='bilinear')
        plt.axis('off')
        
        # Ajouter un titre élégant
        plt.title('Technologies les Plus Demandées', 
                  fontsize=24, fontweight='bold', color='#33FFF5', pad=20)
        
        plt.tight_layout()
        
        # Sauvegarder le nuage de mots
        plt.savefig('data/analysis/visualizations/welcome_jungle/technologies_wordcloud.png', 
                    facecolor='#121212', bbox_inches='tight')
        plt.close()
    
    return {
        'counts': tech_counts,
        'total_mentions': sum(tech_counts.values()),
        'percentage_with_tech': (df[existing_tech_columns].any(axis=1).sum() / len(df) * 100).round(2)
    }

def analyze_experience(df):
    """
    Analyse les niveaux d'expérience demandés dans les offres.
    
    Args:
        df (pandas.DataFrame): DataFrame contenant les offres d'emploi
    
    Returns:
        dict: Statistiques sur les niveaux d'expérience
    """
    if df.empty or 'experience_level' not in df.columns:
        logger.warning("Données manquantes pour l'analyse des niveaux d'expérience")
        return {}
    
    # Remplacer les valeurs manquantes ou NOT_SPECIFIED par "Tous niveaux d'expérience"
    df_exp = df.copy()
    df_exp['experience_level'].fillna('Tous niveaux d\'expérience', inplace=True)
    df_exp['experience_level'] = df_exp['experience_level'].replace(['NOT_SPECIFIED', 'Non précisé'], 'Tous niveaux d\'expérience')
    
    # Compter les niveaux d'expérience
    experience_counts = df_exp['experience_level'].value_counts()
    
    # Calculer les pourcentages
    experience_percentages = (experience_counts / len(df) * 100).round(2)
    
    # Créer un dictionnaire de résultats
    results = {
        'counts': experience_counts.to_dict(),
        'percentages': experience_percentages.to_dict(),
        'total': len(df)
    }
    
    # Créer un graphique personnalisé pour les niveaux d'expérience
    plt.figure(figsize=(14, 10), facecolor='#121212')
    ax = plt.gca()
    ax.set_facecolor('#1E1E1E')
    
    # Préparer les données
    exp_data = df_exp['experience_level'].value_counts().sort_values(ascending=True)
    
    # Créer des barres horizontales avec des effets spéciaux
    bars = ax.barh(exp_data.index, exp_data.values, height=0.6)
    
    # Personnaliser chaque barre avec des couleurs et des effets
    for i, bar in enumerate(bars):
        # Utiliser un dégradé de couleurs pour les barres
        bar_color = custom_palette[i % len(custom_palette)]
        bar.set_color(bar_color)
        bar.set_edgecolor('white')
        bar.set_linewidth(1.5)
        
        # Ajouter la valeur et le pourcentage à la fin de chaque barre
        width = bar.get_width()
        percentage = (width / len(df_exp) * 100).round(1)
        label_y_pos = bar.get_y() + bar.get_height() / 2
        ax.text(width + 2, label_y_pos, f'{width} ({percentage}%)', 
                va='center', fontweight='bold', color='white', fontsize=10)
    
    # Ajouter un titre et des étiquettes d'axe personnalisés
    plt.title('Distribution des Niveaux d\'Expérience - Welcome to the Jungle', 
              fontsize=20, fontweight='bold', color='#33FFF5', pad=20)
    plt.xlabel('Nombre d\'offres', fontsize=16, fontweight='bold', color='#FF5733', labelpad=15)
    plt.ylabel('Niveau d\'Expérience', fontsize=16, fontweight='bold', color='#FF5733', labelpad=15)
    
    # Personnaliser les étiquettes des axes
    plt.xticks(fontsize=12, color='white')
    plt.yticks(fontsize=12, color='white', fontweight='bold')
    
    # Ajouter une grille stylisée
    plt.grid(True, linestyle='--', alpha=0.3, axis='x')
    
    # Ajouter une bordure au graphique
    for spine in ax.spines.values():
        spine.set_edgecolor('#444444')
        spine.set_linewidth(2)
    
    plt.tight_layout()
    
    # Sauvegarder le graphique
    os.makedirs('data/analysis/visualizations/welcome_jungle', exist_ok=True)
    plt.savefig('data/analysis/visualizations/welcome_jungle/experience_levels.png')
    plt.close()
    
    # Analyse des années d'expérience minimales et maximales
    if 'min_experience' in df.columns and 'max_experience' in df.columns:
        exp_df = df[(df['min_experience'].notna()) | (df['max_experience'].notna())].copy()
        
        if len(exp_df) > 0:
            # Statistiques sur les années d'expérience
            exp_stats = {
                'min_experience_avg': exp_df['min_experience'].mean(),
                'max_experience_avg': exp_df['max_experience'].mean(),
                'min_experience_median': exp_df['min_experience'].median(),
                'max_experience_median': exp_df['max_experience'].median(),
                'count_with_experience': len(exp_df),
                'percentage_with_experience': len(exp_df) / len(df) * 100
            }
            
            # Ajouter les statistiques d'expérience aux résultats
            results['experience_years'] = exp_stats
    
    return results

def generate_html_report(results):
    """
    Génère un rapport HTML à partir des résultats d'analyse.
    
    Args:
        results (dict): Résultats de l'analyse
    
    Returns:
        str: Chemin vers le fichier HTML généré
    """
    html_content = f"""
    <!DOCTYPE html>
    <html lang="fr">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Analyse des Offres d'Emploi - Welcome to the Jungle</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; color: #333; }}
            h1 {{ color: #2c3e50; text-align: center; }}
            h2 {{ color: #3498db; margin-top: 30px; }}
            .container {{ max-width: 1200px; margin: 0 auto; }}
            .stats-container {{ display: flex; flex-wrap: wrap; justify-content: space-between; margin-bottom: 20px; }}
            .stat-box {{ background-color: #f9f9f9; border-radius: 5px; padding: 15px; margin-bottom: 15px; flex: 1; min-width: 200px; margin-right: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
            .stat-box h3 {{ margin-top: 0; color: #2980b9; }}
            .chart-container {{ margin: 20px 0; text-align: center; }}
            .chart-container img {{ max-width: 100%; height: auto; border-radius: 5px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
            table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
            th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }}
            th {{ background-color: #3498db; color: white; }}
            tr:hover {{ background-color: #f5f5f5; }}
            .footer {{ text-align: center; margin-top: 50px; color: #7f8c8d; font-size: 0.9em; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Analyse des Offres d'Emploi - Welcome to the Jungle</h1>
            
            <div class="stats-container">
                <div class="stat-box">
                    <h3>Statistiques Générales</h3>
                    <p>Nombre total d'offres: <strong>{results['metadata']['total_jobs']}</strong></p>
                    <p>Date de génération: <strong>{results['metadata']['generated_at']}</strong></p>
                </div>
            </div>
            
            <h2>Types de Contrat</h2>
            <div class="chart-container">
                <img src="./contract_types.png" alt="Distribution des Types de Contrat">
            </div>
            
            <h2>Localisations</h2>
            <div class="chart-container">
                <img src="./top_locations.png" alt="Top 15 des Localisations">
            </div>
            
            <h2>Technologies</h2>
            <div class="chart-container">
                <img src="./technologies.png" alt="Technologies les Plus Demandées">
                <img src="./technologies_wordcloud.png" alt="Nuage de Mots des Technologies">
            </div>
            
            <h2>Salaires</h2>
            <div class="chart-container">
                <img src="./salary_distribution.png" alt="Distribution des Salaires">
                <img src="./salary_by_contract.png" alt="Salaire Moyen par Type de Contrat">
            </div>
            
            <h2>Expérience</h2>
            <div class="chart-container">
                <img src="./experience_levels.png" alt="Distribution des Niveaux d'Expérience">
            </div>
            
            <div class="footer">
                <p>Rapport généré le {datetime.now().strftime('%d/%m/%Y à %H:%M:%S')}</p>
            </div>
        </div>
    </body>
    </html>
    """
    
    # Créer le répertoire de rapports
    os.makedirs('reports', exist_ok=True)
    
    # Sauvegarder le rapport HTML
    html_path = 'data/analysis/visualizations/welcome_jungle/dashboard.html'
    with open(html_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    logger.info(f"Rapport HTML généré avec succès: {html_path}")
    return html_path

def generate_welcome_jungle_report(df=None):
    """
    Génère un rapport complet d'analyse des offres d'emploi Welcome to the Jungle.
    
    Args:
        df (pandas.DataFrame, optional): DataFrame contenant les offres d'emploi
    
    Returns:
        dict: Résultats de l'analyse
    """
    # Charger les données si non fournies
    if df is None:
        # Essayer d'abord la base de données
        df = load_welcome_jungle_data_from_db()
        
        # Si la base de données échoue, essayer les fichiers CSV locaux
        if df.empty:
            df = load_welcome_jungle_data_from_csv()
    
    if df.empty:
        logger.error("Impossible de charger les données d'offres d'emploi Welcome to the Jungle")
        return {}
    
    # Créer le répertoire de rapports
    os.makedirs('data/analysis/visualizations/welcome_jungle', exist_ok=True)
    
    # Effectuer les analyses
    logger.info("Génération du rapport d'analyse Welcome to the Jungle...")
    
    results = {
        'metadata': {
            'total_jobs': len(df),
            'generated_at': datetime.now().isoformat(),
            'date_range': {
                'min': df['date'].min() if 'date' in df.columns else None,
                'max': df['date'].max() if 'date' in df.columns else None
            }
        },
        'contract_types': analyze_contract_types(df),
        'locations': analyze_locations(df),
        'salaries': analyze_salaries(df),
        'technologies': analyze_technologies(df),
        'experience': analyze_experience(df)
    }
    
    # Sauvegarder les résultats
    with open('data/analysis/visualizations/welcome_jungle/analysis_report.json', 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=4, cls=DateTimeEncoder)
    
    # Générer le rapport HTML
    html_path = generate_html_report(results)
    
    logger.info("Rapport d'analyse Welcome to the Jungle généré avec succès:")
    logger.info(f"- JSON: data/analysis/visualizations/welcome_jungle/analysis_report.json")
    logger.info(f"- HTML: {html_path}")
    logger.info("- Graphiques: data/analysis/visualizations/welcome_jungle/")
    
    return results

if __name__ == "__main__":
    # Générer le rapport
    generate_welcome_jungle_report()
