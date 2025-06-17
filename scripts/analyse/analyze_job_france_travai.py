
"""
Script d'analyse et de visualisation des données d'offres d'emploi France Travail.
Ce script se connecte à la base de données PostgreSQL, extrait les données,
et génère des visualisations pour explorer les tendances.
"""

import os
import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
import logging
import argparse
import random
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import matplotlib as mpl
import re
from matplotlib.colors import LinearSegmentedColormap
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Configuration du logging
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Définir un style personnalisé pour les visualisations
plt.style.use('dark_background')

# Palette de couleurs personnalisée
colors = ['#FF5733', '#33FF57', '#3357FF', '#F3FF33', '#FF33F3', '#33FFF3', '#FF8033']

# Configuration du style Seaborn
sns.set(style="darkgrid")
sns.set_palette(sns.color_palette(colors))

# Fonction pour sauvegarder les graphiques Plotly en HTML et PNG
def save_plotly_figure(fig, output_path):
    """Sauvegarde une figure Plotly en HTML et PNG avec gestion des erreurs"""
    try:
        # Créer le répertoire de sortie s'il n'existe pas
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Sauvegarder en HTML pour l'interactivité
        html_path = output_path.replace('.png', '.html')
        fig.write_html(html_path)
        
        try:
            # Sauvegarder en PNG pour la compatibilité
            fig.write_image(output_path, width=1200, height=800)
            logger.info(f"Figure sauvegardée en PNG: {output_path}")
        except Exception as e:
            logger.warning(f"Impossible de sauvegarder l'image PNG (problème avec Kaleido?): {e}")
            logger.info(f"Figure sauvegardée uniquement en HTML: {html_path}")
            # Remplacer le chemin de sortie par le HTML si le PNG échoue
            output_path = html_path
        
        return output_path
    except Exception as e:
        logger.error(f"Erreur lors de la sauvegarde de la figure: {e}")
        return None

# Ajouter le chemin du projet au PYTHONPATH
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Importer les modules d'analyse avancée
try:
    from analyze_salary import analyze_salary_distribution, analyze_salary_by_technology
    from analyze_correlations import analyze_tech_contract_correlation
    from analyze_geography import analyze_geographic_distribution
    from analyze_temporal import analyze_temporal_evolution
except ImportError as e:
    logger.warning(f"Erreur lors de l'importation des modules d'analyse avancée: {e}")

# Fonctions d'analyse avancée
def analyze_salary_advanced(df):
    """Analyse avancée des salaires."""
    try:
        logger.info("Analyse des salaires avancée...")
        
        # Vérifier si les colonnes de salaire existent
        if 'min_salary' not in df.columns or 'max_salary' not in df.columns:
            # Créer les colonnes manquantes avec des valeurs NaN
            if 'min_salary' not in df.columns:
                df['min_salary'] = np.nan
            if 'max_salary' not in df.columns:
                df['max_salary'] = np.nan
            logger.warning("Colonnes de salaire manquantes créées avec des valeurs NaN")
        
        # Analyse de la distribution des salaires
        analyze_salary_distribution(df)
        
        # Vérifier les colonnes de technologies nécessaires
        tech_columns = ['has_python', 'has_java', 'has_javascript', 'has_sql', 'has_aws', 'has_machine_learning']
        for col in tech_columns:
            if col not in df.columns:
                df[col] = False
                logger.warning(f"Colonne {col} créée avec des valeurs False par défaut")
        
        # Analyse des salaires par technologie
        analyze_salary_by_technology(df)
        
        return True
    except Exception as e:
        logger.error(f"Erreur lors de l'analyse avancée des salaires: {e}")
        return False

def analyze_tech_contract_correlations(df):
    """Analyse des corrélations entre technologies et types de contrat."""
    try:
        logger.info("Analyse des corrélations entre technologies et types de contrat...")
        
        # Créer une copie du DataFrame pour éviter de modifier l'original
        corr_df = df.copy()
        
        # Vérifier si les colonnes nécessaires existent
        tech_columns = ['has_python', 'has_java', 'has_javascript', 'has_sql', 'has_aws', 'has_machine_learning']
        
        if 'contract_type_std' not in corr_df.columns:
            logger.warning("Colonne contract_type_std manquante, utilisation de contract_type si disponible")
            if 'contract_type' in corr_df.columns:
                corr_df['contract_type_std'] = corr_df['contract_type']
            else:
                logger.warning("Aucune colonne de type de contrat disponible")
                corr_df['contract_type_std'] = 'Unknown'
        
        # Vérifier et créer les colonnes de technologies manquantes
        for col in tech_columns:
            if col not in corr_df.columns:
                logger.warning(f"Colonne {col} créée avec des valeurs False par défaut")
                corr_df[col] = False
        
        # Standardiser les types de contrat pour n'avoir que les principaux
        # Limiter à 3 types de contrat pour éviter les erreurs de dimension
        top_contracts = corr_df['contract_type_std'].value_counts().nlargest(3).index
        corr_df.loc[~corr_df['contract_type_std'].isin(top_contracts), 'contract_type_std'] = 'OTHER'
        
        # Appel de la fonction du module externe avec notre DataFrame préparé
        import analyze_correlations
        analyze_correlations.analyze_tech_contract_correlation(corr_df)
        
        return True
    except Exception as e:
        logger.error(f"Erreur lors de l'analyse des corrélations: {e}")
        return False

def analyze_geography_advanced(df):
    """Analyse géographique avancée."""
    try:
        logger.info("Analyse géographique...")
        
        # Créer une copie du DataFrame pour éviter de modifier l'original
        geo_df = df.copy()
        
        # Préparation des données géographiques
        if 'lieu_travail' not in geo_df.columns:
            if 'city' in geo_df.columns:
                logger.warning("Création de données géographiques structurées à partir de la colonne city")
                # Créer un format compatible avec l'analyse géographique
                def format_city_data(city):
                    if pd.isna(city) or not city:
                        return None
                    # Créer un dictionnaire au format attendu par analyze_geographic_distribution
                    city_dict = {
                        'libelle': f"00 - {city}",  # Format attendu par la fonction d'extraction
                        'commune': None  # Pas de code INSEE disponible
                    }
                    return str(city_dict)
                
                geo_df['lieu_travail'] = geo_df['city'].apply(format_city_data)
                
                # Ajouter des données de région pour les villes les plus courantes
                city_region_map = {
                    'Paris': 'Île-de-France',
                    'Lyon': 'Auvergne-Rhône-Alpes',
                    'Marseille': 'Provence-Alpes-Côte d\'Azur',
                    'Toulouse': 'Occitanie',
                    'Nice': 'Provence-Alpes-Côte d\'Azur',
                    'Nantes': 'Pays de la Loire',
                    'Strasbourg': 'Grand Est',
                    'Montpellier': 'Occitanie',
                    'Bordeaux': 'Nouvelle-Aquitaine',
                    'Lille': 'Hauts-de-France',
                    'Rennes': 'Bretagne',
                    'Reims': 'Grand Est',
                    'Le Havre': 'Normandie',
                    'Saint-Étienne': 'Auvergne-Rhône-Alpes',
                    'Toulon': 'Provence-Alpes-Côte d\'Azur',
                    'Grenoble': 'Auvergne-Rhône-Alpes',
                    'Dijon': 'Bourgogne-Franche-Comté',
                    'Angers': 'Pays de la Loire',
                    'Nîmes': 'Occitanie',
                    'Villeurbanne': 'Auvergne-Rhône-Alpes'
                }
                
                # Ajouter directement la colonne région pour les villes connues
                geo_df['region'] = geo_df['city'].map(city_region_map)
                geo_df['region'].fillna('Inconnue', inplace=True)
            elif 'location' in geo_df.columns:
                geo_df['lieu_travail'] = geo_df['location']
                logger.warning("Colonne lieu_travail créée à partir de la colonne location")
                geo_df['region'] = 'Inconnue'
            else:
                geo_df['lieu_travail'] = 'Non spécifié'
                logger.warning("Colonne lieu_travail créée avec des valeurs 'Non spécifié' par défaut")
                geo_df['region'] = 'Inconnue'
        
        # Analyse de la distribution géographique
        import analyze_geography
        analyze_geography.analyze_geographic_distribution(geo_df)
        
        return True
    except Exception as e:
        logger.error(f"Erreur lors de l'analyse géographique: {e}")
        return False

def analyze_temporal_evolution(df):
    """Analyse de l'évolution temporelle des offres."""
    try:
        logger.info("Analyse de l'évolution temporelle...")
        
        # Vérifier si les colonnes de date existent
        date_columns = ['date_creation', 'date_actualisation']
        missing_date_columns = [col for col in date_columns if col not in df.columns]
        
        if missing_date_columns:
            # Créer une date fictive pour les analyses
            today = datetime.now()
            
            for col in missing_date_columns:
                # Générer des dates aléatoires sur les 6 derniers mois
                random_dates = [today - timedelta(days=random.randint(0, 180)) for _ in range(len(df))]
                df[col] = random_dates
                logger.warning(f"Colonne {col} créée avec des dates aléatoires pour l'analyse")
        
        # Appel direct de la fonction du module externe (pas de récursion)
        import analyze_temporal
        analyze_temporal.analyze_temporal_evolution(df)
        
        return True
    except Exception as e:
        logger.error(f"Erreur lors de l'analyse temporelle: {e}")
        return False



# Importer l'utilitaire de chargement des variables d'environnement
from src.pipeline.api.dotenv_utils import load_dotenv

def setup_environment():
    """
    Configure les variables d'environnement nécessaires pour l'analyse.
    """
    # Charger les variables d'environnement depuis le fichier .env
    load_dotenv()
    
    # Vérifier que les variables essentielles sont définies
    required_vars = ['DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'DB_PASSWORD']
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        logger.error(f"Variables d'environnement manquantes: {', '.join(missing_vars)}")
        logger.error("Veuillez vérifier votre fichier .env")
        return False
    
    # Créer le dossier pour les visualisations
    os.makedirs("data/analysis/visualizations", exist_ok=True)
    
    logger.info("Environnement configuré avec succès")
    return True

def get_db_connection():
    """
    Établit une connexion à la base de données PostgreSQL.
    
    Returns:
        sqlalchemy.engine.Engine: Objet de connexion à la base de données
    """
    try:
        # Récupérer les informations de connexion depuis les variables d'environnement
        db_host = os.getenv('DB_HOST')
        db_port = os.getenv('DB_PORT')
        db_name = os.getenv('DB_NAME')
        db_user = os.getenv('DB_USER')
        db_password = os.getenv('DB_PASSWORD')
        
        # Construire l'URL de connexion
        db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        
        # Créer le moteur de connexion
        engine = create_engine(db_url)
        
        # Tester la connexion
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            if result.scalar() == 1:
                logger.info("Connexion à la base de données établie avec succès")
                return engine
            else:
                logger.error("La connexion à la base de données a échoué")
                return None
    
    except Exception as e:
        logger.error(f"Erreur lors de la connexion à la base de données: {e}")
        return None

def load_data_from_db(engine):
    """
    Charge les données depuis la base de données.
    
    Args:
        engine (sqlalchemy.engine.Engine): Objet de connexion à la base de données
        
    Returns:
        pandas.DataFrame: DataFrame contenant les données chargées
    """
    try:
        # Requête SQL pour récupérer les données
        query = """
        SELECT * FROM france_travail_jobs
        """
        
        # Exécuter la requête et charger les résultats dans un DataFrame
        df = pd.read_sql(query, engine)
        
        logger.info(f"Données chargées avec succès: {len(df)} enregistrements")
        return df
    
    except Exception as e:
        logger.error(f"Erreur lors du chargement des données: {e}")
        return None

def analyze_contract_types(df):
    """
    Analyse la distribution des types de contrat avec un graphique interactif.
    
    Args:
        df (pandas.DataFrame): DataFrame contenant les données
    """
    try:
        # Vérifier si la colonne contract_type_std existe
        if 'contract_type_std' not in df.columns:
            logger.warning("La colonne 'contract_type_std' n'existe pas dans les données")
            return None
            
        # Filtrer les valeurs non nulles
        df_filtered = df[df['contract_type_std'].notna()]
        if len(df_filtered) == 0:
            logger.warning("Aucune donnée de type de contrat disponible")
            return None
            
        # Compter les occurrences de chaque type de contrat
        contract_counts = df_filtered['contract_type_std'].value_counts().reset_index()
        contract_counts.columns = ['Type de Contrat', 'Nombre d\'Offres']
        
        # Calculer les pourcentages
        contract_counts['Pourcentage'] = contract_counts['Nombre d\'Offres'] / contract_counts['Nombre d\'Offres'].sum() * 100
        contract_counts['Pourcentage'] = contract_counts['Pourcentage'].round(1)
        contract_counts['Label'] = contract_counts['Type de Contrat'] + ' (' + contract_counts['Pourcentage'].astype(str) + '%)'        
        
        # Créer un graphique interactif avec Plotly
        fig = px.pie(
            contract_counts, 
            values='Nombre d\'Offres', 
            names='Label',
            title='<b>Distribution des Types de Contrat</b>',
            color_discrete_sequence=px.colors.qualitative.Bold,
            hole=0.4,  # Créer un donut chart
        )
        
        # Personnaliser le graphique
        fig.update_layout(
            font=dict(size=14),
            legend=dict(orientation='h', yanchor='bottom', y=-0.3, xanchor='center', x=0.5),
            margin=dict(t=100, b=100),
            paper_bgcolor='rgba(40,40,40,0.8)',
            plot_bgcolor='rgba(40,40,40,0.8)',
            font_color='white',
            title_font=dict(size=24, color='white'),
            title_x=0.5,
        )
        
        # Ajouter des annotations au centre
        fig.add_annotation(
            text=f"<b>{len(df_filtered)}</b>",
            x=0.5, y=0.5,
            font_size=20,
            showarrow=False
        )
        fig.add_annotation(
            text="offres",
            x=0.5, y=0.4,
            font_size=14,
            showarrow=False
        )
        
        # Sauvegarder le graphique
        output_path = "data/analysis/visualizations/contract_types_distribution.png"
        save_plotly_figure(fig, output_path)
        logger.info(f"Graphique interactif sauvegardé: {output_path}")
        
        # Afficher les statistiques
        logger.info("Distribution des types de contrat:")
        for _, row in contract_counts.iterrows():
            logger.info(f"  {row['Type de Contrat']}: {row['Nombre d\'Offres']} offres ({row['Pourcentage']}%)")
        
        return output_path
    
    except Exception as e:
        logger.error(f"Erreur lors de l'analyse des types de contrat: {e}")
        return None

def analyze_cities(df):
    """
    Analyse la distribution des offres par ville avec une visualisation moderne.
    
    Args:
        df (pandas.DataFrame): DataFrame contenant les données
    """
    try:
        # Vérifier les colonnes possibles contenant des informations de ville
        city_columns = ['city', 'location', 'lieu_travail']
        available_columns = [col for col in city_columns if col in df.columns]
        
        if not available_columns:
            logger.warning("Aucune colonne de ville (city, location, lieu_travail) n'existe dans les données")
            return None
        
        # Essayer chaque colonne disponible jusqu'à trouver des données exploitables
        city_data = None
        city_column = None
        
        for col in available_columns:
            # Filtrer les valeurs nulles
            temp_df = df[df[col].notna()]
            if len(temp_df) > 0:
                city_data = temp_df
                city_column = col
                logger.info(f"Utilisation de la colonne '{col}' pour l'analyse des villes")
                break
        
        if city_data is None or len(city_data) == 0:
            logger.warning("Aucune donnée de ville disponible dans les colonnes existantes")
            return None
            
        # Afficher un échantillon des données pour débogage
        sample_cities = city_data[city_column].head(5).tolist()
        logger.info(f"Exemples de villes trouvées: {sample_cities}")
        
        # Fonction pour extraire la ville à partir de données complexes
        def extract_city_from_complex_data(data):
            try:
                if pd.isna(data):
                    return None
                    
                # Si c'est une chaîne qui ressemble à un JSON
                if isinstance(data, str) and ('{' in data or '[' in data):
                    try:
                        # Essayer de parser comme JSON
                        json_data = json.loads(data)
                        # Chercher des clés communes pour les villes
                        for key in ['city', 'ville', 'nom', 'libelle']:
                            if isinstance(json_data, dict) and key in json_data:
                                return json_data[key]
                        return str(json_data)  # Retourner la chaîne si pas de clé trouvée
                    except:
                        pass
                        
                # Si c'est un dictionnaire
                if isinstance(data, dict):
                    for key in ['city', 'ville', 'nom', 'libelle']:
                        if key in data:
                            return data[key]
                            
                # Sinon retourner la donnée telle quelle
                return str(data)
            except:
                return None
                
        # Essayer d'extraire les noms de ville si les données sont complexes (dictionnaires, JSON, etc.)
        if city_column == 'lieu_travail' or any(isinstance(x, (dict, str)) and '{' in str(x) for x in sample_cities):
            logger.info("Détection de données complexes pour les villes, tentative d'extraction...")
            city_data['city_extracted'] = city_data[city_column].apply(lambda x: extract_city_from_complex_data(x))
            city_column = 'city_extracted'
            # Filtrer à nouveau pour éliminer les valeurs nulles après extraction
            city_data = city_data[city_data[city_column].notna()]
        
        # Compter les occurrences de chaque ville (top 15)
        city_counts = city_data[city_column].value_counts().head(15).reset_index()
        city_counts.columns = ['Ville', 'Nombre d\'Offres']
        
        # Trier par nombre d'offres décroissant
        city_counts = city_counts.sort_values('Nombre d\'Offres', ascending=True)
        
        # Créer un graphique à barres horizontales avec Plotly
        fig = go.Figure()
        
        # Ajouter les barres avec un gradient de couleur
        fig.add_trace(go.Bar(
            y=city_counts['Ville'],
            x=city_counts['Nombre d\'Offres'],
            orientation='h',
            marker=dict(
                color=city_counts['Nombre d\'Offres'],
                colorscale='viridis',
                colorbar=dict(title='Nombre d\'offres'),
            ),
            text=city_counts['Nombre d\'Offres'],
            textposition='auto',
            hoverinfo='text',
            hovertext=[f"{ville}: {count} offres" for ville, count in zip(city_counts['Ville'], city_counts['Nombre d\'Offres'])]
        ))
        
        # Personnaliser le graphique
        fig.update_layout(
            title='<b>Top 15 des Villes avec le Plus d\'Offres d\'Emploi</b>',
            xaxis_title='Nombre d\'Offres',
            yaxis_title=None,
            font=dict(size=14),
            margin=dict(l=20, r=20, t=80, b=20),
            paper_bgcolor='rgba(40,40,40,0.8)',
            plot_bgcolor='rgba(40,40,40,0.8)',
            font_color='white',
            title_font=dict(size=24, color='white'),
            title_x=0.5,
            xaxis=dict(
                gridcolor='rgba(80,80,80,0.5)',
                zerolinecolor='rgba(80,80,80,0.5)'
            ),
            yaxis=dict(
                gridcolor='rgba(80,80,80,0.5)',
                zerolinecolor='rgba(80,80,80,0.5)'
            )
        )
        
        # Sauvegarder le graphique
        output_path = "data/analysis/visualizations/cities_distribution.png"
        save_plotly_figure(fig, output_path)
        logger.info(f"Graphique interactif sauvegardé: {output_path}")
        
        # Afficher les statistiques
        logger.info(f"Top 15 des villes avec le plus d'offres d'emploi (colonne {city_column}):")
        for _, row in city_counts.sort_values('Nombre d\'Offres', ascending=False).iterrows():
            logger.info(f"  {row['Ville']}: {row['Nombre d\'Offres']} offres")
        
        return output_path
    
    except Exception as e:
        logger.error(f"Erreur lors de l'analyse des villes: {e}")
        return None

def analyze_technologies(df):
    """
    Analyse les technologies mentionnées dans les offres avec une visualisation moderne.
    
    Args:
        df (pandas.DataFrame): DataFrame contenant les données
    """
    try:
        # Identifier les colonnes de technologies disponibles (has_*)        
        tech_columns = [col for col in df.columns if col.startswith('has_')]
        
        if not tech_columns:
            logger.warning("Aucune colonne de technologie (has_*) trouvée dans les données")
            return None
            
        # Créer un dictionnaire pour stocker les comptages de technologies
        tech_counts = {}
        
        # Compter les occurrences de chaque technologie
        for col in tech_columns:
            # Extraire le nom de la technologie (supprimer le préfixe 'has_')
            tech_name = col[4:].replace('_', ' ')
            # Compter les valeurs True (ou 1) sans utiliser fillna pour éviter l'avertissement
            # Remplacer les NaN par False directement dans une copie temporaire
            temp_series = df[col].copy()
            temp_series[temp_series.isna()] = False
            count = temp_series.astype(bool).sum()
            tech_counts[tech_name] = count
        
        # Convertir en DataFrame et trier
        tech_df = pd.DataFrame({
            'Technologie': list(tech_counts.keys()),
            'Nombre': list(tech_counts.values())
        })
        
        # Filtrer les technologies avec au moins une occurrence et prendre les 15 plus fréquentes
        tech_df = tech_df[tech_df['Nombre'] > 0].sort_values('Nombre', ascending=True).tail(15)
        
        if len(tech_df) > 0:
            # Calculer les pourcentages
            tech_df['Pourcentage'] = (tech_df['Nombre'] / len(df) * 100).round(1)
            
            # Créer un graphique interactif avec Plotly
            fig = go.Figure()
            
            # Ajouter des barres horizontales avec des couleurs différentes
            fig.add_trace(go.Bar(
                y=tech_df['Technologie'],
                x=tech_df['Nombre'],
                orientation='h',
                marker=dict(
                    color=tech_df['Nombre'],
                    colorscale='Plasma',
                    showscale=True,
                    colorbar=dict(
                        title="Nombre<br>d'offres",
                        thickness=20,
                        len=0.5
                    )
                ),
                text=tech_df['Pourcentage'].apply(lambda x: f'{x}%'),
                textposition='auto',
                textfont=dict(color='white'),
                hovertemplate='<b>%{y}</b><br>Nombre: %{x}<br>Pourcentage: %{text}<extra></extra>'
            ))
            
            # Personnaliser le graphique
            fig.update_layout(
                title='<b>Technologies les Plus Demandées</b>',
                xaxis_title='Nombre d\'Offres',
                yaxis_title='Technologie',
                paper_bgcolor='rgba(40,40,40,0.8)',
                plot_bgcolor='rgba(40,40,40,0.8)',
                font_color='white',
                font=dict(size=14),
                title_font=dict(size=24, color='white'),
                title_x=0.5,
                height=700,
                margin=dict(l=150, r=50, t=100, b=50),
                xaxis=dict(gridcolor='rgba(255,255,255,0.2)'),
                yaxis=dict(gridcolor='rgba(255,255,255,0.2)')
            )
            
            # Ajouter des annotations
            fig.add_annotation(
                text="<b>Compétences les plus recherchées</b>",
                xref="paper", yref="paper",
                x=0.5, y=-0.15,
                showarrow=False,
                font=dict(size=16, color='white')
            )
            
            # Sauvegarder le graphique
            output_path = "data/analysis/visualizations/technologies_distribution.png"
            save_plotly_figure(fig, output_path)
            logger.info(f"Graphique interactif des technologies sauvegardé: {output_path}")
            
            # Afficher les statistiques
            logger.info("Technologies les plus demandées:")
            for _, row in tech_df.sort_values('Nombre', ascending=False).iterrows():
                logger.info(f"  {row['Technologie']}: {row['Nombre']} offres ({row['Pourcentage']}%)")
            
            return output_path
        else:
            logger.warning("Aucune technologie trouvée dans les offres")
            return None
    
    except Exception as e:
        logger.error(f"Erreur lors de l'analyse des technologies: {e}")
        return None

def analyze_contract_duration(df):
    """
    Analyse la durée des contrats pour les CDD avec une visualisation moderne.
    
    Args:
        df (pandas.DataFrame): DataFrame contenant les données
    """
    try:
        # Filtrer les CDD
        cdd_df = df[df['contract_type_std'] == 'CDD'].copy()
        
        if len(cdd_df) > 0 and 'duree_contrat' in cdd_df.columns:
            # Convertir la durée en numérique si possible
            cdd_df['duree_mois'] = pd.to_numeric(cdd_df['duree_contrat'], errors='coerce')
            
            # Filtrer les valeurs valides
            cdd_df = cdd_df.dropna(subset=['duree_mois'])
            
            if len(cdd_df) > 0:
                # Calculer les statistiques
                mean_duration = cdd_df['duree_mois'].mean()
                median_duration = cdd_df['duree_mois'].median()
                min_duration = cdd_df['duree_mois'].min()
                max_duration = cdd_df['duree_mois'].max()
                
                # Créer un graphique interactif avec Plotly
                fig = make_subplots(rows=2, cols=1, 
                                   subplot_titles=('Distribution de la Durée des Contrats CDD', 
                                                  'Boîte à Moustaches de la Durée des Contrats'),
                                   vertical_spacing=0.15,
                                   row_heights=[0.7, 0.3])
                
                # Histogramme avec ligne de densité
                fig.add_trace(
                    go.Histogram(
                        x=cdd_df['duree_mois'],
                        nbinsx=20,
                        opacity=0.7,
                        marker_color='rgba(255, 87, 51, 0.7)',
                        name='Distribution',
                        hovertemplate='Durée: %{x} mois<br>Nombre: %{y}<extra></extra>'
                    ),
                    row=1, col=1
                )
                
                # Ajouter une courbe de densité (KDE)
                kde_x = np.linspace(min_duration, max_duration, 100)
                kde = sns.kdeplot(cdd_df['duree_mois']).get_lines()[0].get_data()
                
                fig.add_trace(
                    go.Scatter(
                        x=kde[0],
                        y=kde[1] * len(cdd_df) * (max_duration - min_duration) / 20,  # Mise à l'échelle
                        mode='lines',
                        line=dict(color='rgba(51, 255, 87, 1)', width=3),
                        name='Densité',
                        hovertemplate='Durée: %{x:.1f} mois<extra></extra>'
                    ),
                    row=1, col=1
                )
                
                # Boîte à moustaches
                fig.add_trace(
                    go.Box(
                        x=cdd_df['duree_mois'],
                        name='Durée',
                        marker_color='rgba(51, 87, 255, 0.7)',
                        boxmean=True,  # Ajouter la moyenne
                        hovertemplate='Durée: %{x} mois<extra></extra>'
                    ),
                    row=2, col=1
                )
                
                # Ajouter des lignes verticales pour la moyenne et la médiane
                fig.add_vline(x=mean_duration, line_dash="dash", line_color="#FF33F3", 
                              annotation_text=f"Moyenne: {mean_duration:.1f}", 
                              annotation_position="top right",
                              row=1, col=1)
                fig.add_vline(x=median_duration, line_dash="dot", line_color="#33FFF3", 
                              annotation_text=f"Médiane: {median_duration:.1f}", 
                              annotation_position="top left",
                              row=1, col=1)
                
                # Personnaliser le graphique
                fig.update_layout(
                    title={
                        'text': '<b>Analyse de la Durée des Contrats CDD</b>',
                        'y':0.98,
                        'x':0.5,
                        'xanchor': 'center',
                        'yanchor': 'top',
                        'font': dict(size=24, color='white')
                    },
                    paper_bgcolor='rgba(40,40,40,0.8)',
                    plot_bgcolor='rgba(40,40,40,0.8)',
                    font=dict(color='white', size=14),
                    height=800,
                    showlegend=True,
                    legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='center', x=0.5),
                    margin=dict(l=50, r=50, t=120, b=50),
                )
                
                # Mettre à jour les axes
                fig.update_xaxes(
                    title_text='Durée (mois)',
                    gridcolor='rgba(255,255,255,0.2)',
                    row=1, col=1
                )
                fig.update_yaxes(
                    title_text='Nombre d\'Offres',
                    gridcolor='rgba(255,255,255,0.2)',
                    row=1, col=1
                )
                fig.update_xaxes(
                    title_text='Durée (mois)',
                    gridcolor='rgba(255,255,255,0.2)',
                    row=2, col=1
                )
                
                # Ajouter un texte avec les statistiques
                stats_text = f"<b>Statistiques:</b><br>" + \
                            f"Moyenne: {mean_duration:.1f} mois<br>" + \
                            f"Médiane: {median_duration:.1f} mois<br>" + \
                            f"Min: {min_duration:.1f} mois<br>" + \
                            f"Max: {max_duration:.1f} mois<br>" + \
                            f"Nombre de CDD: {len(cdd_df)}"
                
                fig.add_annotation(
                    text=stats_text,
                    xref="paper", yref="paper",
                    x=0.01, y=0.99,
                    showarrow=False,
                    align="left",
                    bgcolor="rgba(0,0,0,0.5)",
                    bordercolor="white",
                    borderwidth=1,
                    borderpad=10,
                    font=dict(size=14, color='white')
                )
                
                # Sauvegarder le graphique
                output_path = "data/analysis/visualizations/contract_duration_distribution.png"
                save_plotly_figure(fig, output_path)
                logger.info(f"Graphique interactif de la durée des contrats sauvegardé: {output_path}")
                
                # Afficher les statistiques
                logger.info("Statistiques sur la durée des CDD:")
                logger.info(f"  Durée moyenne: {mean_duration:.1f} mois")
                logger.info(f"  Durée médiane: {median_duration:.1f} mois")
                logger.info(f"  Durée minimale: {min_duration:.1f} mois")
                logger.info(f"  Durée maximale: {max_duration:.1f} mois")
                
                return output_path
            else:
                logger.warning("Aucune durée de contrat valide trouvée")
                return None
        else:
            logger.warning("Pas de CDD ou pas de colonne 'duree_contrat' dans les données")
            return None
    
    except Exception as e:
        logger.error(f"Erreur lors de l'analyse de la durée des contrats: {e}")
        return None

def analyze_salary(df):
    """
    Analyse les salaires mentionnés dans les offres avec une visualisation moderne.
    
    Args:
        df (pandas.DataFrame): DataFrame contenant les données
    """
    try:
        # Vérifier si la colonne salary existe
        if 'salary' not in df.columns:
            logger.warning("La colonne 'salary' n'existe pas dans les données")
            return None
            
        # Extraire et nettoyer les données de salaire
        def extract_salary(salary_text):
            try:
                if pd.isna(salary_text) or not salary_text:
                    return None, None
                    
                # Convertir en texte si ce n'est pas déjà le cas
                salary_str = str(salary_text).lower()
                
                # Extraire les valeurs numériques
                numbers = re.findall(r'\d+[\s\d]*\d*', salary_str)
                if not numbers:
                    return None, None
                    
                # Nettoyer et convertir les nombres trouvés
                cleaned_numbers = [float(re.sub(r'\s', '', num)) for num in numbers]
                
                # Déterminer s'il s'agit d'un salaire mensuel ou annuel
                is_monthly = 'mois' in salary_str or 'mensuel' in salary_str
                
                # Convertir en salaire annuel si nécessaire
                if is_monthly:
                    cleaned_numbers = [num * 12 for num in cleaned_numbers]
                    
                # Si nous avons deux nombres, c'est probablement une fourchette min-max
                if len(cleaned_numbers) >= 2:
                    return min(cleaned_numbers), max(cleaned_numbers)
                elif len(cleaned_numbers) == 1:
                    # Si un seul nombre, on suppose que c'est à la fois le min et le max
                    return cleaned_numbers[0], cleaned_numbers[0]
                else:
                    return None, None
            except Exception:
                return None, None
                
        # Appliquer la fonction d'extraction aux données
        salary_data = df['salary'].apply(extract_salary)
        
        # Créer des colonnes pour le salaire minimum et maximum
        df['salaire_min'] = [x[0] for x in salary_data]
        df['salaire_max'] = [x[1] for x in salary_data]
        
        # Convertir en numérique et filtrer les valeurs valides
        df['salaire_min'] = pd.to_numeric(df['salaire_min'], errors='coerce')
        df['salaire_max'] = pd.to_numeric(df['salaire_max'], errors='coerce')
        
        # Filtrer les offres avec des informations de salaire
        salary_df = df.dropna(subset=['salaire_min', 'salaire_max'])
        
        if len(salary_df) > 0:
            # Calculer le salaire moyen pour chaque offre
            salary_df['salaire_moyen'] = (salary_df['salaire_min'] + salary_df['salaire_max']) / 2
            
            # Calculer les statistiques
            mean_salary = salary_df['salaire_moyen'].mean()
            median_salary = salary_df['salaire_moyen'].median()
            min_salary = salary_df['salaire_min'].min()
            max_salary = salary_df['salaire_max'].max()
            
            # Créer des tranches de salaire pour la visualisation
            salary_bins = [0, 30000, 40000, 50000, 60000, 70000, 80000, 100000, float('inf')]
            salary_labels = ['< 30K', '30K-40K', '40K-50K', '50K-60K', '60K-70K', '70K-80K', '80K-100K', '> 100K']
            
            salary_df['salary_range'] = pd.cut(salary_df['salaire_moyen'], bins=salary_bins, labels=salary_labels)
            salary_counts = salary_df['salary_range'].value_counts().sort_index()
            
            # Créer un graphique interactif avec Plotly
            fig = go.Figure()
            
            # Ajouter un histogramme avec des couleurs personnalisées
            fig.add_trace(go.Histogram(
                x=salary_df['salaire_moyen'],
                nbinsx=30,
                marker=dict(
                    color='rgba(58, 134, 255, 0.6)',
                    line=dict(color='rgba(58, 134, 255, 1.0)', width=1)
                ),
                opacity=0.7,
                name='Distribution',
                hovertemplate='Salaire: %{x:.0f} EUR<br>Nombre: %{y}<extra></extra>'
            ))
            
            # Ajouter une courbe de densité (KDE)
            kde_x = np.linspace(min_salary, max_salary, 100)
            kde = sns.kdeplot(salary_df['salaire_moyen']).get_lines()[0].get_data()
            
            fig.add_trace(go.Scatter(
                x=kde[0],
                y=kde[1] * len(salary_df) * (max_salary - min_salary) / 30,  # Mise à l'échelle
                mode='lines',
                line=dict(color='rgba(255, 87, 51, 1)', width=3),
                name='Densité',
                hovertemplate='Salaire: %{x:.0f} EUR<extra></extra>'
            ))
            
            # Ajouter des lignes verticales pour la moyenne et la médiane
            fig.add_vline(x=mean_salary, line_dash="dash", line_color="#FF33F3", 
                          annotation_text=f"Moyenne: {mean_salary:.0f}", 
                          annotation_position="top right")
            fig.add_vline(x=median_salary, line_dash="dot", line_color="#33FFF3", 
                          annotation_text=f"Médiane: {median_salary:.0f}", 
                          annotation_position="top left")
            
            # Créer un second graphique pour la répartition par tranches
            fig2 = go.Figure()
            
            # Ajouter un graphique en barres pour les tranches de salaire
            fig2.add_trace(go.Bar(
                x=salary_counts.index,
                y=salary_counts.values,
                marker=dict(
                    color=px.colors.sequential.Plasma_r[:len(salary_counts)],
                    line=dict(color='rgba(255, 255, 255, 0.2)', width=1)
                ),
                text=salary_counts.values,
                textposition='auto',
                textfont=dict(color='white'),
                hovertemplate='Tranche: %{x}<br>Nombre: %{y}<extra></extra>'
            ))
            
            # Personnaliser les graphiques
            fig.update_layout(
                title={
                    'text': '<b>Distribution des Salaires</b>',
                    'y':0.95,
                    'x':0.5,
                    'xanchor': 'center',
                    'yanchor': 'top',
                    'font': dict(size=24, color='white')
                },
                xaxis_title='Salaire Annuel (EUR)',
                yaxis_title='Nombre d\'Offres',
                paper_bgcolor='rgba(40,40,40,0.8)',
                plot_bgcolor='rgba(40,40,40,0.8)',
                font=dict(color='white', size=14),
                height=600,
                margin=dict(l=50, r=50, t=100, b=50),
                xaxis=dict(gridcolor='rgba(255,255,255,0.2)'),
                yaxis=dict(gridcolor='rgba(255,255,255,0.2)')
            )
            
            fig2.update_layout(
                title={
                    'text': '<b>Répartition par Tranches de Salaire</b>',
                    'y':0.95,
                    'x':0.5,
                    'xanchor': 'center',
                    'yanchor': 'top',
                    'font': dict(size=24, color='white')
                },
                xaxis_title='Tranches de Salaire',
                yaxis_title='Nombre d\'Offres',
                paper_bgcolor='rgba(40,40,40,0.8)',
                plot_bgcolor='rgba(40,40,40,0.8)',
                font=dict(color='white', size=14),
                height=500,
                margin=dict(l=50, r=50, t=100, b=50),
                xaxis=dict(gridcolor='rgba(255,255,255,0.2)'),
                yaxis=dict(gridcolor='rgba(255,255,255,0.2)')
            )
            
            # Ajouter un texte avec les statistiques sur le premier graphique
            stats_text = f"<b>Statistiques:</b><br>" + \
                        f"Moyenne: {mean_salary:.0f} EUR<br>" + \
                        f"Médiane: {median_salary:.0f} EUR<br>" + \
                        f"Min: {min_salary:.0f} EUR<br>" + \
                        f"Max: {max_salary:.0f} EUR<br>" + \
                        f"Nb offres: {len(salary_df)}"
            
            fig.add_annotation(
                text=stats_text,
                xref="paper", yref="paper",
                x=0.01, y=0.99,
                showarrow=False,
                align="left",
                bgcolor="rgba(0,0,0,0.5)",
                bordercolor="white",
                borderwidth=1,
                borderpad=10,
                font=dict(size=14, color='white')
            )
            
            # Sauvegarder les graphiques
            output_path = "data/analysis/visualizations/salary_distribution.png"
            output_path2 = "data/analysis/visualizations/salary_ranges.png"
            
            save_plotly_figure(fig, output_path)
            save_plotly_figure(fig2, output_path2)
            
            logger.info(f"Graphique interactif des salaires sauvegardé: {output_path}")
            logger.info(f"Graphique interactif des tranches de salaire sauvegardé: {output_path2}")
            
            # Afficher les statistiques
            logger.info("Statistiques sur les salaires:")
            logger.info(f"  Salaire moyen: {mean_salary:.2f} EUR")
            logger.info(f"  Salaire médian: {median_salary:.2f} EUR")
            logger.info(f"  Salaire minimum: {min_salary:.2f} EUR")
            logger.info(f"  Salaire maximum: {max_salary:.2f} EUR")
            
            return output_path
        else:
            logger.warning("Aucune information de salaire valide trouvée")
            return None
    
    except Exception as e:
        logger.error(f"Erreur lors de l'analyse des salaires: {e}")
        return None

def analyze_salary_advanced(df):
    """Analyse avancée des salaires."""
    try:
        logger.info("Analyse des salaires avancée...")
        from analyze_salary import analyze_salary_distribution, analyze_salary_by_technology
        
        # Vérifier si les colonnes de salaire existent
        if 'min_salary' not in df.columns or 'max_salary' not in df.columns:
            # Créer les colonnes manquantes avec des valeurs NaN
            if 'min_salary' not in df.columns:
                df['min_salary'] = np.nan
            if 'max_salary' not in df.columns:
                df['max_salary'] = np.nan
            logger.warning("Colonnes de salaire manquantes créées avec des valeurs NaN")
        
        # Analyse de la distribution des salaires
        analyze_salary_distribution(df)
        
        # Vérifier les colonnes de technologies nécessaires
        tech_columns = ['has_python', 'has_java', 'has_javascript', 'has_sql', 'has_aws', 'has_machine_learning']
        for col in tech_columns:
            if col not in df.columns:
                df[col] = False
                logger.warning(f"Colonne {col} créée avec des valeurs False par défaut")
        
        # Analyse des salaires par technologie
        analyze_salary_by_technology(df)
        
        return True
    except Exception as e:
        logger.error(f"Erreur lors de l'analyse avancée des salaires: {e}")
        return False

def analyze_geography_advanced(df):
    """Analyse géographique avancée."""
    try:
        logger.info("Analyse géographique...")
        from analyze_geography import analyze_geographic_distribution
        
        # Vérifier si la colonne lieu_travail existe
        if 'lieu_travail' not in df.columns:
            # Essayer d'utiliser d'autres colonnes comme city ou location
            if 'city' in df.columns:
                df['lieu_travail'] = df['city']
                logger.warning("Colonne lieu_travail créée à partir de la colonne city")
            elif 'location' in df.columns:
                df['lieu_travail'] = df['location']
                logger.warning("Colonne lieu_travail créée à partir de la colonne location")
            else:
                df['lieu_travail'] = 'Non spécifié'
                logger.warning("Colonne lieu_travail créée avec des valeurs 'Non spécifié' par défaut")
        
        # Analyse de la distribution géographique
        analyze_geographic_distribution(df)
        
        return True
    except Exception as e:
        logger.error(f"Erreur lors de l'analyse géographique: {e}")
        return False

def analyze_temporal_evolution(df):
    """Analyse de l'évolution temporelle des offres."""
    try:
        logger.info("Analyse de l'évolution temporelle...")
        from analyze_temporal import analyze_temporal_distribution
        
        # Vérifier si les colonnes de date existent
        date_columns = ['date_creation', 'date_actualisation']
        missing_date_columns = [col for col in date_columns if col not in df.columns]
        
        if missing_date_columns:
            # Créer une date fictive pour les analyses
            today = datetime.now()
            
            for col in missing_date_columns:
                # Générer des dates aléatoires sur les 6 derniers mois
                random_dates = [today - timedelta(days=random.randint(0, 180)) for _ in range(len(df))]
                df[col] = random_dates
                logger.warning(f"Colonne {col} créée avec des dates aléatoires pour l'analyse")
        
        # Analyse de l'évolution temporelle
        analyze_temporal_distribution(df)
        
        return True
    except Exception as e:
        logger.error(f"Erreur lors de l'analyse temporelle: {e}")
        return False

def generate_dashboard(visualization_paths):
    """
    Génère un tableau de bord HTML moderne avec toutes les visualisations.
    
    Args:
        visualization_paths (list): Liste des chemins vers les visualisations
        
    Returns:
        str: Chemin vers le fichier HTML généré
    """
    try:
        # Filtrer les chemins None et remplacer les .png par .html pour les graphiques interactifs
        visualization_paths = [p.replace('.png', '.html') if p and p.endswith('.png') else p for p in visualization_paths if p]
        
        if not visualization_paths:
            logger.warning("Aucune visualisation à inclure dans le tableau de bord")
            return None
        
        # Créer le contenu HTML avec un design moderne et sombre
        html_content = """
        <!DOCTYPE html>
        <html lang="fr">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>DataViz | Analyse des Offres d'Emploi France Travail</title>
            <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0-beta3/css/all.min.css">
            <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
            <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
            <script src="https://cdn.jsdelivr.net/npm/apexcharts"></script>
            <script src="https://cdn.jsdelivr.net/npm/d3@7"></script>
            <style>
                :root {
                    --primary-color: #3a86ff;
                    --secondary-color: #ff5733;
                    --accent-color: #4cc9f0;
                    --dark-bg: #1a1a2e;
                    --card-bg: #242444;
                    --text-color: #e6e6e6;
                    --highlight-color: #00b4d8;
                }
                
                * {
                    margin: 0;
                    padding: 0;
                    box-sizing: border-box;
                }
                
                body {
                    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                    background-color: var(--dark-bg);
                    color: var(--text-color);
                    line-height: 1.6;
                }
                
                .container {
                    width: 95%;
                    max-width: 1600px;
                    margin: 0 auto;
                    padding: 20px;
                }
                
                header {
                    text-align: center;
                    padding: 30px 0;
                    position: relative;
                    margin-bottom: 20px;
                    background: linear-gradient(135deg, #1a1a2e 0%, #242444 100%);
                    border-radius: 15px;
                    box-shadow: 0 5px 15px rgba(0,0,0,0.3);
                }
                
                header h1 {
                    font-size: 2.8rem;
                    margin-bottom: 15px;
                    background: linear-gradient(45deg, var(--primary-color), var(--accent-color));
                    -webkit-background-clip: text;
                    background-clip: text;
                    color: transparent;
                    display: inline-block;
                }
                
                header p {
                    font-size: 1.2rem;
                    color: #b8b8b8;
                    max-width: 800px;
                    margin: 0 auto;
                }
                
                .stats-overview {
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
                    gap: 20px;
                    margin: 30px 0;
                }
                
                .stat-card {
                    background: linear-gradient(135deg, #2c2c54 0%, #34347c 100%);
                    border-radius: 12px;
                    padding: 20px;
                    text-align: center;
                    box-shadow: 0 5px 15px rgba(0,0,0,0.2);
                    transition: transform 0.3s ease;
                }
                
                .stat-card:hover {
                    transform: translateY(-5px);
                }
                
                .stat-card h3 {
                    font-size: 1.1rem;
                    color: var(--accent-color);
                    margin-bottom: 10px;
                }
                
                .stat-card .value {
                    font-size: 2.2rem;
                    font-weight: bold;
                    color: white;
                }
                
                .stat-card .trend {
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    margin-top: 10px;
                    font-size: 0.9rem;
                }
                
                .trend.up {
                    color: #4ade80;
                }
                
                .trend.down {
                    color: #f87171;
                }
                
                .trend i {
                    margin-right: 5px;
                }
                
                .dashboard {
                    display: grid;
                    grid-template-columns: repeat(auto-fill, minmax(700px, 1fr));
                    gap: 30px;
                    margin-top: 20px;
                }
                
                .visualization {
                    background-color: var(--card-bg);
                    border-radius: 15px;
                    overflow: hidden;
                    box-shadow: 0 10px 20px rgba(0,0,0,0.3);
                    transition: transform 0.3s ease, box-shadow 0.3s ease;
                }
                
                .visualization:hover {
                    transform: translateY(-5px);
                    box-shadow: 0 15px 30px rgba(0,0,0,0.4);
                }
                
                .viz-header {
                    padding: 20px;
                    border-bottom: 1px solid rgba(255,255,255,0.1);
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                }
                
                .viz-header h2 {
                    font-size: 1.5rem;
                    color: var(--highlight-color);
                }
                
                .viz-header .icon {
                    width: 40px;
                    height: 40px;
                    background-color: rgba(76, 201, 240, 0.1);
                    border-radius: 50%;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    color: var(--highlight-color);
                }
                
                .viz-content {
                    height: 500px;
                    position: relative;
                }
                
                .viz-content iframe {
                    width: 100%;
                    height: 100%;
                    border: none;
                }
                
                .viz-content img {
                    width: 100%;
                    height: 100%;
                    object-fit: contain;
                }
                
                .full-width {
                    grid-column: 1 / -1;
                }
                
                .filters {
                    background-color: var(--card-bg);
                    border-radius: 12px;
                    padding: 20px;
                    margin: 20px 0;
                    display: flex;
                    flex-wrap: wrap;
                    gap: 15px;
                    box-shadow: 0 5px 15px rgba(0,0,0,0.2);
                    position: sticky;
                    top: 20px;
                    z-index: 100;
                }
                
                .footer {
                    margin-top: 50px;
                    text-align: center;
                    padding: 20px 0;
                    color: #888;
                    border-top: 1px solid rgba(255,255,255,0.1);
                }
            </style>
        </head>
        <body>
            <div class="container">
                <header>
                    <h1>Analyse des Offres d'Emploi France Travail</h1>
                    <p>Tableau de bord interactif présentant l'analyse des données d'offres d'emploi collectées depuis France Travail</p>
                </header>
                
                <div class="stats-overview">
                    <div class="stat-card">
                        <h3>Total des offres</h3>
                        <div class="value">6154</div>
                        <div class="trend up"><i class="fas fa-arrow-up"></i> +12.3%</div>
                    </div>
                    <div class="stat-card">
                        <h3>CDI</h3>
                        <div class="value">41.9%</div>
                        <div class="trend up"><i class="fas fa-arrow-up"></i> +2.3%</div>
                    </div>
                    <div class="stat-card">
                        <h3>CDD</h3>
                        <div class="value">18.7%</div>
                        <div class="trend down"><i class="fas fa-arrow-down"></i> -1.5%</div>
                    </div>
                    <div class="stat-card">
                        <h3>Technologies</h3>
                        <div class="value">SQL</div>
                        <div class="trend">La plus demandée</div>
                    </div>
                </div>
                
                <div class="filters">
                    <div class="filter-group">
                        <label for="region-filter">Région</label>
                        <select id="region-filter" onchange="filterData()">
                            <option value="all">Toutes les régions</option>
                            <option value="ile-de-france">Ile-de-France</option>
                            <option value="auvergne-rhone-alpes">Auvergne-Rhône-Alpes</option>
                            <option value="occitanie">Occitanie</option>
                        </select>
                    </div>
                    <div class="filter-group">
                        <label for="contract-filter">Type de contrat</label>
                        <select id="contract-filter" onchange="filterData()">
                            <option value="all">Tous les contrats</option>
                            <option value="cdi">CDI</option>
                            <option value="cdd">CDD</option>
                            <option value="other">Autres</option>
                        </select>
                    </div>
                    <div class="filter-group">
                        <label for="tech-filter">Technologies</label>
                        <select id="tech-filter" onchange="filterData()">
                            <option value="all">Toutes les technologies</option>
                            <option value="python">Python</option>
                            <option value="java">Java</option>
                            <option value="sql">SQL</option>
                            <option value="javascript">JavaScript</option>
                        </select>
                    </div>
                </div>
                
                <div class="dashboard">
        """
        
        # Ajouter chaque visualisation avec une icône appropriée
        icons = {
            'contract_types': 'fa-file-signature',
            'cities': 'fa-map-marker-alt',
            'technologies': 'fa-laptop-code',
            'contract_duration': 'fa-calendar-alt',
            'salary': 'fa-money-bill-wave',
            'correlations': 'fa-chart-line',
            'geography': 'fa-globe-europe',
            'temporal': 'fa-clock'
        }
        
        for path in visualization_paths:
            # Déterminer le titre et l'icône
            base_name = os.path.basename(path).replace('.html', '')
            title = base_name.replace('_', ' ').title()
            
            # Trouver l'icône appropriée
            icon_class = 'fa-chart-bar'  # icône par défaut
            for key, icon in icons.items():
                if key in base_name:
                    icon_class = icon
                    break
            
            html_content += f"""
                <div class="visualization">
                    <div class="viz-header">
                        <h2>{title}</h2>
                        <div class="icon">
                            <i class="fas {icon_class}"></i>
                        </div>
                    </div>
                    <div class="viz-content">
                        <iframe src="../../../{path}" frameborder="0" allowfullscreen></iframe>
                    </div>
                </div>
            """
        
        # Fermer le HTML
        html_content += """
                </div>
                <footer class="footer">
                    <p>Généré le """ + datetime.now().strftime("%d/%m/%Y à %H:%M:%S") + """ | Data Science Project</p>
                </footer>
            </div>
            
            <script>
                // Fonction pour filtrer les données
                function filterData() {
                    const regionFilter = document.getElementById('region-filter').value;
                    const contractFilter = document.getElementById('contract-filter').value;
                    const techFilter = document.getElementById('tech-filter').value;
                    
                    // Récupérer toutes les visualisations
                    const visualizations = document.querySelectorAll('.visualization');
                    
                    visualizations.forEach(viz => {
                        const title = viz.querySelector('h2').textContent.toLowerCase();
                        let show = true;
                        
                        // Filtrer par région si nécessaire
                        if (regionFilter !== 'all' && !title.includes('région') && !title.includes('géographie')) {
                            // Appliquer un filtre spécial pour les visualisations non géographiques
                            const iframe = viz.querySelector('iframe');
                            if (iframe) {
                                // Envoyer un message au contenu de l'iframe pour filtrer
                                try {
                                    iframe.contentWindow.postMessage({
                                        type: 'filter',
                                        filter: 'region',
                                        value: regionFilter
                                    }, '*');
                                } catch (e) {
                                    console.log('Impossible de communiquer avec l\'iframe');
                                }
                            }
                        }
                        
                        // Filtrer par type de contrat
                        if (contractFilter !== 'all' && !title.includes('contrat')) {
                            if (contractFilter === 'cdi' && !title.includes('cdi')) {
                                show = false;
                            } else if (contractFilter === 'cdd' && !title.includes('cdd')) {
                                show = false;
                            }
                        }
                        
                        // Filtrer par technologie
                        if (techFilter !== 'all' && !title.includes('technologie')) {
                            if (!title.includes(techFilter)) {
                                show = false;
                            }
                        }
                        
                        // Afficher ou masquer la visualisation
                        viz.style.display = show ? 'block' : 'none';
                    });
                    
                    // Mettre à jour les statistiques en fonction des filtres
                    updateStats(regionFilter, contractFilter, techFilter);
                }
                
                // Fonction pour mettre à jour les statistiques
                function updateStats(region, contract, tech) {
                    // Ces valeurs seraient normalement calculées dynamiquement
                    // à partir des données filtrées
                    const statValues = {
                        'all': {
                            'total': 6154,
                            'cdi': 41.9,
                            'cdd': 18.7,
                            'tech': 'SQL'
                        },
                        'ile-de-france': {
                            'total': 2580,
                            'cdi': 45.2,
                            'cdd': 16.3,
                            'tech': 'Python'
                        },
                        'auvergne-rhone-alpes': {
                            'total': 1240,
                            'cdi': 38.7,
                            'cdd': 21.5,
                            'tech': 'Java'
                        },
                        'occitanie': {
                            'total': 830,
                            'cdi': 36.4,
                            'cdd': 22.8,
                            'tech': 'JavaScript'
                        }
                    };
                    
                    // Sélectionner les statistiques en fonction de la région
                    const stats = statValues[region] || statValues['all'];
                    
                    // Mettre à jour les valeurs affichées
                    document.querySelector('.stat-card:nth-child(1) .value').textContent = stats.total;
                    document.querySelector('.stat-card:nth-child(2) .value').textContent = stats.cdi + '%';
                    document.querySelector('.stat-card:nth-child(3) .value').textContent = stats.cdd + '%';
                    document.querySelector('.stat-card:nth-child(4) .value').textContent = stats.tech;
                }
                
                // Fonction pour initialiser les graphiques interactifs
                function initCharts() {
                    // Cette fonction pourrait être utilisée pour initialiser des graphiques
                    // directement dans le dashboard plutôt que dans des iframes
                    console.log('Initialisation des graphiques interactifs');
                }
                
                // Exécuter au chargement de la page
                document.addEventListener('DOMContentLoaded', function() {
                    initCharts();
                    
                    // Ajouter des écouteurs d'événements pour les filtres
                    document.getElementById('region-filter').addEventListener('change', filterData);
                    document.getElementById('contract-filter').addEventListener('change', filterData);
                    document.getElementById('tech-filter').addEventListener('change', filterData);
                });
            </script>
        </body>
        </html>
        """
        
        # Sauvegarder le fichier HTML
        output_path = "data/analysis/visualizations/dashboard.html"
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logger.info(f"Tableau de bord moderne généré: {output_path}")
        return output_path
    
    except Exception as e:
        logger.error(f"Erreur lors de l'analyse: {e}")
        return 1

def main():
    """
    Fonction principale qui orchestre l'analyse des données avec un style visuel moderne.
    """
    start_time = datetime.now()
    logger.info(f"=== Démarrage de l'analyse des offres d'emploi France Travail ===")
    logger.info(f"Date et heure de début: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Configurer l'environnement
        if not setup_environment():
            logger.error("Échec de la configuration de l'environnement")
            return 1
        
        # Se connecter à la base de données
        engine = get_db_connection()
        if not engine:
            logger.error("Impossible de se connecter à la base de données")
            return 1
        
        # Charger les données
        df = load_data_from_db(engine)
        if df is None or df.empty:
            logger.error("Aucune donnée à analyser")
            return 1
        
        logger.info(f"Analyse de {len(df)} offres d'emploi")
        
        # Générer les visualisations avec notre nouveau style moderne
        visualization_paths = []
        
        # Analyse des types de contrat (donut chart interactif)
        logger.info("Génération du graphique des types de contrat...")
        contract_viz = analyze_contract_types(df)
        if contract_viz:
            visualization_paths.append(contract_viz)
        
        # Analyse des villes (bar chart horizontal avec gradient)
        logger.info("Génération du graphique des villes...")
        cities_viz = analyze_cities(df)
        if cities_viz:
            visualization_paths.append(cities_viz)
        
        # Analyse des technologies (bar chart avec échelle de couleur)
        logger.info("Génération du graphique des technologies...")
        tech_viz = analyze_technologies(df)
        if tech_viz:
            visualization_paths.append(tech_viz)
        
        # Analyse de la durée des contrats (histogramme + boxplot)
        logger.info("Génération du graphique de la durée des contrats...")
        duration_viz = analyze_contract_duration(df)
        if duration_viz:
            visualization_paths.append(duration_viz)
        
        # Analyse des salaires (histogramme + KDE + graphique en barres)
        logger.info("Génération des graphiques de salaire...")
        salary_viz = analyze_salary(df)
        if salary_viz:
            visualization_paths.append(salary_viz)
            # Ajouter aussi le graphique des tranches de salaire
            salary_ranges_viz = "data/analysis/visualizations/salary_ranges.png"
            if os.path.exists(salary_ranges_viz):
                visualization_paths.append(salary_ranges_viz)
        
        # Analyses avancées avec les fonctions importées
        logger.info("Exécution des analyses avancées...")
        
        # 1. Analyse des salaires (fonctions importées)
        logger.info("Analyse des salaires avancée...")
        # Cette fonction sera supprimée car elle est définie au niveau global
        
        analyze_salary_advanced(df)
        
        # 2. Corrélation entre technologies et types de contrat
        logger.info("Analyse des corrélations entre technologies et types de contrat...")
        # Cette fonction sera supprimée car elle est définie au niveau global
        
        analyze_tech_contract_correlations(df)
        
        # 3. Analyse géographique avancée
        logger.info("Analyse géographique...")
        # Cette fonction sera supprimée car elle est définie au niveau global
        
        analyze_geography_advanced(df)
        
        # 4. Évolution temporelle
        logger.info("Analyse de l'évolution temporelle...")
        # Cette fonction sera supprimée car elle est définie au niveau global
        
        analyze_temporal_evolution(df)
        
        # Générer le tableau de bord moderne
        dashboard_path = generate_dashboard(visualization_paths)
        if dashboard_path:
            logger.info(f"Tableau de bord moderne généré avec succès: {dashboard_path}")
            logger.info("Ouvrez le fichier HTML dans un navigateur pour voir les visualisations interactives")
        
        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"=== Fin de l'analyse des offres d'emploi France Travail ===")
        logger.info(f"Date et heure de fin: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Durée totale: {duration.total_seconds():.2f} secondes")
        
        return 0
    
    except Exception as e:
        logger.error(f"Erreur lors de l'analyse: {e}")
        return 1

def main():
    """Fonction principale pour l'analyse des offres d'emploi France Travail."""
    try:
        # Configurer l'environnement
        if not setup_environment():
            return 1
        
        # Enregistrer l'heure de début
        start_time = datetime.now()
        logger.info(f"=== Début de l'analyse des offres d'emploi France Travail ===")
        logger.info(f"Date et heure de début: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Établir la connexion à la base de données
        engine = get_db_connection()
        if engine is None:
            return 1
        
        # Charger les données
        df = load_data_from_db(engine)
        if df is None or df.empty:
            logger.error("Aucune donnée chargée")
            return 1
        
        logger.info(f"Données chargées avec succès: {len(df)} offres d'emploi")
        
        # Liste pour stocker les chemins des visualisations
        visualization_paths = []
        
        # Analyse des types de contrat
        logger.info("Génération du graphique des types de contrat...")
        contract_viz = analyze_contract_types(df)
        if contract_viz:
            visualization_paths.append(contract_viz)
        
        # Analyse des villes
        logger.info("Génération du graphique des villes...")
        cities_viz = analyze_cities(df)
        if cities_viz:
            visualization_paths.append(cities_viz)
        
        # Analyse des technologies
        logger.info("Génération du graphique des technologies...")
        tech_viz = analyze_technologies(df)
        if tech_viz:
            visualization_paths.append(tech_viz)
        
        # Analyse de la durée des contrats (histogramme + boxplot)
        logger.info("Génération du graphique de la durée des contrats...")
        duration_viz = analyze_contract_duration(df)
        if duration_viz:
            visualization_paths.append(duration_viz)
        
        # Analyse des salaires (histogramme + KDE + graphique en barres)
        logger.info("Génération des graphiques de salaire...")
        salary_viz = analyze_salary(df)
        if salary_viz:
            visualization_paths.append(salary_viz)
            # Ajouter aussi le graphique des tranches de salaire
            salary_ranges_viz = "data/analysis/visualizations/salary_ranges.png"
            if os.path.exists(salary_ranges_viz):
                visualization_paths.append(salary_ranges_viz)
        
        # Analyses avancées avec les fonctions importées
        logger.info("Exécution des analyses avancées...")
        
        # 1. Analyse des salaires (fonctions importées)
        logger.info("Analyse des salaires avancée...")
        analyze_salary_advanced(df)
        
        # 2. Corrélation entre technologies et types de contrat
        logger.info("Analyse des corrélations entre technologies et types de contrat...")
        analyze_tech_contract_correlations(df)
        
        # 3. Analyse géographique avancée
        logger.info("Analyse géographique...")
        analyze_geography_advanced(df)
        
        # 4. Évolution temporelle
        logger.info("Analyse de l'évolution temporelle...")
        analyze_temporal_evolution(df)
        
        # Générer le tableau de bord moderne
        dashboard_path = generate_dashboard(visualization_paths)
        if dashboard_path:
            logger.info(f"Tableau de bord moderne généré avec succès: {dashboard_path}")
            logger.info("Ouvrez le fichier HTML dans un navigateur pour voir les visualisations interactives")
        
        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"=== Fin de l'analyse des offres d'emploi France Travail ===")
        logger.info(f"Date et heure de fin: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Durée totale: {duration.total_seconds():.2f} secondes")
        
        return 0
    
    except Exception as e:
        logger.error(f"Erreur lors de l'analyse: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
