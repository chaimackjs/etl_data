
"""
Module d'analyse des salaires pour les offres d'emploi France Travail.
"""

import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import logging
import argparse
from datetime import datetime
from sqlalchemy import create_engine, text

# Ajouter le chemin du projet au PYTHONPATH
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Importer l'utilitaire de chargement des variables d'environnement
from src.pipeline.api.dotenv_utils import load_dotenv

# Configuration du logging
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/salary_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def analyze_salary_distribution(df):
    """
    Analyse la distribution des salaires dans les offres d'emploi.
    
    Args:
        df (pandas.DataFrame): DataFrame contenant les données
        
    Returns:
        str: Chemin vers la visualisation générée
    """
    try:
        logger.info("Analyse de la distribution des salaires...")
        
        # Créer une copie du DataFrame pour éviter les modifications en place
        salary_df = df.copy()
        
        # Vérifier si les colonnes de salaire existent
        if 'min_salary' not in salary_df.columns or 'max_salary' not in salary_df.columns:
            logger.warning("Colonnes de salaire non trouvées dans les données")
            return None
        
        # Filtrer les lignes avec des valeurs de salaire valides
        salary_df = salary_df[(salary_df['min_salary'].notna()) | (salary_df['max_salary'].notna())]
        
        if len(salary_df) == 0:
            logger.warning("Aucune donnée de salaire valide trouvée")
            return None
        
        # Calculer le salaire moyen pour chaque offre
        salary_df['avg_salary'] = salary_df[['min_salary', 'max_salary']].mean(axis=1)
        
        # Créer une colonne pour le type de contrat (pour la coloration)
        if 'contract_type_std' in salary_df.columns:
            # Limiter aux types de contrats les plus courants pour la lisibilité
            top_contracts = salary_df['contract_type_std'].value_counts().nlargest(5).index
            salary_df = salary_df[salary_df['contract_type_std'].isin(top_contracts)]
        
        # Créer la figure
        plt.figure(figsize=(12, 8))
        
        # Créer plusieurs visualisations
        plt.subplot(2, 2, 1)
        sns.histplot(data=salary_df, x='avg_salary', bins=30, kde=True)
        plt.title('Distribution des salaires moyens')
        plt.xlabel('Salaire moyen')
        plt.ylabel('Nombre d\'offres')
        
        # Distribution des salaires par type de contrat
        if 'contract_type_std' in salary_df.columns:
            plt.subplot(2, 2, 2)
            sns.boxplot(data=salary_df, x='contract_type_std', y='avg_salary')
            plt.title('Distribution des salaires par type de contrat')
            plt.xlabel('Type de contrat')
            plt.ylabel('Salaire moyen')
            plt.xticks(rotation=45)
        
        # Salaires min et max
        plt.subplot(2, 2, 3)
        sns.histplot(data=salary_df, x='min_salary', color='blue', alpha=0.5, label='Min', bins=20)
        sns.histplot(data=salary_df, x='max_salary', color='red', alpha=0.5, label='Max', bins=20)
        plt.title('Distribution des salaires minimum et maximum')
        plt.xlabel('Salaire')
        plt.ylabel('Nombre d\'offres')
        plt.legend()
        
        # Écart salarial
        salary_df['salary_gap'] = salary_df['max_salary'] - salary_df['min_salary']
        plt.subplot(2, 2, 4)
        sns.histplot(data=salary_df, x='salary_gap', bins=20, kde=True)
        plt.title('Écart entre salaire minimum et maximum')
        plt.xlabel('Écart salarial')
        plt.ylabel('Nombre d\'offres')
        
        # Ajuster la mise en page
        plt.tight_layout()
        
        # Sauvegarder la figure
        os.makedirs("data/analysis/visualizations", exist_ok=True)
        output_path = "data/analysis/visualizations/salary_analysis.png"
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Analyse des salaires terminée, visualisation sauvegardée: {output_path}")
        return output_path
        
    except Exception as e:
        logger.error(f"Erreur lors de l'analyse des salaires: {e}")
        return None

def analyze_salary_by_technology(df):
    """
    Analyse les salaires en fonction des technologies demandées.
    
    Args:
        df (pandas.DataFrame): DataFrame contenant les données
        
    Returns:
        str: Chemin vers la visualisation générée
    """
    try:
        logger.info("Analyse des salaires par technologie...")
        
        # Créer une copie du DataFrame pour éviter les modifications en place
        tech_salary_df = df.copy()
        
        # Vérifier si les colonnes nécessaires existent
        tech_columns = ['has_python', 'has_java', 'has_javascript', 'has_sql', 'has_aws', 'has_machine_learning']
        missing_columns = [col for col in tech_columns if col not in tech_salary_df.columns]
        
        if missing_columns:
            logger.warning(f"Colonnes de technologies manquantes: {missing_columns}")
            # Utiliser uniquement les colonnes disponibles
            tech_columns = [col for col in tech_columns if col in tech_salary_df.columns]
        
        if not tech_columns:
            logger.warning("Aucune colonne de technologie trouvée")
            return None
        
        # Filtrer les lignes avec des valeurs de salaire valides
        tech_salary_df = tech_salary_df[(tech_salary_df['min_salary'].notna()) | (tech_salary_df['max_salary'].notna())]
        
        if len(tech_salary_df) == 0:
            logger.warning("Aucune donnée de salaire valide trouvée")
            return None
        
        # Calculer le salaire moyen pour chaque offre
        tech_salary_df['avg_salary'] = tech_salary_df[['min_salary', 'max_salary']].mean(axis=1)
        
        # Créer la figure
        plt.figure(figsize=(12, 8))
        
        # Préparer les données pour la visualisation
        tech_salary_data = []
        
        for tech_col in tech_columns:
            # Extraire le nom de la technologie (sans le préfixe 'has_')
            tech_name = tech_col.replace('has_', '').replace('_', ' ').title()
            
            # Calculer le salaire moyen pour les offres avec cette technologie
            with_tech = tech_salary_df[tech_salary_df[tech_col] == True]['avg_salary'].mean()
            
            # Calculer le salaire moyen pour les offres sans cette technologie
            without_tech = tech_salary_df[tech_salary_df[tech_col] == False]['avg_salary'].mean()
            
            # Ajouter à la liste des données
            tech_salary_data.append({
                'Technology': tech_name,
                'With': with_tech,
                'Without': without_tech,
                'Difference': with_tech - without_tech,
                'Ratio': with_tech / without_tech if without_tech > 0 else 0
            })
        
        # Convertir en DataFrame
        tech_salary_df = pd.DataFrame(tech_salary_data)
        
        # Trier par différence de salaire
        tech_salary_df = tech_salary_df.sort_values(by='Difference', ascending=False)
        
        # Créer le graphique à barres
        ax = plt.subplot(1, 1, 1)
        
        # Largeur des barres
        bar_width = 0.35
        
        # Positions des barres
        r1 = np.arange(len(tech_salary_df))
        r2 = [x + bar_width for x in r1]
        
        # Créer les barres
        ax.bar(r1, tech_salary_df['With'], color='#5DA5DA', width=bar_width, edgecolor='grey', label='Avec la technologie')
        ax.bar(r2, tech_salary_df['Without'], color='#F15854', width=bar_width, edgecolor='grey', label='Sans la technologie')
        
        # Ajouter les étiquettes, le titre et la légende
        ax.set_xlabel('Technologie')
        ax.set_ylabel('Salaire moyen')
        ax.set_title('Comparaison des salaires moyens selon les technologies requises')
        ax.set_xticks([r + bar_width/2 for r in range(len(tech_salary_df))])
        ax.set_xticklabels(tech_salary_df['Technology'])
        plt.xticks(rotation=45)
        ax.legend()
        
        # Ajouter les valeurs sur les barres
        for i, (with_val, without_val) in enumerate(zip(tech_salary_df['With'], tech_salary_df['Without'])):
            plt.text(i, with_val + 500, f'{with_val:.0f}', ha='center', va='bottom', fontsize=9)
            plt.text(i + bar_width, without_val + 500, f'{without_val:.0f}', ha='center', va='bottom', fontsize=9)
        
        # Ajuster la mise en page
        plt.tight_layout()
        
        # Sauvegarder la figure
        os.makedirs("data/analysis/visualizations", exist_ok=True)
        output_path = "data/analysis/visualizations/salary_by_technology.png"
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Analyse des salaires par technologie terminée, visualisation sauvegardée: {output_path}")
        return output_path
        
    except Exception as e:
        logger.error(f"Erreur lors de l'analyse des salaires par technologie: {e}")
        return None

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
        
        logger.info(f"Données chargées avec succès: {len(df)} lignes")
        return df
    
    except Exception as e:
        logger.error(f"Erreur lors du chargement des données: {e}")
        return None

def parse_arguments():
    """
    Parse les arguments de ligne de commande.
    """
    parser = argparse.ArgumentParser(description='Analyse des salaires des offres d\'emploi')
    parser.add_argument('--force', action='store_true',
                        help='Forcer l\'analyse même si les données existent déjà')
    return parser.parse_args()

def main():
    """
    Fonction principale qui orchestre l'analyse des salaires.
    """
    start_time = datetime.now()
    logger.info(f"=== Démarrage de l'analyse des salaires des offres d'emploi ===")
    logger.info(f"Date et heure de début: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Parser les arguments
        args = parse_arguments()
        
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
        
        logger.info(f"Analyse des salaires de {len(df)} offres d'emploi")
        
        # Analyse des salaires
        salary_viz = analyze_salary_distribution(df)
        tech_salary_viz = analyze_salary_by_technology(df)
        
        # Fin de l'analyse
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info(f"=== Analyse des salaires terminée ===")
        logger.info(f"Date et heure de fin: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Durée totale: {duration}")
        
        if salary_viz:
            logger.info(f"Visualisation de la distribution des salaires disponible à: {salary_viz}")
        
        if tech_salary_viz:
            logger.info(f"Visualisation des salaires par technologie disponible à: {tech_salary_viz}")
        
        return 0
        
    except Exception as e:
        logger.exception(f"Erreur lors de l'analyse des salaires: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
