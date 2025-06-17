#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Module de chargement des compétences et de leur association aux offres d'emploi.
Gère le chargement des compétences et des relations job_skills dans la base PostgreSQL.
"""

import os
import sys
import logging
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, DateTime, ForeignKey, inspect, text, select, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.exc import SQLAlchemyError

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/etl_skills_loader_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Déclaration des modèles SQLAlchemy
Base = declarative_base()

class Skill(Base):
    """Modèle SQLAlchemy pour la table des compétences"""
    __tablename__ = 'skills'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(String(255), nullable=True)  # Colonne détectée dans la structure réelle
    skill = Column(String(100), nullable=False)  # Utilise 'skill' au lieu de 'skill_name'
    # Les colonnes category et created_at ne sont pas dans la table réelle,
    # nous ne les incluons donc pas dans le modèle
    
    def __repr__(self):
        return f"<Skill(id={self.id}, skill='{self.skill}')>"

class JobSkill(Base):
    """Modèle SQLAlchemy pour la table de liaison entre offres et compétences"""
    __tablename__ = 'job_skills'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(String(100), nullable=False)
    # Selon l'inspection de la BD, nous devons utiliser 'skill' directement au lieu de skill_id
    skill = Column(String(100), nullable=False)  # Nom de la compétence
    source = Column(String(50))  # FRANCE_TRAVAIL ou WELCOME_JUNGLE
    etl_timestamp = Column(DateTime, default=datetime.now)
    
    def __repr__(self):
        return f"<JobSkill(id={self.id}, job_id='{self.job_id}', skill='{self.skill}')>"

def get_db_connection():
    """
    Établit une connexion à la base de données PostgreSQL.
    
    Returns:
        tuple: (sqlalchemy.engine.base.Engine, sqlalchemy.orm.session.Session)
    """
    try:
        # Récupérer les paramètres de connexion depuis les variables d'environnement
        host = os.getenv('DB_HOST', 'datawarehouses.c32ygg4oyapa.eu-north-1.rds.amazonaws.com')
        port = os.getenv('DB_PORT', '5432')
        database = os.getenv('DB_NAME', 'datawarehouses')
        user = os.getenv('DB_USER', 'admin')
        password = os.getenv('DB_PASSWORD', 'm!wgz#$gsPD}d7x')

        # Créer l'URL de connexion
        conn_str = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        
        # Créer le moteur avec un timeout augmenté
        engine = create_engine(conn_str, connect_args={'connect_timeout': 30})
        
        # Créer la session
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # Tester la connexion
        session.execute(select(func.now()))
        logger.info(f"Connexion établie avec succès à la base de données {database} sur {host}")
            
        return engine, session
    except SQLAlchemyError as e:
        logger.error(f"Erreur de connexion à la base de données: {e}")
        logger.warning("Vérifiez que l'instance RDS est accessible depuis votre réseau")
        return None, None

def inspect_table_structure(engine, table_name):
    """
    Inspecte la structure d'une table existante pour connaître ses colonnes.
    
    Args:
        engine: Moteur de connexion SQLAlchemy
        table_name: Nom de la table à inspecter
        
    Returns:
        list: Liste des noms de colonnes si la table existe, sinon None
    """
    if engine is None:
        logger.error("Impossible d'inspecter la table: pas de connexion à la base de données")
        return None
    
    try:
        # Utiliser l'inspecteur SQLAlchemy pour examiner la structure de la table
        inspector = inspect(engine)
        if not inspector.has_table(table_name):
            logger.info(f"La table '{table_name}' n'existe pas encore")
            return None
        
        columns = inspector.get_columns(table_name)
        column_names = [col['name'] for col in columns]
        column_types = [str(col['type']) for col in columns]
        
        # Créer un dictionnaire pour affichage dans les logs, mais retourner juste les noms de colonnes
        column_info = dict(zip(column_names, column_types))
        logger.info(f"Structure de la table '{table_name}': {column_info}")
        return column_names
    
    except Exception as e:
        logger.error(f"Erreur lors de l'inspection de la table '{table_name}': {e}")
        return None

def create_skills_tables(engine):
    """
    Crée les tables des compétences si elles n'existent pas déjà.
    S'adapte à la structure existante si les tables existent déjà.
    
    Args:
        engine: Moteur de connexion SQLAlchemy
        
    Returns:
        bool: True si les tables existent ou ont été créées avec succès, False sinon
    """
    if engine is None:
        logger.error("Impossible de créer les tables: pas de connexion à la base de données")
        return False
    
    try:
        # Vérifier si les tables existent et leur structure
        skills_columns = inspect_table_structure(engine, 'skills')
        job_skills_columns = inspect_table_structure(engine, 'job_skills')
        
        # Si les tables existent, adapter nos modèles à leur structure réelle
        if skills_columns is not None:
            # Vérifier quel nom est utilisé pour la colonne de nom de compétence
            if 'skill_name' in skills_columns:
                logger.info("La table 'skills' utilise la colonne 'skill_name'")
                # Modèle déjà configuré pour utiliser skill_name
            elif 'name' in skills_columns:
                logger.info("La table 'skills' utilise la colonne 'name', adaptation du modèle")
                # Adapter notre modèle pour utiliser 'name' au lieu de 'skill_name'
                global Skill
                Skill.skill_name = Skill.name  # Utiliser le nom de colonne existant
            else:
                logger.warning("Structure de table 'skills' non standard, tentative de création/modification")
        
        # Créer/mettre à jour les tables
        Base.metadata.create_all(engine)
        logger.info("Tables 'skills' et 'job_skills' créées ou existaient déjà")
        return True
    except SQLAlchemyError as e:
        logger.error(f"Erreur lors de la création des tables: {e}")
        return False

def load_skills(skills_df, engine, session):
    """
    Charge les compétences dans la base de données.
    
    Args:
        skills_df (pandas.DataFrame): DataFrame des compétences à charger
        engine: Moteur de connexion SQLAlchemy
        session: Session SQLAlchemy
        
    Returns:
        dict: Dictionnaire de correspondance entre noms de compétences et IDs
    """
    if skills_df is None or skills_df.empty:
        logger.warning("Aucune compétence à charger")
        return {}
    
    # Vérifier la structure réelle de la table skills
    skills_columns = inspect_table_structure(engine, 'skills')
    
    if skills_columns is None:
        logger.warning("Impossible d'inspecter la structure de la table skills")
        return {}
        
    logger.info(f"Structure détectée de la table skills: {skills_columns}")
    
    try:
        # Dictionnaire pour stocker les correspondances nom -> id
        skill_id_map = {}
        
        # Récupérer les compétences existantes via une requête SQL directe
        try:
            # La table utilise la colonne 'skill'
            result = session.execute(text("SELECT id, skill FROM skills"))
            for row in result:
                skill_id_map[row['skill']] = row['id']
            logger.info(f"Récupéré {len(skill_id_map)} compétences existantes de la table 'skills'")
        except Exception as e:
            logger.warning(f"Erreur lors de la récupération des compétences: {e}")
        
        # Compétences à ajouter (celles qui n'existent pas encore)
        new_skills = []
        for _, row in skills_df.iterrows():
            skill_name = row['name']
            if skill_name not in skill_id_map:
                new_skill = Skill(
                    skill=skill_name,  # Utilise 'skill' au lieu de 'skill_name'
                    job_id=None  # Pas de job_id lors de la création initiale
                )
                new_skills.append(new_skill)
        
        if new_skills:
            try:
                # Ajouter les nouvelles compétences
                session.add_all(new_skills)
                session.commit()
                
                # Mettre à jour le dictionnaire avec les IDs des nouvelles compétences
                for skill_obj in new_skills:
                    skill_id_map[skill_obj.skill] = skill_obj.id
                
                logger.info(f"{len(new_skills)} nouvelles compétences ajoutées à la base de données")
            except Exception as e:
                logger.error(f"Erreur lors de l'ajout des nouvelles compétences: {e}")
                session.rollback()
        else:
            logger.info("Toutes les compétences existent déjà dans la base de données")
        
        return skill_id_map
    
    except SQLAlchemyError as e:
        logger.error(f"Erreur lors du chargement des compétences: {e}")
        session.rollback()
        return {}

def load_job_skills(job_skills_df, engine, session, skill_id_map):
    """
    Charge les relations offres-compétences dans la base de données.
    
    Args:
        job_skills_df (pandas.DataFrame): DataFrame des relations job-skills à charger
        engine: Moteur de connexion SQLAlchemy
        session: Session SQLAlchemy
        skill_id_map: Correspondance entre noms de compétences et IDs
        
    Returns:
        int: Nombre de relations chargées
    """
    if job_skills_df is None or job_skills_df.empty:
        logger.warning("Aucune relation job_skills à charger")
        return 0
    
    # Vérifier la structure réelle de la table job_skills
    job_skills_columns = inspect_table_structure(engine, 'job_skills')
    
    if job_skills_columns is None:
        logger.warning("Impossible d'inspecter la structure de la table job_skills")
        return 0
    
    try:
        # On ne garde que les relations valides, sans vérifier skill_id_map
        # car les compétences proviennent de l'analyse du texte
        valid_relations = []
        for _, row in job_skills_df.iterrows():
            skill_name = row['name']
            try:
                # Créer l'objet JobSkill avec les bons attributs
                job_skill = JobSkill(
                    job_id=row['job_id'],
                    skill=skill_name,  # Utiliser directement le nom de compétence
                    source=row['source'],
                    etl_timestamp=row['etl_timestamp']
                )
                valid_relations.append(job_skill)
            except Exception as e:
                logger.warning(f"Erreur lors de la création d'une relation job-skill: {e}")
        
        if not valid_relations:
            logger.warning("Aucune relation job_skill valide à charger")
            return 0
        
        # Charger les relations par lots pour éviter les problèmes de mémoire
        batch_size = 100
        total_loaded = 0
        
        for i in range(0, len(valid_relations), batch_size):
            batch = valid_relations[i:i+batch_size]
            try:
                session.add_all(batch)
                session.commit()
                total_loaded += len(batch)
                logger.info(f"Lot de {len(batch)} relations chargé: {total_loaded}/{len(valid_relations)} total")
            except Exception as e:
                logger.error(f"Erreur lors du chargement d'un lot de relations: {e}")
                session.rollback()
                # Continuer avec le lot suivant
        
        logger.info(f"{total_loaded} relations job-skills ajoutées à la base de données")
        return total_loaded
    
    except SQLAlchemyError as e:
        logger.error(f"Erreur lors du chargement des relations job_skills: {e}")
        session.rollback()
        return 0

def execute_skills_loading(jobs_df):
    """
    Exécute le chargement complet des compétences et leurs relations avec les offres.
    
    Args:
        jobs_df (pandas.DataFrame): DataFrame des offres d'emploi
        
    Returns:
        tuple: (nombre de compétences chargées, nombre de relations chargées)
    """
    # Importer le module d'extraction des compétences
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from api.skills_extraction import extract_skills_from_dataframe
    
    try:
        # Étape 1: Extraire les compétences des descriptions d'offres
        skills_df, job_skills_df = extract_skills_from_dataframe(jobs_df)
        
        if skills_df is None or job_skills_df is None:
            logger.error("Échec de l'extraction des compétences")
            return 0, 0
        
        # Étape 2: Se connecter à la base de données
        engine, session = get_db_connection()
        
        if engine is None or session is None:
            logger.error("Échec de la connexion à la base de données")
            return 0, 0
        
        # Étape 3: Créer les tables si nécessaire
        if not create_skills_tables(engine):
            logger.error("Échec de la création des tables")
            return 0, 0
        
        # Étape 4: Charger les compétences
        skill_id_map = load_skills(skills_df, engine, session)
        
        # Étape 5: Charger les relations job_skills
        relations_count = load_job_skills(job_skills_df, skill_id_map, engine, session)
        
        return len(skill_id_map), relations_count
    
    except Exception as e:
        logger.error(f"Erreur lors du chargement des compétences: {e}", exc_info=True)
        return 0, 0
    finally:
        if 'session' in locals() and session:
            session.close()

if __name__ == "__main__":
    # Test du module avec les données extraites
    from api.extraction import extract_data
    from api.transformation import transform_job_dataframe
    
    # Extraire les données des 7 derniers jours
    start_date = (datetime.now() - pd.Timedelta(days=7)).strftime('%Y%m%d')
    end_date = datetime.now().strftime('%Y%m%d')
    
    raw_df = extract_data(start_date=start_date, end_date=end_date)
    
    if raw_df is not None:
        # Transformer les données
        transformed_df = transform_job_dataframe(raw_df)
        
        if transformed_df is not None:
            # Charger les compétences
            skills_count, relations_count = execute_skills_loading(transformed_df)
            
            print(f"\nRésumé du chargement:")
            print(f" - {skills_count} compétences dans la base de données")
            print(f" - {relations_count} relations job_skills chargées")
