#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Module de configuration et d'interaction avec AWS Lambda.
"""

import os
import json
import logging
import zipfile
import boto3
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

def create_lambda_deployment_package(source_dir, output_path):
    """
    Crée un package de déploiement Lambda à partir d'un répertoire source.
    
    Args:
        source_dir (str): Répertoire source contenant le code
        output_path (str): Chemin de sortie pour le fichier ZIP
    
    Returns:
        bool: True si le package a été créé avec succès, False sinon
    """
    try:
        # Créer le dossier de sortie si nécessaire
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Créer le fichier ZIP
        with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            # Parcourir tous les fichiers du répertoire source
            for root, _, files in os.walk(source_dir):
                for file in files:
                    # Ignorer les fichiers __pycache__ et .pyc
                    if '__pycache__' in root or file.endswith('.pyc'):
                        continue
                    
                    # Chemin complet du fichier
                    file_path = os.path.join(root, file)
                    
                    # Chemin relatif pour le ZIP
                    rel_path = os.path.relpath(file_path, source_dir)
                    
                    # Ajouter le fichier au ZIP
                    zipf.write(file_path, rel_path)
        
        logger.info(f"Package de déploiement Lambda créé avec succès: {output_path}")
        return True
    except Exception as e:
        logger.error(f"Erreur lors de la création du package de déploiement Lambda: {e}")
        return False

def create_lambda_function(session, function_name, zip_file, handler, role_arn, timeout=60, memory_size=128):
    """
    Crée ou met à jour une fonction Lambda.
    
    Args:
        session (boto3.Session): Session AWS
        function_name (str): Nom de la fonction Lambda
        zip_file (str): Chemin du fichier ZIP contenant le code
        handler (str): Handler de la fonction (ex: 'lambda_function.handler')
        role_arn (str): ARN du rôle IAM
        timeout (int): Timeout en secondes
        memory_size (int): Taille de la mémoire en Mo
    
    Returns:
        bool: True si la fonction a été créée ou mise à jour avec succès, False sinon
    """
    lambda_client = session.client('lambda')
    
    try:
        # Lire le contenu du fichier ZIP
        with open(zip_file, 'rb') as f:
            zip_content = f.read()
        
        try:
            # Vérifier si la fonction existe déjà
            lambda_client.get_function(FunctionName=function_name)
            
            # Mettre à jour la fonction existante
            response = lambda_client.update_function_code(
                FunctionName=function_name,
                ZipFile=zip_content
            )
            
            logger.info(f"Fonction Lambda {function_name} mise à jour avec succès")
        except lambda_client.exceptions.ResourceNotFoundException:
            # Créer une nouvelle fonction
            response = lambda_client.create_function(
                FunctionName=function_name,
                Runtime='python3.9',
                Role=role_arn,
                Handler=handler,
                Code={'ZipFile': zip_content},
                Timeout=timeout,
                MemorySize=memory_size
            )
            
            logger.info(f"Fonction Lambda {function_name} créée avec succès")
        
        return True
    except Exception as e:
        logger.error(f"Erreur lors de la création/mise à jour de la fonction Lambda: {e}")
        return False

def create_lambda_schedule(session, function_name, schedule_expression):
    """
    Crée une règle EventBridge pour déclencher la fonction Lambda selon un calendrier.
    
    Args:
        session (boto3.Session): Session AWS
        function_name (str): Nom de la fonction Lambda
        schedule_expression (str): Expression cron ou rate (ex: 'rate(1 day)' ou 'cron(0 12 * * ? *)')
    
    Returns:
        bool: True si la règle a été créée avec succès, False sinon
    """
    events_client = session.client('events')
    lambda_client = session.client('lambda')
    
    try:
        # Créer la règle EventBridge
        rule_name = f"{function_name}-schedule"
        events_client.put_rule(
            Name=rule_name,
            ScheduleExpression=schedule_expression,
            State='ENABLED'
        )
        
        # Obtenir l'ARN de la fonction Lambda
        lambda_arn = lambda_client.get_function(FunctionName=function_name)['Configuration']['FunctionArn']
        
        # Ajouter la permission à la fonction Lambda
        try:
            lambda_client.add_permission(
                FunctionName=function_name,
                StatementId=f"{rule_name}-permission",
                Action='lambda:InvokeFunction',
                Principal='events.amazonaws.com',
                SourceArn=events_client.describe_rule(Name=rule_name)['Arn']
            )
        except lambda_client.exceptions.ResourceConflictException:
            # La permission existe déjà
            pass
        
        # Ajouter la cible à la règle
        events_client.put_targets(
            Rule=rule_name,
            Targets=[
                {
                    'Id': '1',
                    'Arn': lambda_arn
                }
            ]
        )
        
        logger.info(f"Règle de planification {rule_name} créée avec succès pour la fonction Lambda {function_name}")
        return True
    except Exception as e:
        logger.error(f"Erreur lors de la création de la règle de planification: {e}")
        return False

def setup_lambda_for_scraper():
    """
    Configure une fonction Lambda pour le scraper Welcome to the Jungle.
    
    Returns:
        bool: True si la configuration a réussi, False sinon
    """
    # Créer la session AWS
    session = boto3.Session(
        aws_access_key_id=os.getenv('KEY_ACCESS'),
        aws_secret_access_key=os.getenv('KEY_SECRET')
    )
    
    # Définir les paramètres
    function_name = 'welcome-jungle-scraper'
    source_dir = 'src/data_collection/scrapers'
    output_path = 'deployment/welcome_jungle_lambda.zip'
    handler = 'welcome_jungle_improved.lambda_handler'
    role_arn = os.getenv('LAMBDA_ROLE_ARN')
    
    # Créer le package de déploiement
    if not create_lambda_deployment_package(source_dir, output_path):
        return False
    
    # Créer la fonction Lambda
    if not create_lambda_function(session, function_name, output_path, handler, role_arn):
        return False
    
    # Créer la règle de planification (exécution quotidienne à minuit)
    if not create_lambda_schedule(session, function_name, 'cron(0 0 * * ? *)'):
        return False
    
    logger.info(f"Configuration Lambda terminée avec succès pour la fonction {function_name}")
    return True

if __name__ == "__main__":
    # Test de configuration Lambda
    setup_lambda_for_scraper()
