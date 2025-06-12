#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Utilitaire pour charger les variables d'environnement depuis un fichier .env
"""

import os
import logging

logger = logging.getLogger(__name__)

def load_dotenv(env_file=".env"):
    """
    Charge les variables d'environnement depuis un fichier .env
    
    Args:
        env_file (str): Chemin vers le fichier .env
        
    Returns:
        bool: True si le fichier a été chargé avec succès, False sinon
    """
    try:
        if not os.path.exists(env_file):
            logger.warning(f"Fichier {env_file} non trouvé")
            return False
        
        logger.info(f"Chargement des variables d'environnement depuis {env_file}")
        with open(env_file, 'r') as f:
            for line in f:
                line = line.strip()
                
                # Ignorer les lignes vides et les commentaires
                if not line or line.startswith('#'):
                    continue
                
                # Parser les lignes de type KEY=VALUE
                if '=' in line:
                    key, value = line.split('=', 1)
                    key = key.strip()
                    value = value.strip()
                    
                    # Supprimer les guillemets si présents
                    if value.startswith('"') and value.endswith('"'):
                        value = value[1:-1]
                    elif value.startswith("'") and value.endswith("'"):
                        value = value[1:-1]
                    
                    # Définir la variable d'environnement
                    os.environ[key] = value
                    logger.debug(f"Variable d'environnement définie: {key}")
        
        logger.info("Variables d'environnement chargées avec succès")
        return True
    
    except Exception as e:
        logger.error(f"Erreur lors du chargement des variables d'environnement: {e}")
        return False

if __name__ == "__main__":
    # Configuration du logging pour les tests
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Test de chargement
    load_dotenv()
    
    # Afficher quelques variables importantes
    aws_access = os.environ.get('KEY_ACCESS')
    aws_secret = os.environ.get('KEY_SECRET')
    
    if aws_access:
        print("Variable KEY_ACCESS définie correctement")
    else:
        print("Variable KEY_ACCESS non définie")
    
    if aws_secret:
        print("Variable KEY_SECRET définie correctement")
    else:
        print("Variable KEY_SECRET non définie")
