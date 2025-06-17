#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script pour créer un rôle IAM pour Lambda avec les permissions nécessaires.
"""

import os
import json
import boto3
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

# Récupérer les identifiants AWS
aws_access_key = os.getenv('KEY_ACCESS')
aws_secret_key = os.getenv('KEY_SECRET')

if not aws_access_key or not aws_secret_key:
    print("❌ Identifiants AWS manquants. Vérifiez les variables d'environnement KEY_ACCESS et KEY_SECRET.")
    exit(1)

# Nom du rôle à créer
role_name = "LambdaETLRole"

# Document de politique d'approbation pour Lambda
trust_policy = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "lambda.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}

try:
    # Créer une session boto3
    session = boto3.Session(
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key
    )
    iam = session.client('iam')
    
    print(f"Création du rôle IAM '{role_name}' pour Lambda...")
    
    # Créer le rôle
    response = iam.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=json.dumps(trust_policy),
        Description="Rôle pour les fonctions Lambda ETL du projet de collecte d'offres d'emploi"
    )
    
    role_arn = response['Role']['Arn']
    print(f"✅ Rôle créé avec succès: {role_arn}")
    
    # Attacher les politiques nécessaires
    policies = [
        "arn:aws:iam::aws:policy/AmazonS3FullAccess",  # Accès à S3
        "arn:aws:iam::aws:policy/AmazonRDSFullAccess",  # Accès à RDS
        "arn:aws:iam::aws:policy/CloudWatchLogsFullAccess"  # Logs CloudWatch
    ]
    
    for policy_arn in policies:
        policy_name = policy_arn.split('/')[-1]
        print(f"Attachement de la politique {policy_name}...")
        iam.attach_role_policy(
            RoleName=role_name,
            PolicyArn=policy_arn
        )
        print(f"✅ Politique {policy_name} attachée avec succès")
    
    print("\n=== CONFIGURATION DU RÔLE LAMBDA TERMINÉE ===")
    print(f"Nom du rôle: {role_name}")
    print(f"ARN du rôle: {role_arn}")
    print("\nAjoutez la ligne suivante à votre fichier .env:")
    print(f"LAMBDA_ROLE_ARN={role_arn}")
    
except Exception as e:
    print(f"❌ Erreur lors de la création du rôle IAM: {e}")
    exit(1)
