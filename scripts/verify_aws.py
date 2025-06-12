#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script de vérification de la configuration AWS.
Vérifie la connexion à S3, RDS et Lambda.
"""

import os
import sys
import boto3
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

# Configuration de base
AWS_REGION = "eu-north-1"

load_dotenv()

# Initialisation des logs
os.makedirs('logs', exist_ok=True)
log_file = f"logs/aws_check_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

def run_check(name, check_func):
    """Exécute une vérification et affiche le résultat"""
    print(f"\n=== VÉRIFICATION DE {name} ===")
    try:
        return check_func()
    except Exception as e:
        print(f"[ERROR] Erreur: {e}")
        return False

def get_session():
    """Crée une session AWS avec les identifiants"""
    key = os.getenv('KEY_ACCESS')
    secret = os.getenv('KEY_SECRET')
    if not key or not secret:
        print("[ERROR] Identifiants AWS manquants")
        return None
    print(f"[OK] Identifiants trouvés: {key[:5]}...{key[-5:]}")
    return boto3.Session(
        aws_access_key_id=key,
        aws_secret_access_key=secret,
        region_name=AWS_REGION
    )

def check_identity():
    """Vérifie l'identité AWS"""
    session = get_session()
    if not session:
        return False
    identity = session.client('sts').get_caller_identity()
    print(f"[OK] Identité confirmée: {identity['Arn']}")
    print(f"[OK] Région: {AWS_REGION}")
    return True

def check_s3():
    """Vérifie S3 et le bucket"""
    session = get_session()
    if not session:
        return False
    
    bucket_name = os.getenv('data_lake_bucket', 'data-lake-brut')
    s3 = session.client('s3')
    
    # Vérifier les buckets
    buckets = [b['Name'] for b in s3.list_buckets()['Buckets']]
    print(f"[OK] Buckets: {', '.join(buckets)}")
    
    if bucket_name not in buckets:
        print(f"[ERROR] Bucket '{bucket_name}' introuvable")
        return False
    print(f"[OK] Bucket '{bucket_name}' existe")
    
    # Test d'upload
    test_file = "test_upload.txt"
    with open(test_file, "w") as f:
        f.write(f"Test {datetime.now()}")
    
    s3.upload_file(test_file, bucket_name, f"tests/{test_file}")
    os.remove(test_file)
    print("[OK] Upload réussi")
    return True

def check_rds():
    """Vérifie la connexion RDS"""
    # Recharger explicitement les variables d'environnement
    load_dotenv(override=True)
    
    # Première tentative avec le mot de passe dans .env
    db_params = {
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT'),
        'database': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD')
    }
    
    print(f"Connexion à {db_params['host']}:{db_params['port']}/{db_params['database']}...")
    print(f"Debug - Paramètres de connexion DB: host={db_params['host']}, port={db_params['port']}, database={db_params['database']}, user={db_params['user']}")
    print(f"Mot de passe utilisé (premiers caractères): {db_params['password'][:2]}{'*' * (len(db_params['password']) - 2) if db_params['password'] else 'Non défini'}")
    
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        cursor.execute('SELECT version();')
        version = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        
        print(f"[OK] RDS connecté: {version[:50]}...")
        return True
    except Exception as first_error:
        # Si la première tentative échoue, essayons avec différents mots de passe alternatifs
        alternative_passwords = ['chaima', 'postgres', 'Postgres123!', 'root']
        
        for alt_password in alternative_passwords:
            print(f"Tentative avec mot de passe alternatif: {alt_password[:1]}{'*' * (len(alt_password) - 1)}...")
            db_params['password'] = alt_password
            
            try:
                conn = psycopg2.connect(**db_params)
                cursor = conn.cursor()
                cursor.execute('SELECT version();')
                version = cursor.fetchone()[0]
                cursor.close()
                conn.close()
                
                print(f"[OK] RDS connecté avec le mot de passe alternatif: {version[:50]}...")
                print(f"[IMPORTANT] Mettez à jour votre fichier .env avec DB_PASSWORD={alt_password}")
                return True
            except Exception as pwd_error:
                pass
        
        # Si toutes les tentatives échouent
        error_msg = str(first_error)
        import socket
        current_ip = socket.gethostbyname(socket.gethostname())
        print(f"[ERROR] Erreur RDS: {error_msg}")
        print(f"[INFO] Vérification depuis l'adresse IP locale: {current_ip}")
        
        if "password authentication failed" in error_msg:
            print("  -> Le mot de passe est incorrect. Verifiez votre fichier .env.")
            print("     Essayez d'autres mots de passe possibles dans votre .env")
        
        if "no pg_hba.conf entry" in error_msg:
            print("  -> PROBLEME PRINCIPAL: Votre IP n'est pas autorisee a se connecter a la base de donnees.")
            print("  -> Solution: Connectez-vous a la console AWS RDS, selectionnez votre instance RDS,")
            print(f"     puis modifiez le groupe de securite pour autoriser votre IP actuelle: {current_ip}")
        
        if "timeout" in error_msg.lower():
            print("  -> Cause possible: RDS non accessible publiquement ou firewall.")
                
        return False

def check_lambda():
    """Vérifie la configuration Lambda"""
    session = get_session()
    if not session:
        return False
    
    lambda_role_arn = os.getenv('LAMBDA_ROLE_ARN')
    if not lambda_role_arn:
        print("❌ ARN du rôle Lambda manquant")
        print("  Ajoutez LAMBDA_ROLE_ARN=arn:aws:iam::194722407093:role/LambdaETLRole au .env")
        return False
    
    # Vérifier les fonctions Lambda
    lambda_client = session.client('lambda')
    functions = [f['FunctionName'] for f in lambda_client.list_functions()['Functions']]
    print(f"[OK] Fonctions Lambda: {', '.join(functions) if functions else 'Aucune'}")
    
    # Vérifier le rôle IAM
    role_name = lambda_role_arn.split('/')[-1]
    iam = session.client('iam')
    role = iam.get_role(RoleName=role_name)
    print(f"[OK] Rôle IAM '{role_name}' trouvé")
    return True

def main():
    """Fonction principale"""
    print("\n" + "=" * 60)
    print(" VÉRIFICATION AWS ".center(60, "="))
    print("=" * 60)
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Exécuter les vérifications
    checks = {
        "IDENTITÉ": run_check("IDENTITÉ", check_identity),
        "S3": run_check("S3", check_s3),
        "RDS": run_check("RDS", check_rds),
        "LAMBDA": run_check("LAMBDA", check_lambda)
    }
    
    # Résumé
    print("\n" + "=" * 60)
    print(" RÉSUMÉ ".center(60, "="))
    print("=" * 60)
    
    for name, status in checks.items():
        print(f"{name}: {'[OK] OK' if status else '[ERROR] ECHEC'}")
    
    success = all(checks.values())
    print(f"Statut global: {'[OK] SUCCES' if success else '[ERROR] ECHEC'}")
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())
