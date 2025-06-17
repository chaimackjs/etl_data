
import os
import sys
import logging
import argparse
import subprocess
from datetime import datetime
from dotenv import load_dotenv

# Ajouter le répertoire parent au chemin Python
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Configuration du logging
os.makedirs('logs', exist_ok=True)
log_file = f"logs/pipeline_execution_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Charger les variables d'environnement
load_dotenv()

def parse_arguments():
    """Parse les arguments de ligne de commande."""
    parser = argparse.ArgumentParser(description='Pipeline de collecte et analyse d\'offres d\'emploi')
    parser.add_argument('--action', choices=['scrape', 'etl', 'analyze', 'all'], default='all',
                        help='Action à exécuter (scrape, etl, analyze, all)')
    parser.add_argument('--source', choices=['welcome_jungle', 'france_travail', 'all'], default='welcome_jungle',
                        help='Source des données (welcome_jungle, france_travail, all)')
    parser.add_argument('--terms', type=str, help='Termes de recherche séparés par des virgules')
    parser.add_argument('--pages', type=int, default=3, help='Nombre maximum de pages à scraper par terme')
    parser.add_argument('--no-s3', action='store_true', help='Ne pas utiliser S3')
    parser.add_argument('--no-rds', action='store_true', help='Ne pas utiliser RDS')
    parser.add_argument('--save-locally', action='store_true', 
                        help='Sauvegarder les données en local (désactivé par défaut)')
    parser.add_argument('--force', action='store_true',
                        help='Forcer la collecte et le traitement même si les données existent déjà')
    return parser.parse_args()

def check_aws_configuration():
    """Vérifie la configuration AWS avant de démarrer le pipeline."""
    logger.info("Vérification de la configuration AWS...")
    
    # Exécuter le script de vérification AWS directement
    print("\nVérification de la configuration AWS avant de démarrer le pipeline...\n")
    
    try:
        # Exécuter le script de déploiement d'infrastructure AWS en mode direct (pas de capture)
        # pour permettre l'affichage en temps réel
        aws_check_script = os.path.join('scripts', 'utils', 'deploy_infrastructure_aws.py')
        
        if not os.path.exists(aws_check_script):
            logger.error(f"Script de vérification AWS non trouvé: {aws_check_script}")
            print(f"❌ Script de vérification AWS non trouvé: {aws_check_script}")
            return False
        
        logger.info(f"Exécution du script de vérification AWS: {aws_check_script}")
        result = subprocess.run([sys.executable, aws_check_script])
        
        if result.returncode != 0:
            logger.error(f"La vérification AWS a échoué avec le code de sortie {result.returncode}")
            print("\n\033[91m\033[1m✖ ERREUR: \033[0m\033[91mLa configuration AWS n'est pas correcte.\033[0m\n\033[3mVeuillez corriger les erreurs avant de continuer.\033[0m")
            return False
        
        logger.info("Vérification AWS réussie")
        print("\n\033[92m\033[1m✓ SUCCÈS: \033[0m\033[92mLa configuration AWS est correcte.\033[0m\n\033[1m\033[96mLe pipeline est prêt à démarrer!\033[0m")
        return True
    
    except Exception as e:
        logger.error(f"Erreur lors de la vérification AWS: {e}")
        print(f"\n\033[91m\033[1m⚠ ALERTE: \033[0m\033[91mErreur lors de la vérification AWS\033[0m\n\033[3m{e}\033[0m")
        return False

def print_header(title):
    """
    Affiche un en-tête bien visible pour chaque étape du pipeline avec un style moderne.
    
    Args:
        title (str): Titre de l'étape
    """
    # Utilisation de caractères Unicode pour un style plus moderne
    width = 60
    stars = "★" * 3
    separator = "═" * width
    
    # Couleurs ANSI pour le terminal (ne fonctionne que sur certains terminaux)
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BOLD = '\033[1m'
    RESET = '\033[0m'
    
    logger.info(f"\n{BLUE}{separator}{RESET}")
    logger.info(f"{YELLOW}{stars} {BOLD}{title}{RESET} {YELLOW}{stars}{RESET}")
    logger.info(f"{GREEN}{separator}{RESET}\n")

def main():
    """Fonction principale du pipeline."""
    # Enregistrer l'heure de début pour calculer le temps d'exécution
    start_time = datetime.now()
    # Parser les arguments
    args = parse_arguments()
    
    # Affichage d'un header moderne avec ASCII art
    print("\n\033[1;36m")
    print("  ╔══════════════════════════════════════════════════════════╗")
    print("  ║                                                      ║")
    print("  ║   ✨ PIPELINE DE COLLECTE ET ANALYSE D'EMPLOI ✨    ║")
    print("  ║                                                      ║")
    print("  ╚══════════════════════════════════════════════════════════╝")
    print("\033[0m")
    
    # Affichage de la version et de la date
    current_date = datetime.now().strftime("%d/%m/%Y %H:%M")
    print(f"\033[90m  Version 2.0 | {current_date} | Exécution: {os.getenv('USER', 'utilisateur')}\033[0m\n")
    
    # Créer les dossiers nécessaires
    os.makedirs('logs', exist_ok=True)
    
    # Créer les dossiers de données brutes seulement si on sauvegarde en local
    if args.save_locally:
        os.makedirs('data/raw/welcome_jungle', exist_ok=True)
        os.makedirs('data/raw/france_travail', exist_ok=True)
    
    # Toujours créer les dossiers pour les données traitées et les rapports
    os.makedirs('data/processed/welcome_jungle', exist_ok=True)
    os.makedirs('data/processed/france_travail', exist_ok=True)
    os.makedirs('reports/figures', exist_ok=True)
    
    # Vérifier la configuration AWS avant de démarrer le pipeline
    if not check_aws_configuration():
        logger.error("La vérification de la configuration AWS a échoué. Arrêt du pipeline.")
        return 1
    
    # Afficher les informations de configuration
    logger.info("Démarrage du pipeline de collecte et analyse d'offres d'emploi")
    logger.info(f"Action: {args.action}")
    logger.info(f"Source: {args.source}")
    
    # Définir les termes de recherche
    search_terms = args.terms.split(',') if args.terms else ["data scientist", "data engineer", "data analyst"]
    logger.info(f"Termes de recherche: {search_terms}")
    
    # Exécuter les actions demandées
    if args.action in ['scrape', 'all']:
        print_header("ÉTAPE 1: COLLECTE DES DONNÉES")
        
        # Paramètres communs pour les scripts de collecte
        script_params = []
        if args.no_s3:
            script_params.append("--no-s3")
        if args.save_locally:
            script_params.append("--save-locally")
        
        # Collecte des données Welcome to the Jungle
        if args.source in ['welcome_jungle', 'all']:
            logger.info("Collecte des données depuis Welcome to the Jungle")
            
            welcome_jungle_script = os.path.join('scripts', 'scraping_api', 'collect_welcome_jungle_jobs.py')
            
            for term in search_terms:
                logger.info(f"Collecte des offres pour le terme: {term}")
                cmd = [sys.executable, welcome_jungle_script, f"--query={term}", f"--max-pages={args.pages}", "--headless"]
                cmd.extend(script_params)
                
                try:
                    logger.info(f"Exécution de la commande: {' '.join(cmd)}")
                    result = subprocess.run(cmd, check=True)
                    logger.info(f"Collecte terminée pour '{term}' avec code de sortie {result.returncode}")
                except subprocess.CalledProcessError as e:
                    logger.error(f"Erreur lors de la collecte pour '{term}': {e}")
        
        # Collecte des données France Travail
        if args.source in ['france_travail', 'all']:
            logger.info("Collecte des données depuis France Travail (Pôle Emploi)")
            
            france_travail_script = os.path.join('scripts', 'scraping_api', 'collect_all_france_travail_jobs.py')
            cmd = [sys.executable, france_travail_script, f"--max-pages={args.pages}", "--delay=1.0"]
            cmd.extend(script_params)
            
            try:
                logger.info(f"Exécution de la commande: {' '.join(cmd)}")
                result = subprocess.run(cmd, check=True)
                logger.info(f"Collecte terminée avec code de sortie {result.returncode}")
            except subprocess.CalledProcessError as e:
                logger.error(f"Erreur lors de la collecte depuis France Travail: {e}")

    
    if args.action in ['etl', 'all']:
        print_header("ÉTAPE 2: TRAITEMENT DES DONNÉES (ETL)")
        
        # Paramètres communs pour les scripts ETL
        etl_params = []
        if args.force:
            etl_params.append("--force")
        
        if args.source in ['welcome_jungle', 'all']:
            # Exécuter le script ETL Welcome to the Jungle
            logger.info("Exécution du pipeline ETL pour Welcome to the Jungle")
            welcome_jungle_etl_cmd = [
                sys.executable,
                "scripts/pipeline/run_etl_welcome_jungle.py",
                "--all"
            ]
            welcome_jungle_etl_cmd.extend(etl_params)
            
            try:
                logger.info(f"Exécution de la commande: {' '.join(welcome_jungle_etl_cmd)}")
                result = subprocess.run(welcome_jungle_etl_cmd, check=True)
                logger.info(f"Traitement ETL Welcome to the Jungle terminé avec code de sortie {result.returncode}")
            except subprocess.CalledProcessError as e:
                logger.error(f"Erreur lors du traitement ETL pour Welcome to the Jungle: {e}")
        
        if args.source in ['france_travail', 'all']:
            # Exécuter le script ETL France Travail
            logger.info("Exécution du pipeline ETL pour France Travail")
            france_travail_etl_cmd = [
                sys.executable,
                "scripts/pipeline/run_etl_france_travail.py",
                "--all"
            ]
            france_travail_etl_cmd.extend(etl_params)
            
            try:
                logger.info(f"Exécution de la commande: {' '.join(france_travail_etl_cmd)}")
                result = subprocess.run(france_travail_etl_cmd, check=True)
                logger.info(f"Traitement ETL France Travail terminé avec code de sortie {result.returncode}")
            except subprocess.CalledProcessError as e:
                logger.error(f"Erreur lors du traitement ETL pour France Travail: {e}")
    
    if args.action in ['analyze', 'all']:
        print_header("ÉTAPE 3: ANALYSE DES DONNÉES")
        
        # Paramètres communs pour les scripts d'analyse
        analysis_params = []
        if args.force:
            analysis_params.append("--force")
        
        # Créer le dossier pour les visualisations
        os.makedirs("data/analysis/visualizations", exist_ok=True)
        
        # Exécuter le script d'analyse France Travail
        if args.source in ['france_travail', 'all']:
            logger.info("\n\033[95m••• ANALYSE DES DONNÉES FRANCE TRAVAIL •••\033[0m")
            logger.info("\033[90m" + "─" * 50 + "\033[0m")
            france_travail_analysis_cmd = [
                sys.executable,
                "scripts/analyse/analyze_job_france_travai.py"
            ]
            france_travail_analysis_cmd.extend(analysis_params)
            
            try:
                logger.info(f"Exécution de la commande: {' '.join(france_travail_analysis_cmd)}")
                result = subprocess.run(france_travail_analysis_cmd, check=True)
                logger.info(f"Analyse France Travail terminée avec code de sortie {result.returncode}")
            except subprocess.CalledProcessError as e:
                logger.error(f"Erreur lors de l'analyse France Travail: {e}")
        
        # Exécuter le script d'analyse Welcome to the Jungle
        if args.source in ['welcome_jungle', 'all']:
            logger.info("\n\033[95m••• ANALYSE DES DONNÉES WELCOME TO THE JUNGLE •••\033[0m")
            logger.info("\033[90m" + "─" * 50 + "\033[0m")
            welcome_jungle_analysis_cmd = [
                sys.executable,
                "src/analyse/run_welcome_jungle_analysis.py",
                "--source", "db",
                "--open-html"
            ]
            welcome_jungle_analysis_cmd.extend(analysis_params)
            
            try:
                logger.info(f"Exécution de la commande: {' '.join(welcome_jungle_analysis_cmd)}")
                result = subprocess.run(welcome_jungle_analysis_cmd, check=True)
                logger.info(f"Analyse Welcome to the Jungle terminée avec code de sortie {result.returncode}")
            except subprocess.CalledProcessError as e:
                logger.error(f"Erreur lors de l'analyse Welcome to the Jungle: {e}")
        
        # Exécuter les analyses avancées
        if args.source in ['france_travail', 'all']:
            # Analyse des corrélations
            logger.info("\n\033[95m••• ANALYSE DES CORRÉLATIONS ENTRE TECHNOLOGIES ET TYPES DE CONTRAT •••\033[0m")
            logger.info("\033[90m" + "─" * 50 + "\033[0m")
            correlations_cmd = [
                sys.executable,
                "scripts/analyse/analyze_correlations.py"
            ]
            correlations_cmd.extend(analysis_params)
            
            try:
                logger.info(f"Exécution de la commande: {' '.join(correlations_cmd)}")
                result = subprocess.run(correlations_cmd, check=True)
                logger.info(f"Analyse des corrélations terminée avec code de sortie {result.returncode}")
            except subprocess.CalledProcessError as e:
                logger.error(f"Erreur lors de l'analyse des corrélations: {e}")
            
            # Analyse géographique
            logger.info("\n\033[95m••• ANALYSE GÉOGRAPHIQUE DES OFFRES D'EMPLOI •••\033[0m")
            logger.info("\033[90m" + "─" * 50 + "\033[0m")
            geography_cmd = [
                sys.executable,
                "scripts/analyse/analyze_geography.py"
            ]
            geography_cmd.extend(analysis_params)
            
            try:
                logger.info(f"Exécution de la commande: {' '.join(geography_cmd)}")
                result = subprocess.run(geography_cmd, check=True)
                logger.info(f"Analyse géographique terminée avec code de sortie {result.returncode}")
            except subprocess.CalledProcessError as e:
                logger.error(f"Erreur lors de l'analyse géographique: {e}")
            
            # Analyse des salaires
            logger.info("\n\033[95m••• ANALYSE DES SALAIRES DES OFFRES D'EMPLOI •••\033[0m")
            logger.info("\033[90m" + "─" * 50 + "\033[0m")
            salary_cmd = [
                sys.executable,
                "scripts/analyse/analyze_salary.py"
            ]
            salary_cmd.extend(analysis_params)
            
            try:
                logger.info(f"Exécution de la commande: {' '.join(salary_cmd)}")
                result = subprocess.run(salary_cmd, check=True)
                logger.info(f"Analyse des salaires terminée avec code de sortie {result.returncode}")
            except subprocess.CalledProcessError as e:
                logger.error(f"Erreur lors de l'analyse des salaires: {e}")
            
            # Analyse temporelle
            logger.info("\n\033[95m••• ANALYSE DE L'ÉVOLUTION TEMPORELLE DES OFFRES D'EMPLOI •••\033[0m")
            logger.info("\033[90m" + "─" * 50 + "\033[0m")
            temporal_cmd = [
                sys.executable,
                "scripts/analyse/analyze_temporal.py"
            ]
            temporal_cmd.extend(analysis_params)
            
            try:
                logger.info(f"Exécution de la commande: {' '.join(temporal_cmd)}")
                result = subprocess.run(temporal_cmd, check=True)
                logger.info(f"Analyse temporelle terminée avec code de sortie {result.returncode}")
            except subprocess.CalledProcessError as e:
                logger.error(f"Erreur lors de l'analyse temporelle: {e}")
        
        # Fin des analyses
    
    # Affichage d'un message de fin moderne
    print("\n\033[42m\033[1;30m  PIPELINE TERMINÉ AVEC SUCCÈS  \033[0m")
    
    # Calcul du temps d'exécution
    end_time = datetime.now()
    execution_time = end_time - start_time
    minutes, seconds = divmod(execution_time.seconds, 60)
    
    print(f"\n\033[92m⌛ Temps d'exécution: {minutes}m {seconds}s\033[0m")
    print("\033[93mℹ Consultez les logs pour plus de détails\033[0m")
    
    # ASCII art de fin
    print("\n\033[1;36m")
    print("  ┌───────────────────────────────────────┐")
    print("  │                                       │")
    print("  │   Données collectées et analysées    │")
    print("  │   avec succès! ✅                    │")
    print("  │                                       │")
    print("  └───────────────────────────────────────┘")
    print("\033[0m\n")
    return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
