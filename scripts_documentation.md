# Documentation des Scripts du Projet ETL et Analyse de Données d'Emploi

## Vue d'ensemble

Ce document présente tous les scripts disponibles dans le projet ETL et Analyse de Données d'Emploi, organisés par module fonctionnel. Chaque script est décrit avec son objectif, ses fonctionnalités principales et son utilisation.

## Structure des Scripts

Le projet est organisé en plusieurs modules fonctionnels :

1. **Extraction de données** : Scripts pour collecter les données depuis Welcome to the Jungle et France Travail
2. **Pipeline ETL** : Scripts pour transformer et charger les données
3. **Analyse** : Scripts pour analyser et visualiser les données
4. **Tests** : Scripts pour tester les différentes fonctionnalités
5. **Infrastructure** : Scripts pour configurer l'infrastructure AWS
6. **Utilitaires** : Scripts d'aide et de configuration

## 1. Scripts d'Extraction de Données

### 1.1. Welcome to the Jungle Scraper
**Fichier** : `scripts/scraping_api/collect_welcome_jungle_jobs.py`

**Description** : Script de scraping pour extraire les offres d'emploi depuis welcometothejungle.com.

**Fonctionnalités principales** :
- Extraction des offres d'emploi avec Selenium
- Collecte des informations détaillées (titre, entreprise, lieu, type de contrat, etc.)
- Stockage des données au format JSON
- Upload des données vers AWS S3
- Vérification des données déjà collectées pour éviter les duplications

**Utilisation** :
```bash
python scripts/scraping_api/collect_welcome_jungle_jobs.py --query "data scientist" --location "Paris" --max-pages 5 --headless --save-locally
```

**Options** :
- `--query` : Terme de recherche (vide pour toutes les offres)
- `--location` : Localisation (vide pour toutes les localisations)
- `--max-pages` : Nombre maximum de pages à récupérer (défaut: 10)
- `--headless` : Exécuter le navigateur en mode headless
- `--no-s3` : Ne pas télécharger les fichiers vers S3
- `--save-locally` : Sauvegarder les données en local (désactivé par défaut)
- `--force` : Forcer la collecte même si des données ont déjà été collectées aujourd'hui

### 1.2. France Travail API
**Fichier** : `scripts/scraping_api/collect_all_france_travail_jobs.py`

**Description** : Script pour extraire les offres d'emploi depuis l'API France Travail (Pôle Emploi).

**Fonctionnalités principales** :
- Authentification à l'API France Travail
- Recherche d'offres d'emploi par mots-clés
- Pagination des résultats
- Stockage des données au format JSON
- Upload des données vers AWS S3
- Gestion des erreurs et des limites de l'API

**Utilisation** :
```bash
python scripts/scraping_api/collect_all_france_travail_jobs.py --query "data scientist" --max-pages 5 --save-locally
```

**Options** :
- `--query` : Terme de recherche (obligatoire)
- `--max-pages` : Nombre maximum de pages à récupérer (défaut: 10)
- `--save-locally` : Sauvegarder les données en local (désactivé par défaut)
- `--no-s3` : Ne pas télécharger les fichiers vers S3
- `--force` : Forcer la collecte même si des données ont déjà été collectées aujourd'hui

## 2. Scripts de Pipeline ETL

### 2.1. Welcome to the Jungle ETL

**Fichier** : `scripts/pipeline/run_etl_welcome_jungle.py`

**Description** : Script principal pour exécuter le pipeline ETL Welcome to the Jungle.

**Fonctionnalités principales** :
- Extraction des données depuis AWS S3 ou fichiers locaux
- Transformation et nettoyage des données
- Chargement dans PostgreSQL
- Gestion des erreurs et des doublons
- Possibilité d'exécuter des étapes spécifiques du pipeline

**Utilisation** :
```bash
python scripts/pipeline/run_etl_welcome_jungle.py --all --force
```

**Options** :
- `--extract` : Exécuter uniquement l'étape d'extraction
- `--transform` : Exécuter uniquement l'étape de transformation
- `--load` : Exécuter uniquement l'étape de chargement
- `--all` : Exécuter toutes les étapes du pipeline
- `--force` : Force le chargement des données même si elles existent déjà
- `--force-download` : Force le téléchargement des fichiers même s'ils existent déjà localement
- `--file` : Fichier spécifique à traiter (optionnel)

### 2.2. France Travail ETL

**Fichier** : `scripts/pipeline/run_etl_france_travail.py`

**Description** : Script principal pour exécuter le pipeline ETL France Travail.

**Fonctionnalités principales** :
- Extraction des données depuis AWS S3 ou fichiers locaux
- Transformation et nettoyage des données
- Chargement dans PostgreSQL
- Gestion des erreurs et des doublons
- Enrichissement des données (détection des technologies, normalisation des lieux, etc.)

**Utilisation** :
```bash
python scripts/pipeline/run_etl_france_travail.py --all --force
```

**Options** :
- `--extract` : Exécuter uniquement l'étape d'extraction
- `--transform` : Exécuter uniquement l'étape de transformation
- `--load` : Exécuter uniquement l'étape de chargement
- `--all` : Exécuter toutes les étapes du pipeline
- `--force` : Force le chargement des données même si elles existent déjà
- `--force-download` : Force le téléchargement des fichiers même s'ils existent déjà localement
- `--file` : Fichier spécifique à traiter (optionnel)

### 2.3. Utilitaires ETL

#### 2.3.1. Recréation de Table
**Fichier** : `scripts/utils/recreate_table.py`

**Description** : Utilitaire pour recréer une table dans la base de données.

**Fonctionnalités principales** :
- Suppression d'une table existante
- Recréation du schéma avec les bonnes colonnes
- Correction des problèmes de schéma

**Utilisation** :
```bash
python scripts/utils/recreate_table.py --table welcome_jungle_jobs
```

## 3. Scripts d'Analyse

### 3.1. Welcome to the Jungle Analysis
**Fichier** : `src/analyse/welcome_jungle_analysis.py`

**Description** : Module d'analyse des offres d'emploi Welcome to the Jungle.

**Fonctionnalités principales** :
- Analyse des types de contrat
- Analyse des localisations
- Analyse des salaires
- Analyse des niveaux d'expérience
- Analyse des technologies demandées
- Génération de visualisations
- Création d'un rapport HTML

### 3.2. Runner d'Analyse Welcome to the Jungle
**Fichier** : `src/analyse/run_welcome_jungle_analysis.py`

**Description** : Script d'interface en ligne de commande pour exécuter l'analyse.

**Fonctionnalités principales** :
- Chargement des données depuis PostgreSQL ou CSV
- Exécution de l'analyse complète
- Génération du rapport HTML
- Options pour ouvrir automatiquement le rapport

**Utilisation** :
```bash
python src/analyse/run_welcome_jungle_analysis.py --source db --open-html
```

### 3.3. Analyse Générale des Offres d'Emploi
**Fichier** : `src/analyse/job_analysis.py`

**Description** : Module d'analyse générale des offres d'emploi.

**Fonctionnalités principales** :
- Analyse comparative entre différentes sources
- Statistiques globales sur le marché de l'emploi

## 4. Scripts d'Infrastructure

### 4.1. Configuration S3
**Fichier** : `src/infrastructure/s3_setup.py`

**Description** : Script pour configurer les buckets S3.

**Fonctionnalités principales** :
- Création des buckets S3
- Configuration des permissions
- Vérification de l'existence des buckets

### 4.2. Configuration RDS
**Fichier** : `src/infrastructure/rds_setup.py`

**Description** : Script pour configurer la base de données RDS.

**Fonctionnalités principales** :
- Création de l'instance RDS
- Configuration des paramètres de sécurité
- Initialisation du schéma de la base de données

### 4.3. Configuration Lambda
**Fichier** : `src/infrastructure/lambda_setup.py`

**Description** : Script pour configurer les fonctions Lambda.

**Fonctionnalités principales** :
- Création des fonctions Lambda
- Configuration des déclencheurs
- Déploiement du code

## 5. Scripts Utilitaires

### 5.1. Configuration
**Fichier** : `src/utils/config.py`

**Description** : Module de gestion de la configuration.

**Fonctionnalités principales** :
- Chargement des paramètres de configuration
- Validation des paramètres
- Gestion des valeurs par défaut

### 5.2. Logging
**Fichier** : `src/utils/logger.py`

**Description** : Module de gestion des logs.

**Fonctionnalités principales** :
- Configuration du logging
- Formatage des messages
- Rotation des fichiers de log

## 6. Script Principal

### 6.1. Main
**Fichier** : `main.py`

**Description** : Script principal pour exécuter l'ensemble du pipeline.

**Fonctionnalités principales** :
- Interface en ligne de commande unifiée
- Orchestration des différentes étapes du pipeline
- Gestion des erreurs et des logs
- Vérification de la configuration AWS

**Utilisation** :
```bash
python main.py --action all --source all --terms "data scientist,data engineer" --pages 5
```

## Exécution du Pipeline Complet

Pour exécuter le pipeline complet, utilisez le script principal `main.py` avec les options appropriées :

```bash
# Exécuter tout le pipeline pour Welcome to the Jungle
python main.py --action all --source welcome_jungle

# Exécuter tout le pipeline pour France Travail
python main.py --action all --source france_travail

# Exécuter tout le pipeline pour toutes les sources
python main.py --action all --source all

# Exécuter uniquement le scraping
python main.py --action scrape --source all

# Exécuter uniquement l'ETL
python main.py --action etl --source all

# Exécuter uniquement l'analyse
python main.py --action analyze --source all
```

## Conclusion

Cette documentation fournit une vue d'ensemble de tous les scripts disponibles dans le projet ETL et Analyse de Données d'Emploi. Chaque script est conçu pour être modulaire et peut être exécuté indépendamment ou dans le cadre du pipeline complet via le script principal `main.py`.
