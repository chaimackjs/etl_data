# Documentation sur la mise en place du Data Lake et du Data Warehouse

## Architecture globale

Le projet utilise une architecture moderne de traitement de données basée sur AWS avec deux composants principaux :

1. **Data Lake** : Stockage des données brutes dans Amazon S3
2. **Data Warehouse** : Base de données PostgreSQL hébergée sur Amazon RDS

Cette architecture permet de collecter, stocker, transformer et analyser les données d'offres d'emploi provenant de différentes sources.

## Configuration du Data Lake (Amazon S3)

### Informations de configuration

- **Bucket S3** : `data-lake-brut`
- **Région** : `eu-north-1`
- **Structure de dossiers** :
  - `/france_travail` : Données brutes des offres d'emploi France Travail (anciennement Pôle Emploi)
  - `/welcome_jungle` : Données brutes des offres d'emploi Welcome to the Jungle

### Accès au Data Lake

- **Clé d'accès AWS** :
- **Clé secrète AWS** :
- **Utilisateur IAM** : 
- **URL de connexion IAM** : `https://194722407093.signin.aws.amazon.com/console`

### Organisation des données dans le Data Lake

Les données sont organisées selon la structure suivante :

```
data-lake-brut/
├── france_travail/
│   ├── raw/
│   │   └── [fichiers JSON des offres d'emploi]
│   └── processed/
│       └── [fichiers CSV transformés]
└── welcome_jungle/
    ├── raw/
    │   └── [fichiers JSON des offres d'emploi]
    └── processed/
        └── [fichiers CSV transformés]
```

## Configuration du Data Warehouse (Amazon RDS PostgreSQL)

### Informations de connexion

- **Point de terminaison** :
- **Port** :
- **Nom de la base de données** :
- **Utilisateur** :
- **Mot de passe** :

> **Note** : Une seconde configuration existe mais semble avoir des problèmes d'accès :
> - Point de terminaison :
> - Utilisateur :
> - Mot de passe :
> - Base de données :
> - Problème : Connection timeout - Base de données non accessible publiquement

### Structure des tables

#### Table `france_travail_jobs`

Cette table stocke les offres d'emploi de France Travail après transformation.

```sql
CREATE TABLE france_travail_jobs (
    id VARCHAR(255) PRIMARY KEY,
    title TEXT,
    description TEXT,
    company_name TEXT,
    company_description TEXT,
    contract_type TEXT,
    contract_type_std TEXT,
    contract_duration TEXT,
    location TEXT,
    city TEXT,
    region TEXT,
    country TEXT,
    publication_date DATE,
    experience_level TEXT,
    salary TEXT,
    has_python BOOLEAN,
    has_java BOOLEAN,
    has_javascript BOOLEAN,
    has_csharp BOOLEAN,
    has_cpp BOOLEAN,
    has_php BOOLEAN,
    has_ruby BOOLEAN,
    has_sql BOOLEAN,
    has_nosql BOOLEAN,
    has_aws BOOLEAN,
    has_azure BOOLEAN,
    has_gcp BOOLEAN
);
```

#### Table `welcome_jungle_jobs`

Cette table stocke les offres d'emploi de Welcome to the Jungle après transformation.

```sql
CREATE TABLE welcome_jungle_jobs (
    id SERIAL PRIMARY KEY,
    title TEXT,
    company_name TEXT,
    location TEXT,
    city TEXT,
    region TEXT,
    country TEXT,
    contract_type TEXT,
    contract_type_std TEXT,
    publication_date DATE,
    description TEXT,
    has_python BOOLEAN,
    has_java BOOLEAN,
    has_javascript BOOLEAN,
    has_csharp BOOLEAN,
    has_cpp BOOLEAN,
    has_php BOOLEAN,
    has_ruby BOOLEAN,
    has_sql BOOLEAN,
    has_nosql BOOLEAN,
    has_aws BOOLEAN,
    has_azure BOOLEAN,
    has_gcp BOOLEAN
);
```

#### Table `job_skills`

Cette table établit une relation entre les offres d'emploi et les compétences requises.

```sql
CREATE TABLE job_skills (
    id SERIAL PRIMARY KEY,
    job_id VARCHAR(255),
    skill_name TEXT,
    source VARCHAR(50),
    FOREIGN KEY (job_id) REFERENCES france_travail_jobs(id)
);
```

## Processus ETL

### 1. Extraction

- **France Travail** : Les données sont extraites depuis le bucket S3 `data-lake-brut/france_travail/raw/`
- **Welcome to the Jungle** : Les données sont extraites par scraping du site web et stockées dans le bucket S3

### 2. Transformation

Le processus de transformation inclut :

- Normalisation des types de contrat (CDI, CDD, STAGE, ALTERNANCE)
- Extraction et standardisation des données de localisation (ville, région, pays)
- Analyse des technologies mentionnées dans les descriptions d'offres
- Gestion des valeurs manquantes
- Validation de la qualité des données

### 3. Chargement

Les données transformées sont chargées dans la base de données PostgreSQL sur Amazon RDS avec les optimisations suivantes :

- Vérification de l'existence des données avant chargement pour éviter les duplications
- Option `--force` pour forcer le rechargement des données
- Utilisation de `sqlalchemy.text()` pour les requêtes SQL brutes
- Type `Text` pour les colonnes textuelles longues au lieu de `String(255)`

## Configuration de l'environnement

### Variables d'environnement

Un fichier `.env` doit être créé à la racine du projet avec les variables suivantes :

```
# AWS Credentials
KEY_ACCESS=
KEY_SECRET=

# S3 Configuration
DATA_LAKE_BUCKET=

#Lambda Configuration
LAMBDA_ROLE_ARN=

# PostgreSQL Database
DB_HOST=
DB_PORT=
DB_NAME=
DB_USER=
DB_PASSWORD=
```

### Script de configuration

Le script `setup_env.py` permet de configurer facilement toutes les variables nécessaires au pipeline.

## Analyse des données

Une fois les données chargées dans le Data Warehouse, des analyses sont effectuées pour extraire des insights :

1. Distribution des types de contrat
2. Distribution géographique des offres
3. Technologies les plus demandées
4. Durée des contrats CDD

Ces analyses sont visualisées dans un tableau de bord HTML interactif généré par le script `scripts/analysis/analyze_job_data.py`.

## Maintenance et résolution des problèmes

### Recréation des tables

En cas de problème avec la structure des tables, le script `recreate_table.py` permet de recréer les tables avec la structure correcte.

### Optimisation du pipeline

Le pipeline inclut des optimisations pour éviter de relancer inutilement le processus si les données sont déjà chargées. L'option `--force` permet de forcer l'exécution du pipeline même si les données sont déjà présentes.

### Problèmes connus

1. **Connexion RDS** : La base de données `datawarehouses.c32ygg4oyapa.eu-north-1.rds.amazonaws.com` n'est pas accessible publiquement, ce qui provoque des erreurs de timeout.
2. **Authentification API France Travail** : Des erreurs `invalid_client` peuvent survenir lors de l'authentification à l'API.
3. **Scraper Welcome to the Jungle** : Le scraper peut rencontrer des problèmes si la structure HTML du site change.

## Bonnes pratiques

1. **Sécurité** : Ne pas exposer les identifiants AWS et les mots de passe dans le code source.
2. **Sauvegarde** : Effectuer des sauvegardes régulières de la base de données.
3. **Monitoring** : Surveiller l'utilisation des ressources AWS pour éviter des coûts imprévus.
4. **Documentation** : Maintenir cette documentation à jour avec les modifications apportées à l'infrastructure.
