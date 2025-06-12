# Documentation API France Travail (ex-Pôle Emploi)

Cette documentation détaille l'intégration avec l'API France Travail, les scripts créés pour la collecte d'offres d'emploi et comment les utiliser efficacement.

## Table des matières
- [1. Vue d'ensemble](#1-vue-densemble)
- [2. Prérequis](#2-prérequis)
- [3. Structure des scripts](#3-structure-des-scripts)
- [4. Utilisation des scripts](#4-utilisation-des-scripts)
- [5. Fonctionnalités clés](#5-fonctionnalités-clés)
- [6. Architecture des données](#6-architecture-des-données)
- [7. Résolution des problèmes](#7-résolution-des-problèmes)

## 1. Vue d'ensemble

L'intégration avec l'API France Travail permet de collecter des offres d'emploi à grande échelle. Le système mis en place:
- Collecte les offres d'emploi via l'API officielle France Travail
- Stocke les données en local et sur AWS S3
- Évite les collectes redondantes par vérification de l'existant
- Permet des collectes générales ou ciblées par secteur d'activité
- Gère l'authentification OAuth2, la pagination et les codes HTTP 206

## 2. Prérequis

### Variables d'environnement
Créer un fichier `.env` à la racine du projet avec les variables suivantes:

```
# Pôle Emploi / France Travail API
POLE_EMPLOI_CLIENT_ID=votre_client_id
POLE_EMPLOI_CLIENT_SECRET=votre_client_secret
POLE_EMPLOI_SCOPE=api_offresdemploiv2
URL_POLE_EMPLOI=https://entreprise.pole-emploi.fr

# AWS S3
KEY_ACCESS=votre_aws_access_key
KEY_SECRET=votre_aws_secret_key
data_lake_bucket=data-lake-brut
```

### Dépendances Python
Installer les dépendances requises:
```
pip install requests python-dotenv boto3 logging argparse
```

### Configuration AWS
S'assurer que:
1. Les credentials AWS sont valides
2. Le bucket S3 `data-lake-brut` existe
3. L'utilisateur AWS a les droits d'écriture sur ce bucket

## 3. Structure des scripts

```
/Projet
├── scripts/
│   ├── collect_all_france_travail_jobs.py    # Collecte générale sans filtre
│   ├── collect_france_travail_jobs.py        # Collecte avec mots-clés spécifiques
│   ├── collect_sectorial_jobs.py             # Collecte par secteur d'activité
│   ├── test_pole_emploi_api.py               # Test de l'API
│   └── verify_aws.py                         # Vérification de la configuration AWS
├── src/
│   └── data_collection/
│       └── apis/
│           └── pole_emploi.py                # Module principal d'interaction avec l'API
├── configs/
│   └── france_travail_sectors.json           # Configuration des secteurs d'activité
└── data/
    └── raw/
        └── france_travail/                   # Stockage local des données collectées
```

## 4. Utilisation des scripts

### Collecter toutes les offres sans filtre:
```bash
python scripts/collect_all_france_travail_jobs.py [options]
```
Options:
- `--max-pages N`: Limite le nombre de pages à collecter (défaut: 100)
- `--delay N.N`: Délai entre requêtes en secondes (défaut: 1.0)
- `--no-s3`: Ne pas télécharger vers S3
- `--force`: Forcer une nouvelle collecte même si des données existent

### Collecter avec des mots-clés spécifiques:
```bash
python scripts/collect_france_travail_jobs.py --keywords "data scientist" [options]
```
Options:
- `--keywords "votre recherche"`: Termes de recherche (obligatoire)
- `--location "ville"`: Filtrer par localisation
- `--distance N`: Distance de recherche en km
- `--max-pages N`: Limite le nombre de pages (défaut: 10)
- `--no-s3`: Ne pas télécharger vers S3

### Collecter par secteur d'activité:
```bash
python scripts/collect_sectorial_jobs.py [options]
```
Options:
- `--sector "nom_secteur"`: Nom du secteur défini dans configs/france_travail_sectors.json
- `--max-pages N`: Limite le nombre de pages par secteur (défaut: 5)
- `--delay N.N`: Délai entre requêtes en secondes (défaut: 1.0)
- `--force`: Forcer une nouvelle collecte même si des données existent

### Vérifier la configuration AWS:
```bash
python scripts/verify_aws.py
```

## 5. Fonctionnalités clés

### Vérification des données existantes
- Le système vérifie si des données ont déjà été collectées pour le jour actuel
- La vérification s'effectue à la fois en local et sur S3
- L'utilisateur est informé si des données existent déjà
- L'option `--force` permet de forcer une nouvelle collecte

### Gestion de la pagination
- Les offres sont collectées page par page (150 offres max par page)
- Le système s'arrête automatiquement quand:
  - Le nombre maximum de pages est atteint
  - Aucune offre n'est trouvée
  - Moins de 150 offres sont trouvées (fin des résultats)

### Gestion des erreurs
- Détection des réponses HTTP 206 (Partial Content)
- Renouvellement automatique du token d'accès
- Journalisation détaillée des erreurs et des opérations

### Collecte sectorielle
La collecte par secteur utilise un fichier de configuration JSON pour définir les mots-clés par secteur:
```json
{
  "tech": ["developpeur", "programmeur", "ingenieur logiciel"],
  "data": ["data scientist", "data engineer", "data analyst"],
  "...": ["..."]
}
```

## 6. Architecture des données

### Format de stockage local
Les fichiers sont enregistrés sous le format:
```
data/raw/france_travail/france_travail_{keywords}_{YYYYMMDD_HHMMSS}_p{page}.json
```

### Structure dans S3
Les fichiers sont téléchargés vers S3 avec la structure:
```
s3://data-lake-brut/raw/france_travail/france_travail_{keywords}_{YYYYMMDD_HHMMSS}_p{page}.json
```

### Format de données
Les données sont stockées en JSON avec la structure native de l'API France Travail.

## 7. Résolution des problèmes

### Problème d'authentification
```
Erreur: Impossible d'obtenir un token d'accès
```
Solutions:
- Vérifier les valeurs CLIENT_ID et CLIENT_SECRET
- Vérifier que l'application est bien activée sur le portail France Travail
- Regarder les logs pour des détails sur l'erreur HTTP

### Erreur lors de l'upload S3
```
Erreur lors du téléchargement vers S3
```
Solutions:
- Vérifier les clés AWS (KEY_ACCESS et KEY_SECRET)
- Vérifier que le bucket existe et que vous avez les droits d'écriture
- Exécuter `python scripts/verify_aws.py` pour diagnostiquer les problèmes

### Données déjà collectées
```
Des données ont déjà été collectées aujourd'hui
```
Solutions:
- Utiliser l'option `--force` pour forcer une nouvelle collecte
- Attendre le jour suivant pour une nouvelle collecte régulière
- Vérifier les fichiers existants dans `data/raw/france_travail/`

---

## Automatisation et prochaines étapes

Pour automatiser la collecte quotidienne, vous pouvez:

1. **Création d'une tâche planifiée Windows**:
   ```
   schtasks /create /tn "CollecteEmploi" /tr "path\to\python scripts\collect_all_france_travail_jobs.py" /sc daily /st 01:00
   ```

2. **Utilisation d'AWS Lambda**:
   - Déployer le code dans une fonction Lambda
   - Configurer un déclencheur EventBridge (CloudWatch Events) pour l'exécution quotidienne

3. **Développement du pipeline ETL**:
   - Ajouter un script de transformation pour extraire les informations pertinentes
   - Charger les données transformées dans PostgreSQL
   - Créer des tableaux de bord d'analyse
