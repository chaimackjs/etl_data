# Documentation de l'Analyse Welcome to the Jungle

## Introduction

Cette documentation décrit le module d'analyse des offres d'emploi provenant de Welcome to the Jungle. Ce module fait partie d'un pipeline ETL plus large qui collecte, transforme et analyse des données d'offres d'emploi de différentes sources. L'analyse Welcome to the Jungle permet d'extraire des insights précieux sur le marché de l'emploi tech en France à partir des offres publiées sur cette plateforme.

## Architecture du Module d'Analyse

Le module d'analyse Welcome to the Jungle est composé de deux scripts principaux :

1. **welcome_jungle_analysis.py** : Contient toutes les fonctions d'analyse et de visualisation des données
2. **run_welcome_jungle_analysis.py** : Script CLI permettant d'exécuter l'analyse avec différentes options

### Structure des Dossiers

```
Projet/
├── src/
│   └── analysis/
│       ├── welcome_jungle_analysis.py
│       └── run_welcome_jungle_analysis.py
├── data/
│   ├── raw/
│   │   └── welcome_jungle/        # Données brutes
│   ├── processed/
│   │   └── welcome_jungle/        # Données transformées
│   └── analysis/
│       └── visualizations/
│           └── welcome_jungle/    # Visualisations générées
└── docs/
    └── welcome_jungle_analysis_documentation.md
```

## Fonctionnalités Principales

Le module d'analyse Welcome to the Jungle offre les fonctionnalités suivantes :

1. **Chargement des données** : Depuis PostgreSQL ou depuis des fichiers CSV
2. **Analyse statistique** : Calcul de statistiques descriptives sur les offres d'emploi
3. **Visualisations** : Génération de graphiques pour différentes dimensions d'analyse
4. **Rapport HTML** : Création d'un tableau de bord interactif regroupant toutes les visualisations
5. **Rapport JSON** : Sauvegarde des résultats d'analyse au format JSON pour une utilisation ultérieure

## Sources de Données

L'analyse peut utiliser deux sources de données :

1. **Base de données PostgreSQL** : Connexion à une base de données PostgreSQL hébergée sur AWS RDS
2. **Fichiers CSV** : Utilisation des fichiers CSV transformés stockés localement

La source de données est spécifiée via l'option `--source` lors de l'exécution du script CLI.

## Dimensions d'Analyse

Le module analyse les offres d'emploi selon plusieurs dimensions :

1. **Types de contrat** : Distribution des offres par type de contrat (CDI, CDD, Stage, etc.)
2. **Localisation** : Distribution géographique des offres d'emploi
3. **Niveau d'expérience** : Répartition des offres selon le niveau d'expérience requis
4. **Salaires** : Distribution et statistiques des salaires proposés
5. **Technologies** : Analyse des technologies mentionnées dans les descriptions
6. **Catégories d'emploi** : Répartition des offres par catégorie
7. **Évolution temporelle** : Analyse de l'évolution des offres dans le temps

## Utilisation du Module

### Exécution via CLI

```bash
python src/analysis/run_welcome_jungle_analysis.py [options]
```

Options disponibles :
- `--source {db,csv}` : Source des données (base de données ou fichiers CSV)
- `--csv-file PATH` : Chemin vers un fichier CSV spécifique (si source=csv)
- `--open-html` : Ouvre automatiquement le rapport HTML dans le navigateur

### Exemple d'utilisation

```bash
# Analyse à partir de la base de données
python src/analysis/run_welcome_jungle_analysis.py --source db --open-html

# Analyse à partir d'un fichier CSV spécifique
python src/analysis/run_welcome_jungle_analysis.py --source csv --csv-file data/processed/welcome_jungle/welcome_jungle_transformed_20250614_175507.csv
```

## Gestion des Valeurs Manquantes

Le module d'analyse gère intelligemment les valeurs manquantes :

- **Localisation** : Les valeurs non spécifiées sont remplacées par "Télétravail / Remote"
- **Niveau d'expérience** : Les valeurs non spécifiées sont remplacées par "Tous niveaux d'expérience"
- **Salaires** : Les offres sans salaire sont exclues des analyses de salaire mais comptabilisées dans les statistiques globales

## Visualisations Générées

Le module génère les visualisations suivantes :

1. **Distribution des types de contrat** : Graphique à barres des types de contrat
2. **Top 15 des localisations** : Graphique à barres horizontales des principales villes
3. **Distribution des niveaux d'expérience** : Graphique à barres des niveaux d'expérience
4. **Distribution des salaires** : Histogramme des salaires proposés
5. **Top technologies** : Graphique à barres des technologies les plus demandées
6. **Nuage de mots des compétences** : Représentation visuelle des compétences les plus mentionnées
7. **Évolution temporelle** : Graphique linéaire de l'évolution des offres dans le temps

Toutes les visualisations sont sauvegardées au format PNG dans le dossier `data/analysis/visualizations/welcome_jungle/`.

## Rapport HTML

Le module génère un rapport HTML interactif qui regroupe toutes les visualisations et les résultats d'analyse. Ce rapport est sauvegardé dans `data/analysis/visualizations/welcome_jungle/welcome_jungle_dashboard.html`.

## Dépendances

Le module d'analyse dépend des bibliothèques Python suivantes :

- pandas : Manipulation et analyse des données
- matplotlib et seaborn : Création de visualisations
- psycopg2 : Connexion à la base de données PostgreSQL
- wordcloud : Génération de nuages de mots
- numpy : Calculs numériques
- dotenv : Chargement des variables d'environnement
- json : Manipulation de données JSON
- logging : Journalisation des événements

## Variables d'Environnement

Le module utilise les variables d'environnement suivantes pour la connexion à la base de données :

- `DB_HOST` : Hôte de la base de données
- `DB_PORT` : Port de la base de données
- `DB_NAME` : Nom de la base de données
- `DB_USER` : Utilisateur de la base de données
- `DB_PASSWORD` : Mot de passe de la base de données

Ces variables sont chargées à partir du fichier `.env` à la racine du projet.

## Gestion des Erreurs

Le module implémente une gestion robuste des erreurs :

- Journalisation des erreurs avec différents niveaux de gravité
- Gestion des erreurs de connexion à la base de données
- Gestion des erreurs de lecture de fichiers
- Gestion des erreurs de génération de visualisations

## Extensions Possibles

Le module pourrait être étendu avec les fonctionnalités suivantes :

1. Analyse comparative avec d'autres sources d'offres d'emploi
2. Analyse de sentiment sur les descriptions d'offres
3. Prédiction de tendances futures du marché de l'emploi
4. Filtrage interactif des visualisations
5. Exportation des résultats vers d'autres formats (PDF, Excel)
