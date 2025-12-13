
📊 Projet Business Intelligence Northwind

📝 Description du Projet

Ce projet implémente une solution de Business Intelligence (BI) de bout en bout basée sur le célèbre jeu de données Northwind Traders. L'objectif est de transformer des données transactionnelles brutes en informations décisionnelles exploitables via un Data Warehouse et des tableaux de bord interactifs.

🚀 Fonctionnalités Clés

Pipeline ETL Automatisé : Scripts Python pour extraire, nettoyer et transformer les données.

Data Warehouse (DWH) : Modélisation dimensionnelle en Schéma en Étoile (Star Schema).

Analyse Multidimensionnelle (OLAP) : Cube de données visualisé en 3D (Année x Client x Employé).

Tableau de Bord Interactif : KPIs financiers, carte géographique des ventes et analyse des livraisons.

🏗️ Architecture Technique

Le projet suit une architecture BI classique en 3 couches :

Extraction (E) : Récupération des données sources (Fichiers CSV / Access).

Transformation (T) :

Nettoyage des données (Gestion des NULLs, formatage des dates).

Création des Dimensions (DimClient, DimEmployee, DimProduct, DimDate).

Création de la Table de Faits (FactSales).

Chargement (L) : Stockage des données structurées dans une base SQLite (northwind_dwh.db).

Visualisation : Utilisation de Plotly et Jupyter Notebooks.

📂 Structure du Projet
code
Text
download
content_copy
expand_less
Northwind_project/
│
├── data/
│   ├── raw/                 # Données sources brutes (Orders.csv, Customers.csv...)
│   ├── processed/           # Fichiers nettoyés et transformés (CSVs intermédiaires)
│   └── warehouse/           # Base de données finale (northwind_dwh.db) et logs
│
├── figures/                # Rapports générés (Graphiques HTML et images PNG)
│
├── notebooks/
│   ├── exploration.ipynb     # Analyse exploratoire des données (EDA)
│   ├── etl_dev.ipynb         # Environnement de test pour le développement ETL
│   ├── modelling.ipynb       # Documentation du schéma en étoile
│   ├── verification.ipynb    # Tests de cohérence des données
│   └── dashboard_analysis.ipynb # 📊 LE DASHBOARD PRINCIPAL
│
├── scripts/
│   ├── extract_data.py      # Extraction des sources
│   ├── transform_data.py    # Logique de transformation (Star Schema)
│   ├── load_dwh.py          # Chargement en base de données
│   ├── visualize_3d.py      # Génération du Cube OLAP 3D et rapports HTML
│   └── etl_main.py          # 🚀 Script maître pour lancer tout le pipeline
│
├── requirements.txt         # Liste des dépendances Python
└── README.md                # Documentation du projet
⚙️ Installation et Configuration
1. Prérequis

Python 3.8 ou supérieur installé.

Git (optionnel, pour cloner le projet).

2. Installation

Ouvrez votre terminal et exécutez les commandes suivantes :

code
Bash
download
content_copy
expand_less
# 1. Cloner le dépôt (si applicable)
git clone https://github.com/tedjeddine22/Northwind_project.git
cd Northwind_project

# 2. Créer un environnement virtuel (recommandé)
python -m venv venv
# Sur Windows :
venv\Scripts\activate
# Sur Mac/Linux :
source venv/bin/activate

# 3. Installer les dépendances
pip install -r requirements.txt
▶️ Utilisation
Étape 1 : Exécuter le Pipeline ETL

Pour mettre à jour les données (Extraction -> Transformation -> Chargement DWH), lancez le script maître :

code
Bash
download
content_copy
expand_less
python scripts/etl_main.py

Vérifiez les logs dans le terminal pour confirmer le succès ("✅ PIPELINE ETL TERMINÉ").

Étape 2 : Générer les Visualisations (Rapports HTML)

Pour créer les graphiques interactifs (Cube 3D, Graphes de livraison) sauvegardés dans le dossier figures/ :

code
Bash
download
content_copy
expand_less
python scripts/visualize_3d.py

Ouvrez ensuite le fichier figures/3d_olap_scatter.html dans votre navigateur.

Étape 3 : Explorer le Dashboard

Pour une analyse interactive, lancez Jupyter et ouvrez le dashboard :

code
Bash
download
content_copy
expand_less
jupyter notebook notebooks/dashboard_analysis.ipynb
📊 Modèle de Données (Star Schema)

Le Data Warehouse est structuré autour de la table de faits centrale :

FactSales : Contient les métriques (Quantity, TotalAmount) et les clés étrangères.

DimDate : Axe temporel (Année, Mois, Trimestre).

DimClient : Axe client (Nom, Ville, Pays).

DimEmployee : Axe performance vendeur (Nom, Titre).

DimProduct : Axe produit (Nom, Catégorie).

📈 Aperçu des Visualisations

Le projet inclut des visualisations avancées :

KPIs Financiers : Chiffre d'affaires global, Panier moyen.

Cube OLAP 3D : Visualisation unique permettant de croiser 3 dimensions (Temps, Géographie, Ressource Humaine) en un seul graphique rotatif.

Analyse Géographique : Carte choroplèthe des ventes mondiales ou Top Villes.

Performance Logistique : Répartition des commandes Livrées vs Non Livrées.

👤 Auteur

BOUDERBA / Tadj eddine

Matricule : 222231244012

Étudiant en 3eme année ingénierie informatique cybersécurité / Business Intelligence

Projet réalisé dans le cadre du module Business Intelligence.
