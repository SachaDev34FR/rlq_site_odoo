import pandas as pd
import numpy as np
from loguru import logger
from icecream import ic
import sys
from typing import Union, List, Optional, Tuple
import warnings
from pathlib import Path
import time

# ================================================================================
# CONFIGURATION LOGGING AVEC LOGURU
# ================================================================================

# Supprimer le handler par défaut de loguru
logger.remove()

# Configuration personnalisée de loguru
logger.add(
    sys.stderr,  # Sortie console
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    level="DEBUG",
    colorize=True
)

# Fichier de log pour les erreurs critiques
logger.add(
    "logs/data_processing_errors.log",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} - {message}",
    level="ERROR",
    rotation="10 MB",  # Rotation tous les 10MB
    retention="30 days",  # Garde 30 jours
    compression="zip"  # Compression des anciens logs
)

# Fichier de log pour tout le processus
logger.add(
    "logs/data_processing_full.log",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} - {message}",
    level="INFO",
    rotation="1 day",
    retention="7 days"
)

# Configuration d'icecream
ic.configureOutput(prefix='🔍 DEBUG | ', includeContext=True)

# ================================================================================
# FONCTIONS UTILITAIRES DE VALIDATION ET LOGGING
# ================================================================================

def log_dataframe_info(df: pd.DataFrame, step_name: str) -> None:
    """
    Log des informations détaillées sur un DataFrame
    
    Args:
        df: DataFrame à analyser
        step_name: Nom de l'étape pour identifier dans les logs
    """
    logger.info(f"=== ANALYSE DATAFRAME - {step_name.upper()} ===")
    logger.info(f"Forme: {df.shape} (lignes: {df.shape[0]}, colonnes: {df.shape[1]})")
    logger.info(f"Mémoire utilisée: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    
    # Types de données
    types_info = df.dtypes.value_counts().to_dict()
    logger.info(f"Types de données: {types_info}")
    
    # Valeurs manquantes
    missing_info = df.isnull().sum()
    if missing_info.sum() > 0:
        logger.warning(f"Valeurs manquantes détectées:")
        for col, count in missing_info[missing_info > 0].items():
            pct = (count / len(df)) * 100
            logger.warning(f"  - {col}: {count} ({pct:.2f}%)")
    else:
        logger.info("Aucune valeur manquante")
    
    # Doublons
    duplicates = df.duplicated().sum()
    if duplicates > 0:
        logger.warning(f"Doublons détectés: {duplicates} lignes")
    else:
        logger.info("Aucun doublon détecté")

def validate_list_column(df: pd.DataFrame, column_name: str) -> Tuple[bool, List[str]]:
    """
    Valide qu'une colonne contient bien des listes et retourne les problèmes détectés
    
    Args:
        df: DataFrame à valider
        column_name: Nom de la colonne à vérifier
    
    Returns:
        Tuple[bool, List[str]]: (is_valid, liste_des_erreurs)
    """
    errors = []
    
    # Vérification de l'existence de la colonne
    if column_name not in df.columns:
        errors.append(f"Colonne '{column_name}' introuvable")
        return False, errors
    
    # Vérification du type des éléments
    non_list_count = 0
    empty_list_count = 0
    length_variations = []
    
    for idx, value in enumerate(df[column_name]):
        if pd.isna(value):
            errors.append(f"Valeur NaN à l'index {idx}")
        elif not isinstance(value, list):
            non_list_count += 1
            if non_list_count <= 5:  # Limite les exemples dans les logs
                errors.append(f"Type incorrect à l'index {idx}: {type(value)} - {value}")
        else:
            if len(value) == 0:
                empty_list_count += 1
            length_variations.append(len(value))
    
    # Résumé des problèmes
    if non_list_count > 5:
        errors.append(f"... et {non_list_count - 5} autres valeurs de type incorrect")
    
    if empty_list_count > 0:
        errors.append(f"{empty_list_count} listes vides détectées")
    
    # Vérification de la cohérence des longueurs
    if length_variations:
        min_len, max_len = min(length_variations), max(length_variations)
        if min_len != max_len:
            logger.warning(f"Longueurs variables détectées: min={min_len}, max={max_len}")
            ic(min_len, max_len, np.mean(length_variations))
    
    is_valid = len(errors) == 0
    return is_valid, errors

def safe_explode_column(df: pd.DataFrame, column_name: str, 
                       prefix: str = "col", validate: bool = True) -> pd.DataFrame:
    """
    Éclate une colonne de listes de manière sécurisée avec logging complet
    
    Args:
        df: DataFrame source
        column_name: Nom de la colonne à éclater
        prefix: Préfixe pour les nouvelles colonnes
        validate: Si True, valide les données avant traitement
    
    Returns:
        pd.DataFrame: DataFrame avec colonnes éclatées
    """
    logger.info(f"Début de l'éclatement de la colonne '{column_name}'")
    start_time = time.time()
    
    try:
        # Étape 1: Validation des données
        if validate:
            logger.info("Validation des données...")
            is_valid, errors = validate_list_column(df, column_name)
            
            if not is_valid:
                logger.error("Erreurs de validation détectées:")
                for error in errors:
                    logger.error(f"  - {error}")
                raise ValueError(f"Validation échouée pour la colonne '{column_name}'")
            else:
                logger.success("Validation réussie")
        
        # Étape 2: Analyse des données
        log_dataframe_info(df, "avant_eclatement")
        
        # Étape 3: Traitement des listes
        logger.info("Analyse des longueurs de listes...")
        lengths = df[column_name].apply(lambda x: len(x) if isinstance(x, list) else 0)
        max_length = lengths.max()
        unique_lengths = lengths.unique()
        
        ic(max_length, unique_lengths, lengths.describe())
        
        # Étape 4: Normalisation si nécessaire
        if len(unique_lengths) > 1:
            logger.warning("Longueurs variables détectées - normalisation nécessaire")
            
            def normalize_list(lst, target_length):
                if not isinstance(lst, list):
                    return [np.nan] * target_length
                normalized = lst.copy()
                while len(normalized) < target_length:
                    normalized.append(np.nan)
                return normalized
            
            normalized_lists = df[column_name].apply(lambda x: normalize_list(x, max_length))
            logger.info(f"Normalisation effectuée vers {max_length} éléments")
        else:
            normalized_lists = df[column_name]
            logger.info("Aucune normalisation nécessaire")
        
        # Étape 5: Création du DataFrame éclaté
        logger.info("Création des nouvelles colonnes...")
        exploded_df = pd.DataFrame(normalized_lists.tolist())
        exploded_df.columns = [f'{prefix}_{i+1}' for i in range(exploded_df.shape[1])]
        
        ic(exploded_df.shape, exploded_df.columns.tolist())
        
        # Étape 6: Concaténation
        logger.info("Concaténation avec le DataFrame original...")
        result_df = pd.concat([df.drop(column_name, axis=1), exploded_df], axis=1)
        
        # Étape 7: Validation finale
        log_dataframe_info(result_df, "après_eclatement")
        
        execution_time = time.time() - start_time
        logger.success(f"Éclatement terminé avec succès en {execution_time:.2f}s")
        logger.info(f"Nouvelles colonnes créées: {list(exploded_df.columns)}")
        
        return result_df
        
    except Exception as e:
        logger.error(f"Erreur lors de l'éclatement de la colonne: {str(e)}")
        logger.exception("Traceback complet:")
        raise

# ================================================================================
# FONCTIONS DE NETTOYAGE ET VALIDATION AVANCÉES
# ================================================================================

def detect_data_quality_issues(df: pd.DataFrame) -> dict:
    """
    Détecte automatiquement les problèmes de qualité des données
    
    Returns:
        dict: Dictionnaire avec les problèmes détectés
    """
    logger.info("Analyse de la qualité des données...")
    issues = {}
    
    # 1. Valeurs manquantes
    missing = df.isnull().sum()
    if missing.sum() > 0:
        issues['missing_values'] = missing[missing > 0].to_dict()
        logger.warning(f"Valeurs manquantes: {missing.sum()} au total")
    
    # 2. Doublons
    duplicates = df.duplicated().sum()
    if duplicates > 0:
        issues['duplicates'] = duplicates
        logger.warning(f"Doublons: {duplicates} lignes")
    
    # 3. Types de données incohérents
    for col in df.columns:
        if df[col].dtype == 'object':
            unique_types = set(type(x).__name__ for x in df[col].dropna())
            if len(unique_types) > 1:
                issues[f'mixed_types_{col}'] = list(unique_types)
                logger.warning(f"Types mixtes dans '{col}': {unique_types}")
    
    # 4. Valeurs aberrantes pour les colonnes numériques
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3 - Q1
        outliers = df[(df[col] < Q1 - 1.5 * IQR) | (df[col] > Q3 + 1.5 * IQR)][col]
        if len(outliers) > 0:
            issues[f'outliers_{col}'] = len(outliers)
            logger.warning(f"Valeurs aberrantes dans '{col}': {len(outliers)} valeurs")
    
    ic(issues)
    return issues

def clean_dataframe(df: pd.DataFrame, cleaning_config: dict = None) -> pd.DataFrame:
    """
    Nettoie un DataFrame selon une configuration donnée
    
    Args:
        df: DataFrame à nettoyer
        cleaning_config: Configuration du nettoyage
    """
    if cleaning_config is None:
        cleaning_config = {
            'remove_duplicates': True,
            'handle_missing': 'warn',  # 'drop', 'fill', 'warn'
            'normalize_strings': True,
            'validate_types': True
        }
    
    logger.info("Début du nettoyage des données")
    df_clean = df.copy()
    
    # Suppression des doublons
    if cleaning_config.get('remove_duplicates', False):
        initial_shape = df_clean.shape
        df_clean = df_clean.drop_duplicates()
        if df_clean.shape[0] < initial_shape[0]:
            removed = initial_shape[0] - df_clean.shape[0]
            logger.info(f"Doublons supprimés: {removed} lignes")
    
    # Gestion des valeurs manquantes
    missing_strategy = cleaning_config.get('handle_missing', 'warn')
    missing_count = df_clean.isnull().sum().sum()
    
    if missing_count > 0:
        if missing_strategy == 'drop':
            df_clean = df_clean.dropna()
            logger.info(f"Lignes avec valeurs manquantes supprimées: {missing_count}")
        elif missing_strategy == 'warn':
            logger.warning(f"Valeurs manquantes détectées: {missing_count}")
    
    # Normalisation des chaînes de caractères
    if cleaning_config.get('normalize_strings', False):
        string_cols = df_clean.select_dtypes(include=['object']).columns
        for col in string_cols:
            if df_clean[col].dtype == 'object':
                df_clean[col] = df_clean[col].astype(str).str.strip().str.lower()
        logger.info(f"Normalisation des chaînes effectuée sur {len(string_cols)} colonnes")
    
    log_dataframe_info(df_clean, "après_nettoyage")
    return df_clean

# ================================================================================
# EXEMPLE D'UTILISATION COMPLÈTE
# ================================================================================

def main_example():
    """
    Exemple complet d'utilisation avec toutes les bonnes pratiques
    """
    logger.info("=== DÉBUT DU TRAITEMENT DES DONNÉES ===")
    
    try:
        # Création de données d'exemple avec problèmes volontaires
        logger.info("Création des données d'exemple...")
        
        data = {
            'id': [1, 2, 3, 4, 5, 5],  # Doublon volontaire
            'nom': ['Alice', 'Bob', None, 'Diana', 'Eve', 'Alice'],  # Valeur manquante
            'reponses_formulaire': [
                ['Oui', 'Non', 'Peut-être'],
                ['Non', 'Oui', 'Oui'],
                ['Oui', 'Oui'],  # Longueur différente
                None,  # Valeur problématique
                ['Oui', 'Non', 'Non', 'Oui'],  # Longueur différente
                ['Oui', 'Non', 'Peut-être']
            ]
        }
        
        df = pd.DataFrame(data)
        ic(df.head())
        
        # Étape 1: Analyse initiale
        logger.info("Analyse initiale des données")
        log_dataframe_info(df, "données_brutes")
        
        # Étape 2: Détection des problèmes de qualité
        issues = detect_data_quality_issues(df)
        
        # Étape 3: Nettoyage préliminaire
        logger.info("Nettoyage préliminaire...")
        
        # Traitement spécifique des valeurs None dans la colonne liste
        df_clean = df.copy()
        df_clean = df_clean.dropna(subset=['reponses_formulaire'])  # Supprimer les None
        df_clean = df_clean.drop_duplicates(subset=['id', 'nom'])  # Supprimer les doublons
        
        logger.info(f"Lignes après nettoyage: {len(df_clean)}")
        
        # Étape 4: Éclatement sécurisé de la colonne
        logger.info("Éclatement de la colonne de listes...")
        df_result = safe_explode_column(
            df_clean, 
            'reponses_formulaire', 
            prefix='question',
            validate=True
        )
        
        # Étape 5: Validation finale
        logger.info("Validation finale du résultat")
        final_issues = detect_data_quality_issues(df_result)
        
        if not final_issues:
            logger.success("Aucun problème de qualité détecté dans le résultat final")
        else:
            logger.warning("Problèmes restants après traitement:")
            ic(final_issues)
        
        # Étape 6: Sauvegarde avec logging
        output_path = "data_processed/result.csv"
        logger.info(f"Sauvegarde vers: {output_path}")
        
        # Création du répertoire si nécessaire
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        
        df_result.to_csv(output_path, index=False)
        logger.success(f"Données sauvegardées: {df_result.shape[0]} lignes, {df_result.shape[1]} colonnes")
        
        return df_result
        
    except Exception as e:
        logger.critical(f"Erreur critique dans le traitement: {str(e)}")
        logger.exception("Traceback complet:")
        raise
    
    finally:
        logger.info("=== FIN DU TRAITEMENT ===")

# ================================================================================
# BONNES PRATIQUES ET POINTS D'ATTENTION
# ================================================================================

def print_best_practices():
    """
    Affiche les bonnes pratiques pour la manipulation de données
    """
    best_practices = """
    ================================================================================
    🔥 BONNES PRATIQUES POUR LA MANIPULATION DE DONNÉES
    ================================================================================
    
    1. 📊 VALIDATION DES DONNÉES
       ✅ Toujours vérifier la structure et les types avant traitement
       ✅ Identifier les valeurs manquantes et aberrantes
       ✅ Documenter les hypothèses sur les données
    
    2. 🔍 LOGGING ET DEBUGGING
       ✅ Logger chaque étape importante du traitement
       ✅ Utiliser icecream (ic) pour le debug rapide
       ✅ Conserver les traces d'erreurs avec traceback complet
       ✅ Séparer les logs par niveau (DEBUG, INFO, WARNING, ERROR)
    
    3. 🛡️ GESTION D'ERREURS
       ✅ Encapsuler le code dans des try-catch appropriés
       ✅ Valider les entrées de fonctions
       ✅ Prévoir des stratégies de récupération d'erreur
       ✅ Ne jamais ignorer silencieusement les erreurs
    
    4. 💾 GESTION MÉMOIRE
       ✅ Surveiller l'usage mémoire des DataFrames volumineux
       ✅ Utiliser les types de données optimaux (category, int32, etc.)
       ✅ Libérer la mémoire des variables temporaires
       ✅ Considérer le traitement par chunks pour les gros volumes
    
    5. 🔄 REPRODUCTIBILITÉ
       ✅ Fixer les seeds pour les opérations aléatoires
       ✅ Versionner les scripts et les données
       ✅ Documenter les transformations appliquées
       ✅ Sauvegarder les données intermédiaires critiques
    
    6. 🧪 TESTS ET VALIDATION
       ✅ Tester avec des jeux de données variés
       ✅ Valider les résultats avec des échantillons connus
       ✅ Implémenter des tests unitaires pour les fonctions critiques
       ✅ Vérifier la cohérence avant/après transformation
    
    ================================================================================
    ⚠️  POINTS D'ATTENTION CRITIQUES
    ================================================================================
    
    🚨 ÉCLATEMENT DE COLONNES LISTES:
       - Vérifier que tous les éléments sont bien des listes
       - Gérer les listes de longueurs variables
       - Prévoir la normalisation avec des valeurs NaN
       - Valider la cohérence des données résultantes
    
    🚨 PERFORMANCE:
       - Les opérations sur DataFrames peuvent être coûteuses
       - Préférer les opérations vectorisées aux boucles
       - Surveiller l'usage mémoire avec memory_profiler si nécessaire
       - Utiliser des formats optimisés (parquet) pour les gros volumes
    
    🚨 QUALITÉ DES DONNÉES:
       - Ne jamais supposer que les données sont propres
       - Toujours valider les types et formats attendus
       - Documenter les règles de nettoyage appliquées
       - Conserver une trace des données supprimées/modifiées
    """
    
    print(best_practices)
    logger.info("Bonnes pratiques affichées")

# ================================================================================
# EXÉCUTION DE L'EXEMPLE
# ================================================================================

if __name__ == "__main__":
    # Créer les répertoires de logs s'ils n'existent pas
    Path("logs").mkdir(exist_ok=True)
    Path("data_processed").mkdir(exist_ok=True)
    
    # Afficher les bonnes pratiques
    print_best_practices()
    
    # Exécuter l'exemple complet
    result_df = main_example()
    
    # Affichage final avec icecream
    ic(result_df.head())
    ic(result_df.info())
