import pandas as pd
import numpy as np

# Création d'un DataFrame d'exemple avec une colonne contenant des listes
# Simule des réponses à un formulaire de questions
data = {
    'id': [1, 2, 3, 4, 5],
    'nom': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
    'reponses_formulaire': [
        ['Oui', 'Non', 'Peut-être', 'Oui'],
        ['Non', 'Oui', 'Oui', 'Non'],
        ['Oui', 'Oui', 'Non', 'Peut-être'],
        ['Non', 'Peut-être', 'Oui', 'Oui'],
        ['Oui', 'Non', 'Non', 'Non']
    ]
}

df = pd.DataFrame(data)
print("DataFrame original :")
print(df)
print("\n" + "="*50 + "\n")

# MÉTHODE 1 : Utilisation de pd.DataFrame() directement sur la colonne
# Cette méthode est la plus simple et efficace
print("MÉTHODE 1 : Conversion directe avec pd.DataFrame()")
print("-" * 45)

# Convertir la colonne de listes en DataFrame séparé
# Chaque liste devient une ligne, chaque élément de la liste devient une colonne
colonnes_eclatees = pd.DataFrame(df['reponses_formulaire'].tolist())

# Renommer les colonnes avec des noms explicites
colonnes_eclatees.columns = [f'question_{i+1}' for i in range(colonnes_eclatees.shape[1])]

# Concaténer avec le DataFrame original (en supprimant la colonne d'origine)
df_methode1 = pd.concat([df.drop('reponses_formulaire', axis=1), colonnes_eclatees], axis=1)

print("Résultat méthode 1 :")
print(df_methode1)
print("\n" + "="*50 + "\n")

# MÉTHODE 2 : Utilisation de apply() avec pd.Series
# Plus flexible, permet de gérer des cas complexes
print("MÉTHODE 2 : Utilisation de apply() avec pd.Series")
print("-" * 45)

# Fonction pour convertir chaque liste en Series avec des noms de colonnes personnalisés
def eclater_liste(liste_reponses, prefixe='Q'):
    """
    Convertit une liste en Series pandas avec des index nommés
    
    Args:
        liste_reponses: Liste des réponses à éclater
        prefixe: Préfixe pour les noms de colonnes
    
    Returns:
        pd.Series: Series avec les éléments de la liste
    """
    return pd.Series(liste_reponses, index=[f'{prefixe}{i+1}' for i in range(len(liste_reponses))])

# Appliquer la fonction à chaque ligne
colonnes_apply = df['reponses_formulaire'].apply(lambda x: eclater_liste(x, 'Question_'))

# Créer le DataFrame final
df_methode2 = pd.concat([df.drop('reponses_formulaire', axis=1), colonnes_apply], axis=1)

print("Résultat méthode 2 :")
print(df_methode2)
print("\n" + "="*50 + "\n")

# MÉTHODE 3 : Gestion des listes de tailles variables
# Utile quand les listes n'ont pas toutes la même longueur
print("MÉTHODE 3 : Gestion des listes de tailles variables")
print("-" * 50)

# Création d'un exemple avec des listes de tailles différentes
data_variable = {
    'id': [1, 2, 3],
    'nom': ['Alice', 'Bob', 'Charlie'],
    'reponses_formulaire': [
        ['Oui', 'Non', 'Peut-être'],      # 3 éléments
        ['Non', 'Oui'],                   # 2 éléments
        ['Oui', 'Oui', 'Non', 'Peut-être', 'Oui']  # 5 éléments
    ]
}

df_variable = pd.DataFrame(data_variable)
print("DataFrame avec listes de tailles variables :")
print(df_variable)
print()

# Trouver la longueur maximale des listes
max_longueur = max(len(liste) for liste in df_variable['reponses_formulaire'])
print(f"Longueur maximale des listes : {max_longueur}")

# Fonction pour normaliser les listes (compléter avec NaN si nécessaire)
def normaliser_liste(liste, longueur_max):
    """
    Normalise une liste à une longueur donnée en complétant avec NaN
    
    Args:
        liste: Liste à normaliser
        longueur_max: Longueur cible
    
    Returns:
        list: Liste normalisée
    """
    liste_normalisee = liste.copy()
    while len(liste_normalisee) < longueur_max:
        liste_normalisee.append(np.nan)
    return liste_normalisee

# Appliquer la normalisation
listes_normalisees = df_variable['reponses_formulaire'].apply(lambda x: normaliser_liste(x, max_longueur))

# Convertir en DataFrame
colonnes_normalisees = pd.DataFrame(listes_normalisees.tolist())
colonnes_normalisees.columns = [f'reponse_{i+1}' for i in range(max_longueur)]

# Créer le DataFrame final
df_methode3 = pd.concat([df_variable.drop('reponses_formulaire', axis=1), colonnes_normalisees], axis=1)

print("Résultat avec normalisation :")
print(df_methode3)
print("\n" + "="*50 + "\n")

# MÉTHODE 4 : Fonction générique réutilisable
print("MÉTHODE 4 : Fonction générique réutilisable")
print("-" * 40)

def eclater_colonne_liste(df, nom_colonne, prefixe_nouvelles_colonnes='col', 
                         supprimer_colonne_origine=True, normaliser=True):
    """
    Éclate une colonne contenant des listes en plusieurs colonnes
    
    Args:
        df: DataFrame pandas
        nom_colonne: Nom de la colonne à éclater
        prefixe_nouvelles_colonnes: Préfixe pour les nouvelles colonnes
        supprimer_colonne_origine: Si True, supprime la colonne d'origine
        normaliser: Si True, normalise les listes à la même longueur
    
    Returns:
        pd.DataFrame: DataFrame avec les colonnes éclatées
    """
    # Vérifier que la colonne existe
    if nom_colonne not in df.columns:
        raise ValueError(f"La colonne '{nom_colonne}' n'existe pas dans le DataFrame")
    
    # Copier le DataFrame pour éviter les modifications inattendues
    df_resultat = df.copy()
    
    # Obtenir les listes de la colonne
    listes = df_resultat[nom_colonne]
    
    if normaliser:
        # Trouver la longueur maximale
        longueur_max = max(len(liste) if isinstance(liste, list) else 0 for liste in listes)
        
        # Normaliser les listes
        listes_normalisees = listes.apply(
            lambda x: normaliser_liste(x, longueur_max) if isinstance(x, list) else [np.nan] * longueur_max
        )
        
        # Convertir en DataFrame
        colonnes_eclatees = pd.DataFrame(listes_normalisees.tolist())
    else:
        # Conversion directe sans normalisation
        colonnes_eclatees = pd.DataFrame(listes.tolist())
    
    # Nommer les colonnes
    nb_colonnes = colonnes_eclatees.shape[1]
    colonnes_eclatees.columns = [f'{prefixe_nouvelles_colonnes}_{i+1}' for i in range(nb_colonnes)]
    
    # Supprimer la colonne d'origine si demandé
    if supprimer_colonne_origine:
        df_resultat = df_resultat.drop(nom_colonne, axis=1)
    
    # Concaténer les résultats
    df_final = pd.concat([df_resultat, colonnes_eclatees], axis=1)
    
    return df_final

# Test de la fonction générique
print("Test de la fonction générique :")
df_test = eclater_colonne_liste(
    df=df, 
    nom_colonne='reponses_formulaire', 
    prefixe_nouvelles_colonnes='reponse',
    supprimer_colonne_origine=True,
    normaliser=True
)

print(df_test)
print("\n" + "="*50 + "\n")

# CONSEILS D'UTILISATION ET ALGORITHME
print("ALGORITHME ET CONSEILS :")
print("-" * 25)
print("""
ALGORITHME GÉNÉRAL :
1. Identifier la colonne contenant les listes
2. Déterminer la stratégie de traitement (normalisation ou non)
3. Convertir les listes en DataFrame séparé
4. Nommer les nouvelles colonnes de manière explicite
5. Concaténer avec le DataFrame original
6. Optionnellement supprimer la colonne d'origine

CHOIX DE LA MÉTHODE :
- Méthode 1 : Simple et rapide, toutes les listes ont la même longueur
- Méthode 2 : Plus de contrôle sur le nommage, même longueur de listes
- Méthode 3 : Listes de longueurs variables avec normalisation
- Méthode 4 : Solution générique réutilisable

CONSIDÉRATIONS IMPORTANTES :
- Vérifiez toujours la cohérence des données avant l'éclatement
- Gérez les valeurs manquantes (NaN) appropriément
- Choisissez des noms de colonnes explicites
- Testez avec un échantillon de données avant le traitement complet
- Considérez l'impact sur la mémoire pour de gros datasets
""")
