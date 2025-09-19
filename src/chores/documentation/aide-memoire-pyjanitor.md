# Aide-mémoire Pyjanitor

`pyjanitor` est une bibliothèque Python qui fournit une API propre et un ensemble de fonctions pour nettoyer les données dans les DataFrames pandas. Elle fonctionne par "chaînage de méthodes", rendant le code de nettoyage de données plus lisible et plus fluide.

## Installation

Installez `pyjanitor` et pandas :

```bash
pip install pandas pyjanitor
```

## Principe de Base : Chaînage de Méthodes

L'idée principale est d'enchaîner les opérations de nettoyage directement sur le DataFrame.

**Exemple de base :**

```python
import pandas as pd
import janitor

df = pd.DataFrame(...)

# Chaînage de méthodes pour nettoyer le DataFrame
df_clean = (
    df
    .clean_names()
    .remove_empty()
    .rename_column('old_name', 'new_name')
)
```

## Fonctions Courantes

### `clean_names()`

C'est la fonction la plus emblématique. Elle standardise les noms de colonnes :
- Convertit en minuscules.
- Remplace les espaces et les caractères spéciaux par des underscores (`_`).
- Supprime les caractères non alphanumériques.

```python
df = pd.DataFrame({'First Name': ['John'], 'Last-Name': ['Doe']})
df_cleaned = df.clean_names()
# Colonnes -> ['first_name', 'last_name']
```

### `remove_empty()`

Supprime les lignes et/ou les colonnes qui sont entièrement vides (NaN).

```python
df = pd.DataFrame({'a': [1, np.nan, 3], 'b': [4, np.nan, 6], 'c': [np.nan, np.nan, np.nan]})
df_cleaned = df.remove_empty()
# La ligne 1 et la colonne 'c' sont supprimées.
```

### `get_dupes()`

Identifie les lignes dupliquées dans un DataFrame. C'est utile pour inspecter les doublons avant de les supprimer.

```python
df = pd.DataFrame({'a': [1, 2, 1], 'b': ['x', 'y', 'x']})
duplicates = df.get_dupes()
# Retourne les deux lignes où ('a'==1, 'b'=='x')
```

### `coalesce()`

Remplit les valeurs manquantes (NaN) dans une colonne en utilisant les valeurs d'autres colonnes, dans l'ordre spécifié.

```python
df = pd.DataFrame({'a': [1, np.nan, 3], 'b': [np.nan, 5, 6]})
df_coalesced = df.coalesce('c', 'a', 'b')
# df_coalesced['c'] -> [1.0, 5.0, 3.0]
```

### `convert_excel_date()`

Convertit une colonne de dates au format numérique d'Excel en format datetime.

```python
df = pd.DataFrame({'date_excel': [43831, 43832]})
df_dates = df.convert_excel_date('date_excel')
# La colonne 'date_excel' est maintenant au format datetime64[ns].
```

### `rename_column()`

Renomme une colonne spécifique. Plus fluide que la méthode pandas `rename`.

```python
df = df.rename_column('old_column_name', 'new_column_name')
```

### `case_when()`

Permet de créer une nouvelle colonne basée sur une logique conditionnelle, similaire à un `CASE WHEN` en SQL.

```python
df = pd.DataFrame({'score': [95, 85, 75, 65]})
df_graded = df.case_when(
    (df['score'] >= 90, 'A'),
    (df['score'] >= 80, 'B'),
    (df['score'] >= 70, 'C'),
    'D',  # Valeur par défaut
    column_name='grade'
)
# df_graded['grade'] -> ['A', 'B', 'C', 'D']
```

### `pivot_longer()`

Transforme un DataFrame de format "large" à "long". C'est une version plus intuitive de `pd.melt()`.

```python
df = pd.DataFrame({'student': ['A'], 'midterm': [85], 'final': [92]})
df_long = df.pivot_longer(
    index='student',
    names_to='exam_type',
    values_to='score'
)
# Résultat :
#   student exam_type  score
# 0       A   midterm     85
# 1       A     final     92
```

---
`pyjanitor` rend le processus de nettoyage et de préparation des données plus robuste, lisible et moins sujet aux erreurs.
