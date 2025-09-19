import pandas as pd

try:
    # --- 1. Charger et préparer les données ---
    file_path = '/media/sacha/DATA_P21/projet_RLQ_Odoo_test/output_files/event_registration_cleaned.xlsx'
    df = pd.read_excel(file_path)

    # On propage les informations du participant sur les lignes suivantes
    cols_to_fill = ['nom_du_participant', 'email']
    df[cols_to_fill] = df[cols_to_fill].fillna(method='ffill')

    # On garde uniquement les colonnes nécessaires et on supprime les lignes sans réponse
    df_responses = df[['nom_du_participant', 'email', 'reponses_des_participants']].copy()
    df_responses.dropna(subset=['reponses_des_participants'], inplace=True)


    # --- 2. Numéroter les réponses pour chaque participant ---
    df_responses['num_reponse'] = df_responses.groupby(['nom_du_participant', 'email']).cumcount()


    # --- 3. Pivoter les données ---
    # Index : ce qui reste en lignes (nos participants)
    # Columns : ce qui devient des colonnes (nos numéros de réponse)
    # Values : les valeurs à mettre dans les nouvelles colonnes
    df_pivot = df_responses.pivot_table(
        index=['nom_du_participant', 'email'],
        columns='num_reponse',
        values='reponses_des_participants',
        aggfunc='first'  # On prend la première valeur (il ne devrait y en avoir qu'une)
    ).reset_index()


    # --- 4. Renommer les nouvelles colonnes ---
    # Le pivot crée des colonnes nommées 0, 1, 2, ...
    # On les renomme pour plus de clarté.
    
    # Crée un dictionnaire pour le renommage, ex: {0: 'reponse_1', 1: 'reponse_2', ...}
    rename_dict = {col: f'reponse_{int(col) + 1}' for col in df_pivot.columns if isinstance(col, int)}
    df_pivot.rename(columns=rename_dict, inplace=True)

    print("--- Tableau après pivot et renommage ---")
    print(df_pivot.head())


    # --- 5. (Optionnel) Fusionner avec les données originales ---
    # Pour conserver les autres informations (comme le ticket, le statut, etc.)

    # On récupère les informations uniques de chaque participant
    df_unique_participants = df.drop_duplicates(subset=['nom_du_participant', 'email'])
    
    # On enlève l'ancienne colonne de réponses pour éviter la confusion
    df_unique_participants = df_unique_participants.drop(columns=['reponses_des_participants'])

    # On fusionne les informations uniques avec notre tableau pivoté
    df_final = pd.merge(df_unique_participants, df_pivot, on=['nom_du_participant', 'email'])

    print("
--- Tableau final fusionné ---")
    print(df_final.head())


except FileNotFoundError:
    print(f"Le fichier {file_path} n'a pas été trouvé. Veuillez vérifier le chemin.")
except Exception as e:
    print(f"Une erreur est survenue : {e}")
