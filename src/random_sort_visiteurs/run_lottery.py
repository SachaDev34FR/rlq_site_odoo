import pandas as pd
import numpy as np
import os
from datetime import datetime

def load_and_concatenate_participants(visiteurs_path, benevoles_path):
    """
    Loads participants from 'visiteurs' and 'benevoles' Excel files and concatenates them.
    """
    df_visiteurs = pd.DataFrame()
    df_benevoles = pd.DataFrame()

    try:
        df_visiteurs = pd.read_excel(visiteurs_path)
        print(f"Fichier '{visiteurs_path}' chargé avec {len(df_visiteurs)} lignes.")
    except FileNotFoundError:
        print(f"AVERTISSEMENT : Le fichier des visiteurs '{visiteurs_path}' n'a pas été trouvé.")

    try:
        df_benevoles = pd.read_excel(benevoles_path)
        print(f"Fichier '{benevoles_path}' chargé avec {len(df_benevoles)} lignes.")
    except FileNotFoundError:
        print(f"AVERTISSEMENT : Le fichier des bénévoles '{benevoles_path}' n'a pas été trouvé.")

    df_entree = pd.concat([df_visiteurs, df_benevoles], ignore_index=True)
    print(f"\nTotal de {len(df_entree)} participants après concaténation.")
    return df_entree

def get_ineligible_participants(winners_file_path, name_column='nom', role_column='ticket_devenement'):
    """
    Analyzes the winners file to determine who is no longer eligible to win.
    - Non-volunteers can win once.
    - Volunteers can win twice.
    Returns a list of names of ineligible people and the existing winners DataFrame.
    """
    if not os.path.exists(winners_file_path):
        print("\nAucun fichier de gagnants existant. Tout le monde est éligible.")
        return [], None

    df_gagnants_existants = pd.read_excel(winners_file_path)
    if df_gagnants_existants.empty:
        return [], df_gagnants_existants

    if name_column not in df_gagnants_existants.columns or role_column not in df_gagnants_existants.columns:
        print(f"AVERTISSEMENT : Les colonnes '{name_column}' ou '{role_column}' sont manquantes dans le fichier des gagnants.")
        return [], df_gagnants_existants

    gagnants_benevoles = df_gagnants_existants[df_gagnants_existants[role_column] == 'Benevole']
    gagnants_autres = df_gagnants_existants[df_gagnants_existants[role_column] != 'Benevole']

    personnes_a_exclure_autres = gagnants_autres[name_column].unique().tolist()

    wins_par_benevole = gagnants_benevoles[name_column].value_counts()
    benevoles_a_exclure = wins_par_benevole[wins_par_benevole >= 2].index.tolist()

    personnes_ineligibles = list(set(personnes_a_exclure_autres + benevoles_a_exclure))
    
    print(f"\n{len(personnes_ineligibles)} personnes sont inéligibles pour ce tirage (gains précédents).")
    return personnes_ineligibles, df_gagnants_existants

def main():
    """
    Main function to run the lottery draw with role-based rules.
    """
    # --- 1. Configuration ---
    # Déterminer la racine du projet de manière dynamique
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    base_dir = project_root
    output_dir = os.path.join(base_dir, "output_files")
    
    fichier_visiteurs = os.path.join(output_dir, "event_registration_visiteurs.xlsx")
    fichier_benevoles = os.path.join(output_dir, "event_registration_benevoles.xlsx")
    
    nom_fichier_gagnants = os.path.join(output_dir, "gagnants_combines.xlsx")
    
    colonne_nom = "nom_du_participant"
    colonne_role = "ticket_devenement"
    colonne_id_ticket = "numero_ticket"

    # --- 2. Load and Prepare Data ---
    df_entree = load_and_concatenate_participants(fichier_visiteurs, fichier_benevoles)
    if df_entree.empty:
        print("Aucun participant à tirer au sort. Arrêt du script.")
        return

    # --- 3. Filter Eligible Participants ---
    personnes_ineligibles, df_gagnants_existants = get_ineligible_participants(nom_fichier_gagnants, colonne_nom, colonne_role)
    
    df_eligibles = df_entree[~df_entree[colonne_nom].isin(personnes_ineligibles)]

    if df_gagnants_existants is not None and colonne_id_ticket in df_gagnants_existants.columns:
        tickets_gagnants = df_gagnants_existants[colonne_id_ticket].tolist()
        df_eligibles = df_eligibles[~df_eligibles[colonne_id_ticket].isin(tickets_gagnants)]

    print(f"\nNombre de tickets éligibles pour ce tirage : {len(df_eligibles)}")
    if df_eligibles.empty:
        print("Il n'y a plus de tickets éligibles pour le tirage.")
        return

    # --- 4. Get User Input and Draw ---
    try:
        nb_gagnants_a_tirer = int(input("Nombre de gagnants à tirer ? "))
    except ValueError:
        print("Veuillez entrer un nombre entier valide.")
        return

    if nb_gagnants_a_tirer <= 0:
        print("Le nombre de gagnants doit être supérieur à zéro.")
        return
    
    if len(df_eligibles) < nb_gagnants_a_tirer:
        print(f"Il n'y a pas assez de participants éligibles ({len(df_eligibles)}) pour tirer {nb_gagnants_a_tirer} gagnants.")
        return

    df_nouveaux_gagnants = df_eligibles.sample(n=nb_gagnants_a_tirer)
    df_nouveaux_gagnants['heure_du_tirage'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print(f"\n🎉 Les {nb_gagnants_a_tirer} nouveaux gagnants sont :")
    print(df_nouveaux_gagnants)

    # --- 5. Save Results ---
    if df_gagnants_existants is not None and not df_gagnants_existants.empty:
        df_final_gagnants = pd.concat([df_gagnants_existants, df_nouveaux_gagnants], ignore_index=True)
    else:
        df_final_gagnants = df_nouveaux_gagnants

    df_final_gagnants.to_excel(nom_fichier_gagnants, index=False)
    print(f"\nLa liste complète des gagnants a été mise à jour dans '{nom_fichier_gagnants}'.")

if __name__ == "__main__":
    main()