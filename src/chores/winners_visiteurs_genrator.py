import pandas as pd
import os

def tirer_au_sort_excel(fichier_entree, fichier_gagnants, nb_gagnants=1, col_ticket='numéro_ticket'):
    """
    Réalise un tirage au sort à partir d'un fichier Excel avec un nombre de gagnants spécifié.

    Args:
        fichier_entree (str): Le chemin du fichier Excel contenant tous les participants.
        fichier_gagnants (str): Le chemin du fichier Excel où les gagnants sont enregistrés.
        nb_gagnants (int): Le nombre de gagnants à tirer.
        col_ticket (str): Le nom de la colonne contenant l'identifiant unique du ticket.
    """
    # 1. Charger les participants depuis le fichier d'entrée
    try:
        df_entree = pd.read_excel(fichier_entree)
    except FileNotFoundError:
        print(f"Erreur : Le fichier d'entrée '{fichier_entree}' n'a pas été trouvé.")
        return None

    # 2. Charger les gagnants existants pour les exclure du tirage
    tickets_gagnants_existants = []
    if os.path.exists(fichier_gagnants):
        df_gagnants_existants = pd.read_excel(fichier_gagnants)
        if not df_gagnants_existants.empty:
            tickets_gagnants_existants = df_gagnants_existants[col_ticket].tolist()

    # 3. Filtrer les participants pour ne garder que les éligibles
    df_eligibles = df_entree[~df_entree[col_ticket].isin(tickets_gagnants_existants)]

    if len(df_eligibles) < nb_gagnants:
        print(f"Il n'y a pas assez de participants éligibles pour tirer {nb_gagnants} gagnants.")
        print(f"Seulement {len(df_eligibles)} participants sont disponibles.")
        return None

    # 4. Tirer au sort les nouveaux gagnants
    df_nouveaux_gagnants = df_eligibles.sample(n=nb_gagnants)

    # 5. Combiner les anciens et les nouveaux gagnants
    if 'df_gagnants_existants' in locals() and not df_gagnants_existants.empty:
        df_final_gagnants = pd.concat([df_gagnants_existants, df_nouveaux_gagnants], ignore_index=True)
    else:
        df_final_gagnants = df_nouveaux_gagnants

    # 6. Enregistrer la liste complète des gagnants dans le fichier de sortie
    df_final_gagnants.to_excel(fichier_gagnants, index=False)

    print(f"🎉 Les {nb_gagnants} nouveaux gagnants sont :")
    for index, gagnant in df_nouveaux_gagnants.iterrows():
        # On essaie de trouver une colonne 'nom', sinon on affiche juste le ticket
        nom_gagnant = gagnant.get('nom', '[Nom non trouvé]')
        print(f" - {nom_gagnant} avec le ticket {gagnant[col_ticket]}")

    print(f"\nLa liste complète des gagnants a été mise à jour dans '{fichier_gagnants}'.")
    return df_nouveaux_gagnants


def main():
    """_summary_
    """
    # Définissez les noms de vos fichiers Excel
    nom_fichier_participants = "participants.xlsx"
    nom_fichier_gagnants = "gagnants.xlsx"
    colonne_id_ticket = "numéro_ticket" # Assurez-vous que ce nom de colonne est correct

    # Crée un fichier de participants pour le test s'il n'existe pas
    if not os.path.exists(nom_fichier_participants):
        print(f"Création d'un fichier de test '{nom_fichier_participants}'...")
        test_data = {
            'nom': [f'Participant {i}' for i in range(1, 21)],
            'mail': [f'participant{i}@example.com' for i in range(1, 21)],
            'numéro_ticket': [f'TICKET-{100+i}' for i in range(1, 21)]
        }
        pd.DataFrame(test_data).to_excel(nom_fichier_participants, index=False)


    # Demander à l'utilisateur le nombre de gagnants à tirer
    try:
        nb_gagnants_a_tirer = int(input("Combien de gagnants souhaitez-vous tirer ? "))
        if nb_gagnants_a_tirer > 0:
            tirer_au_sort_excel(
                nom_fichier_participants, nom_fichier_gagnants, nb_gagnants_a_tirer, col_ticket=colonne_id_ticket
            )
        else:
            print("Le nombre de gagnants doit être supérieur à zéro.")
    except ValueError:
        print("Veuillez entrer un nombre entier valide.")