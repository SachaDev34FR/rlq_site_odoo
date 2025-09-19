import os
from datetime import datetime

import pandas as pd
from loguru import logger


def load_and_fill_na_data(file_path):
    """_summary_

    Args:
        file_path (_type_): _description_

    Returns:
        _type_: _description_
        
    """

    
    # Charger les données
    df = pd.read_excel(file_path)

    # Afficher les premières lignes pour vérifier le chargement
    logger.info("Aperçu des données chargées :")
    logger.info(df.head())
    logger.info(f"Colonnes initiales : {df.columns.tolist()}")
    logger.info(f"Nombre de lignes avant remplissage : {len(df)}")
    
    
    
    # Remplir les valeurs NaN en utilisant la méthode 'ffill' (forward fill)
    df_filled = df.fillna(method='ffill')

    return df_filled


def group_data_by_nam_email(df_filled):
    """_summary_

    Args:
        df_filled (_type_): _description_

    Returns:
        _type_: _description_
    """
    df_responses = df_filled[
        ["nom_du_participant", "email", "reponses_des_participants"]
    ].copy()
    df_responses.dropna(subset=["reponses_des_participants"], inplace=True)
    df_responses["num_reponse"] = df_responses.groupby(
        ["nom_du_participant", "email"]
    ).cumcount()

    return df_responses


def pivot_responses(df_responses):
    """_summary_

    Args:
        df_responses (_type_): _description_

    Returns:
        _type_: _description_
    """
    # Pivotement pour transformer les réponses en colonnes distinctes
    df_pivot = df_responses.pivot_table(
        index=["nom_du_participant", "email"],
        columns="num_reponse",
        values="reponses_des_participants",
        aggfunc="first",
    ).reset_index()

    # Renommer les colonnes pour une meilleure lisibilité
    rename_dict = {col: f"reponse_{int(col) + 1}" for col in df_pivot.columns if isinstance(col, int)}
    df_pivot.rename(columns=rename_dict, inplace=True)

    logger.info("--- Tableau après pivot et renommage ---")
    logger.info(df_pivot.head())

    return df_pivot


def pivot_table_and_save_to_excel():
    """_summary_

    Args:
        df_pivot (_type_): _description_
        output_file (_type_): _description_
    """
    base_dir = "/media/sacha/DATA_P21/projet_RLQ_Odoo_test"
    output_dir = os.path.join(base_dir, "output_files")
    output_file = os.path.join(output_dir, "event_registration_cleaned.xlsx")
    output_file_pivot = os.path.join(output_dir,
                                    "event_registration_pivot.xlsx")
    
    if output_file:
        df_filled = load_and_fill_na_data(output_file)      
        df_responses = group_data_by_nam_email(df_filled)    
        df_pivot = pivot_responses(df_responses)
        df_pivot.to_excel(
                        output_file_pivot 
                        + str(datetime.now().strftime("%H-%M-%S"))
                        + ".xlsx",
                        index=False
                        )
        logger.info(f"Saved pivoted data to '{output_file_pivot}'.")
