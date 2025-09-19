import os
import pandas as pd
import numpy as np
import janitor
from loguru import logger

# --- 1. Logging Configuration ---

def setup_logging():
    """Initializes the log file and colored logging."""
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "data_preparation.log")

    logger.remove()  # Remove default handler
    logger.add(
        log_file,
        rotation="1 MB",
        retention="10 days",
        level="INFO",
        encoding="utf-8",
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}",
    )
    logger.add(
        lambda msg: print(msg, end=""),
        level="INFO",
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        colorize=True,
    )
    logger.info("Logging is set up.")

# --- 2. File and Data Loading ---

def find_and_prepare_input_file(input_dir, target_filename="event_registration.xlsx"):
    """
    Finds a single file in the input directory, renames it if necessary,
    and returns the full path to the file.
    """
    logger.info(f"Checking for input file in '{input_dir}'...")
    if not os.path.isdir(input_dir):
        os.makedirs(input_dir)
        logger.error(f"Input directory '{input_dir}' was not found. It has been created. Please add an Excel file and rerun.")
        return None

    files_in_dir = [f for f in os.listdir(input_dir) if os.path.isfile(os.path.join(input_dir, f)) and f.endswith((".xlsx", ".xls"))]

    if len(files_in_dir) == 0:
        logger.error(f"No Excel files found in '{input_dir}'.")
        return None
    elif len(files_in_dir) > 1:
        logger.error(f"Multiple files found in '{input_dir}'. Only one is allowed. Files: {files_in_dir}")
        return None

    original_filename = files_in_dir[0]
    original_filepath = os.path.join(input_dir, original_filename)
    target_filepath = os.path.join(input_dir, target_filename)

    if original_filepath != target_filepath:
        os.rename(original_filepath, target_filepath)
        logger.success(f"File '{original_filename}' renamed to '{target_filename}'.")
    else:
        logger.info(f"File is already named '{target_filename}'.")

    return target_filepath

def load_and_clean_data(file_path):
    """
    Reads an Excel file, cleans column names, removes empty rows/columns,
    and returns a cleaned DataFrame.
    """
    if not file_path:
        return None
    try:
        df = pd.read_excel(file_path)
        logger.info(f"Successfully loaded '{file_path}'.")
        df["numéro_ticket"] = np.arange(1, len(df) + 1)
        df_cleaned = df.clean_names().remove_empty()
        logger.success("Column names cleaned and empty rows/columns removed.")
        return df_cleaned
    except Exception as e:
        logger.exception(f"Failed to load or clean the data from '{file_path}'. Error: {e}")
        return None

# --- 3. Data Processing and Filtering ---

def normalize_ticket_column(df):
    """
    Normalizes the 'ticket_devenement' column to one of three values:
    'Visiteur', 'Benevole', or 'Commanditaire'.
    """
    logger.info("Normalizing 'ticket_devenement' column...")
    if 'ticket_devenement' not in df.columns:
        logger.warning("Colonne 'ticket_devenement' non trouvée.")
        return df

    s = df['ticket_devenement'].astype(str).str.lower()

    conditions = [
        s.str.contains('visiteur'),
        s.str.contains('bénévole|benevole'),
        s.str.contains('commanditaire')
    ]
    choices = ['Visiteur', 'Benevole', 'Commanditaire']
    df['ticket_devenement'] = np.select(conditions, choices, default='Autre')
    
    logger.success(f"Ticket column normalized. Unique values: {df['ticket_devenement'].unique().tolist()}")
    return df

def clean_status_column(df):
    """Cleans the 'status' column by normalizing its values."""
    logger.info("Cleaning 'status' column...")
    df["status"] = df["status"].fillna("absent").astype(str).str.strip()
    df["status"] = df["status"].replace({"Présent": "present", "Inscrit": "inscrit"})
    logger.success(f"Status column cleaned. Unique values: {df['status'].unique().tolist()}")
    return df

def filter_and_save_participants(df, output_dir):
    """
    Filters participants based on the normalized 'ticket_devenement' 
    and 'present' status, then saves them to Excel files.
    """
    if df is None:
        logger.error("Input DataFrame is None. Cannot filter participants.")
        return

    os.makedirs(output_dir, exist_ok=True)
    
    cleaned_output_path = os.path.join(output_dir, "event_registration_cleaned.xlsx")
    df.to_excel(cleaned_output_path, index=False)
    logger.success(f"Full cleaned and processed file saved to '{cleaned_output_path}'.")

    participant_filters = {
        "sponsors": "Commanditaire",
        "benevoles": "Benevole",
        "visiteurs": "Visiteur",
    }

    for role, ticket_name in participant_filters.items():
        logger.info(f"Filtering for present '{role}' (ticket: '{ticket_name}')...")
        
        mask = (df["ticket_devenement"] == ticket_name) & (df["status"] == "present")
        df_present = df[mask]

        count = len(df_present)
        logger.success(f"Found {count} present participants for role '{role}'.")

        output_path = os.path.join(output_dir, f"event_registration_{role}.xlsx")
        df_present.to_excel(output_path, index=False)
        logger.success(f"Saved '{role}' data to '{output_path}'.")

# --- 4. Main Execution ---

def main():
    """Main function to run the data preparation pipeline."""
    setup_logging()
    logger.info("--- Starting Data Preparation Script ---")

    # Déterminer la racine du projet de manière dynamique
    # Le script se trouve dans src/prepare_data, donc la racine est 3 niveaux au-dessus
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    base_dir = project_root
    input_dir = os.path.join(base_dir, "input_files")
    output_dir = os.path.join(base_dir, "output_files")

    input_file = find_and_prepare_input_file(input_dir)
    if not input_file:
        return

    df_cleaned = load_and_clean_data(input_file)
    if df_cleaned is None:
        return
    
    df_normalized = normalize_ticket_column(df_cleaned)
    df_processed = clean_status_column(df_normalized)
    
    filter_and_save_participants(df_processed, output_dir)

    logger.info("--- Data Preparation Script Finished ---")
