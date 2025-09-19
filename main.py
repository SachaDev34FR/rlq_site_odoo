from src.prepare_data.create_files import main as prepare_data_main
from src.explode_form_responses.pivot_form_responses_participants import pivot_table_and_save_to_excel as pivot_table_export
from src.random_sort_visiteurs.run_lottery import main as random_sort
from loguru import logger


if __name__ == "__main__":
    prepare_data_main()
    pivot_table_export()
    random_sort()
    logger.info("All tasks completed successfully.")