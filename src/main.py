import logging
import os

from ingestion import run_ingestion_pipeline
from classification import run_classification_pipeline
from visualization import launch_dashboard

def main():
    # Configure logging for debugging and tracing
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    logger = logging.getLogger("Main")

    logger.info("Starting the Gene Functional Abstraction workflow.")

    # Step 1: Ingestion Pipeline
    try:
        logger.info("Running ingestion pipeline to process raw data...")
        # Loads raw data, processes it, and exports the processed data to the output folder.
        processed_data = run_ingestion_pipeline()
        logger.info("Ingestion pipeline completed successfully.")
    except Exception as e:
        logger.exception("Error during ingestion pipeline: %s", e)
        return

    # Step 2: Classification Pipeline
    try:
        logger.info("Running classification pipeline to compute the impact index...")
        # Loads the processed CSV and computes the functional impact index.
        classified_data = run_classification_pipeline()
        logger.info("Classification pipeline completed successfully.")
    except Exception as e:
        logger.exception("Error during classification pipeline: %s", e)
        return

    # Step 3: Visualization
    try:
        logger.info("Launching interactive visualization dashboard...")
        launch_dashboard(classified_data)
        logger.info("Visualization dashboard launched successfully.")
    except Exception as e:
        logger.exception("Error during visualization: %s", e)

    # Step 4: API Placeholder for Future Integration
    logger.info("API module placeholder: Future integration for web applications.")

    logger.info("Workflow completed successfully.")

if __name__ == "__main__":
    main()
