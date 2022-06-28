import databroker
import json
import os
import prefect


@task
def export_task(uid):

   """
    This is a Prefect task that perform dark subtraction.

    This task is typically triggered manually from the Prefect webapp, but eventually
    it can be triggered by the end_of_run workflow.

    Parameters
    ----------
    uid: string
        This is the uid which will be exported.
    """

    logger = prefect.context.get("logger")

    # Connect to the tiled server, and get the set of runs from the set of uids.
    tiled_client = databroker.from_profile("nsls2", username=None)["rsoxs"]["raw"]
    run = tiled_client[uid]

    # Add task #1 below.
    logger.info(f"Dark subtraction: {uid}")
    dark_subtraction(run)
    logger.info("Dark subtraction is completed")

    # Add task #2 below etc.
    logger.info(f"Dark subtraction: {uid}")
    tiff_export(run)
    logger.info("Tiff export is completed")

    # Add validation task below
    logger.info(f"Validating ....")
    validate(run)
    logger.info("Validation complete.")

    logger.info("#### Add custom message here ####")


# Make the Prefect Flow.
# A separate command is needed to register it with the Prefect server.
with Flow("export") as flow:
    uid = Parameter("uid")
    export_task(uid)


def dark_subtraction(run):

    """
    Subtract dark frame images from the data.

    Parameters
    ----------
    run: BlueskyRun

    """



def tiff_export(run):

    """
    Export processed data into a tiff file.

    Parameters
    ----------
    run: BlueskyRun

    """


def validate(run):

    """
    Validate tiff fikes.

    Parameters
    ----------
    run: BlueskyRun

    """
