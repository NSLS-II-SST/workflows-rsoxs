import sys
import databroker
import json

from pathlib import Path
from tiled.client import from_profile
from tiled.queries import Eq
from prefect import task, Flow, Parameter
from bluesky_darkframes import DarkSubtraction


tiled_client = from_profile("nsls2", username=None)["rsoxs"]
tiled_client_raw = tiled_client["raw"]
tiled_client_processed = tiled_client["sandbox"]

EXPORT_PATH = Path("/nsls2/data/dssi/scratch/prefect-outputs/rsoxs/")


#@task
def write_dark_subtraction(uid):

    """
    This is a Prefect task that perform dark subtraction.

    Subtract dark frame images from the data,
    and write the result to tiled.

    This task is typically triggered manually from the Prefect webapp, but eventually
    it can be triggered by the end_of_run workflow.

    Parameters
    ----------
    uid: string
        This is the uid which will be exported.
    """

    # Defining some variable that we will use later in this function.
    uid = tiled_client_raw[uid].start["uid"]
    run = tiled_client_raw[uid]
    primary_data = run["primary"]["data"]
    dark_data = run["dark"]["data"]

    # The set of fields that should be exported if found in the scan.
    export_fields = {
        "Synced_saxs_image",
        "Synced_waxs_image",
        "Small Angle CCD Detector_image",
        "Wide Angle CCD Detector_image",
    }

    # The set of fields for the primary data set.
    primary_fields = set(run["primary"]["data"])

    # The export_fields that are found in the primary dataset.
    found_fields = export_fields & primary_fields

    # Write the dark substracted images to tiled.
    for field in found_fields:
        light = primary_data[field][:]
        dark = dark_data[field][:]
        subtracted = light - dark.reindex_like(light, "ffill")
        processed_uid = tiled_client_processed.write_array(
            subtracted.data.compute(),
            metadata={"field": field, "python_environment": sys.prefix, "raw_uid": uid},
        )


#@task
def tiff_export(uid):

    """
    Export processed data into a tiff file.

    Parameters
    ----------
    uid: string
        BlueskyRun uid

    """

    uid = tiled_client_raw[uid].start["uid"]

    # Find all of the processed data for a BlueskyRun.
    processed_results = tiled_client_processed.search(Eq('raw_uid', uid))

    # Export a tiff for each of the processed datasets
    dataset = processed_results.values().last()
    num_frames = len(dataset)
    for i in range(num_frames):
        processed_results[dataset].export(
        EXPORT_PATH / f"{uid}-{processed_results[dataset].metadata['field']}_{i:06}.tif", slice=(i, 0)
    )


#@task
def csv_export(uid):

    """
    Export processed data into a tiff file.

    Parameters
    ----------
    uid: string
        BlueskyRun uid

    """

    uid = tiled_client_raw[uid].start["uid"]
    
    # Find all of the processed data for a BlueskyRun.
    processed_results = tiled_client_processed.search(Eq('raw_uid', uid))

    # Export a tiff for each of the processed datasets
    dataset = processed_results.values().last()
    num_frames = len(dataset)
    for i in range(num_frames):
        dataset.export(
        EXPORT_PATH / f"{uid}-{dataset.metadata['field']}_{i:06}.csv", slice=(i, 0)
    )


#@task
def json_export(uid):

    """
    Export processed data into a tiff file.

    Parameters
    ----------
    uid: string
        BlueskyRun uid

    """
    uid = tiled_client_raw[uid].start["uid"]
    start_doc = tiled_client_raw[uid].start   
    with open(EXPORT_PATH / f'{uid}.json', 'w', encoding='utf-8') as f:
        json.dump(start_doc, f, ensure_ascii=False, indent=4)


#Make the Prefect Flow.
# A separate command is needed to register it with the Prefect server.
#with Flow("export") as flow:
#    uid = Parameter("uid")
#    write_dark_subtraction(uid)
#    tiff_export(uid)
#    csv_export(uid)
#    json_export(uid)


#@task
def validate(uid):

    """
    Validate tiff fikes.

    Parameters
    ----------
    uid: string
        BlueskyRun uid

    """


#    logger = prefect.context.get("logger")
#
#    # Connect to the tiled server, and get the set of runs from the set of uids.
#    tiled_client = databroker.from_profile("nsls2", username=None)["rsoxs"]["raw"]
#    run = tiled_client[uid]
#
#    # Add task #1 below.
#    logger.info(f"Dark subtraction: {uid}")
#    dark_subtraction(run)
#    logger.info("Dark subtraction is completed")
#
#    # Add task #2 below etc.
#    logger.info(f"Dark subtraction: {uid}")
#    tiff_export(run)
#    logger.info("Tiff export is completed")
#
#    # Add validation task below
#    logger.info(f"Validating ....")
#    validate(run)
#    logger.info("Validation complete.")
#
#    logger.info("#### Add custom message here ####")
