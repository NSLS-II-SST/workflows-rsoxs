import sys
import databroker

from pathlib import Path
from tiled.client import from_profile
from prefect import task, Flow, Parameter
from bluesky_darkframes import DarkSubtraction

rsoxs = from_profile("nsls2", username=None)["rsoxs"]
raw = rsoxs["raw"]
processed = rsoxs["sandbox"]


PATH = Path("/nsls2/data/dssi/scratch/prefect-outputs/rsoxs/")


@task
def test_task():
    print("Test task")


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

    fields = [
        "Synced_saxs_image",
        "Synced_waxs_image",
        "Small Angle CCD Detector_image",
        "Wide Angle CCD Detector_image",
    ]

    uid = raw[uid].start['uid']
    primary_data = raw[uid]["primary"]["data"]
    dark_data = raw[uid]["dark"]["data"]
    for field in fields:
        if field in primary_data:
            light = primary_data[field][:]
            dark = dark_data[field][:]
            subtracted = safe_subtract(light, dark.reindex_like(light, "ffill"))
            processed_uid = processed.write_array(subtracted.data.compute(), metadata={"field": field, "python_environment": sys.prefix, "raw_uid": uid})
            for i in range(subtracted.shape[0]):
                processed[processed_uid].export(PATH / f"{uid}-{field}_{i:06}.tif", slice=(i,0))


# Make the Prefect Flow.
# A separate command is needed to register it with the Prefect server.
with Flow("export") as flow:
    uid = Parameter("uid")
    test_task()
    export_task(uid)


def safe_subtract(light, dark):
    return light - dark


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
