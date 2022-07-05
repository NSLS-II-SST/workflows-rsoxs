import sys
import json
import numpy

from pathlib import Path
from tiled.client import from_profile
from tiled.queries import Eq
from prefect import task, Flow, Parameter


tiled_client = from_profile("nsls2", username=None)["rsoxs"]
tiled_client_raw = tiled_client["raw"]
tiled_client_processed = tiled_client["sandbox"]

EXPORT_PATH = Path("/nsls2/data/dssi/scratch/prefect-outputs/rsoxs/")


@task
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

    # Map field to processed uid to use in other tasks
    processed_uid_dict = {}

    # Write the dark substracted images to tiled.
    for field in found_fields:
        light = primary_data[field][:]
        dark = dark_data[field][:]
        subtracted = safe_subtract(light, dark)
        processed_uid = tiled_client_processed.write_array(
            subtracted.data,
            metadata={
                "field": field,
                "python_environment": sys.prefix,
                "raw_uid": uid,
                "operation": "dark subtraction",
            },
        )
        processed_uid_dict[field] = processed_uid
    return processed_uid_dict


def safe_subtract(light, dark, pedestal=100):
    dark.load()
    light.load()

    dark = dark - pedestal

    dark = numpy.clip(dark, a_min=0, a_max=None)

    return numpy.clip(
        light - dark.reindex_like(light, "ffill"), a_min=0, a_max=None
    ).astype(light.dtype)


@task
def tiff_export(uid, processed_uids):

    """
    Export processed data into a tiff file.

    Parameters
    ----------
    uid: string
        BlueskyRun uid

    """
    start = tiled_client_raw[uid].start
    uid = start["uid"]
    # This is the result of combining 2 streams so we'll set the stream name as primary
    STREAM_NAME = "primary"

    directory = EXPORT_PATH / "auto" / start["project_name"] / f"{start['scan_id']}"
    directory.mkdir(parents=True, exist_ok=True)
    for field, processed_uid in processed_uids.items():
        dataset = tiled_client_processed[processed_uid]
        assert field == dataset.metadata["field"]
        num_frames = len(dataset)
        for i in range(num_frames):
            dataset.export(
                directory
                / f"{start['scan_id']}-{start['sample_name']}-{STREAM_NAME}-{field}-{i:05d}.tif",
                slice=(i, 0),
            )


@task
def csv_export(uid):

    """
    Export processed data into a csv file.

    Parameters
    ----------
    uid: string
        BlueskyRun uid

    """

    uid = tiled_client_raw[uid].start["uid"]

    # Find all of the processed data for a BlueskyRun.
    processed_results = tiled_client_processed.search(Eq("raw_uid", uid))

    # Export a csv for each of the processed datasets
    dataset = processed_results.values().last()
    num_frames = len(dataset)
    for i in range(num_frames):
        dataset.export(
            EXPORT_PATH / f"{uid}-{dataset.metadata['field']}_{i:06}.csv", slice=(i, 0)
        )


@task
def json_export(uid):

    """
    Export start document into a json file.

    Parameters
    ----------
    uid: string
        BlueskyRun uid

    """
    uid = tiled_client_raw[uid].start["uid"]
    start_doc = tiled_client_raw[uid].start
    with open(EXPORT_PATH / f"{uid}.json", "w", encoding="utf-8") as f:
        json.dump(start_doc, f, ensure_ascii=False, indent=4)


# Make the Prefect Flow.
# A separate command is needed to register it with the Prefect server.
with Flow("export") as flow:
    uid = Parameter("uid")
    processed_uids = write_dark_subtraction(uid)
    tiff_export(uid, processed_uids)
    csv_export(uid)
    json_export(uid)

