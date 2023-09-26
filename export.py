import json
import re
import sys
import httpx
import numpy

from pathlib import Path
from prefect import flow, task, get_run_logger
from tiled.client import from_profile, show_logs


EXPORT_PATH = Path("/nsls2/data/dssi/scratch/prefect-outputs/rsoxs/")

show_logs()
tiled_client = from_profile("nsls2")["rsoxs"]
tiled_client_raw = tiled_client["raw"]
tiled_client_processed = tiled_client["sandbox"]


def lookup_directory(start_doc):
    """
    Return the path for the proposal directory.

    PASS gives us a *list* of cycles, and we have created a proposal directory under each cycle.
    """
    DATA_SESSION_PATTERN = re.compile("[GUPCpass]*-([0-9]+)")
    client = httpx.Client(base_url="https://api-staging.nsls2.bnl.gov")
    data_session = start_doc[
        "data_session"
    ]  # works on old-style Header or new-style BlueskyRun

    try:
        digits = int(DATA_SESSION_PATTERN.match(data_session).group(1))
    except AttributeError:
        raise AttributeError(f"incorrect data_session: {data_session}")

    response = client.get(f"/proposal/{digits}/directories")
    response.raise_for_status()

    paths = [path_info["path"] for path_info in response.json()]

    # Filter out paths from other beamlines.
    paths = [path for path in paths if "sst" == path.lower().split("/")[3]]

    # Filter out paths from other cycles and paths for commisioning.
    paths = [
        path
        for path in paths
        if path.lower().split("/")[5] == "commissioning"
        or path.lower().split("/")[5] == start_doc["cycle"]
    ]

    # There should be only one path remaining after these filters.
    # Convert it to a pathlib.Path.
    return Path(paths[0])


@task
def write_dark_subtraction(ref):
    """
    This is a Prefect task that perform dark subtraction.

    Subtract dark frame images from the data,
    and write the result to tiled.

    Parameters
    ----------
    ref: string
        This is the reference to the BlueskyRun to be exported. It can be
        a partial uid, a full uid, a scan_id, or an index (e.g. -1).

    Returns
    -------
    results: dict
        A dictionary that maps field_name to the matching processed uid.
    """

    logger = get_run_logger()
    logger.info("starting dark subtraction")

    def safe_subtract(light, dark, pedestal=100):
        """
        Subtract a dark_frame from a light_frame.
        Parameters
        ----------
        light: array
            This is the light_frame.
        dark: array
            This is the dark_frame.
        pededtal: integer
            An offset to avoid negative results.
        """

        dark.load()
        light.load()

        dark = dark - pedestal

        dark = numpy.clip(dark, a_min=0, a_max=None)

        return numpy.clip(
            light - dark.reindex_like(light, "ffill"), a_min=0, a_max=None
        ).astype(light.dtype)

    run = tiled_client_raw[ref]

    full_uid = run.start["uid"]
    logger.info(f"{full_uid = }")

    # Raise an exception if the dark stream isn't present
    if "dark" not in run:
        logger.warning(
            "dark stream does not exist in this run. Skipping dark subtraction and tiff export."
        )
        return
        # raise Exception("dark stream does not exist")

    # Access the primary and dark streams as xarray.Datasets.
    primary_data = run["primary"]["data"].read()
    dark_data = run["dark"]["data"].read()

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

    # Map field to processed uid to use in other tasks.
    results = {}

    # Write the dark substracted images to tiled.
    for field in found_fields:
        light = primary_data[field][:]
        dark = dark_data[field][:]
        subtracted = safe_subtract(light, dark)
        processed_array_client = tiled_client_processed.write_array(
            subtracted.data,
            metadata={
                "field": field,
                "python_environment": sys.prefix,
                "raw_uid": full_uid,
                "operation": "dark subtraction",
            },
        )
        results[field] = processed_array_client.item["id"]

    logger.info("completed dark subtraction")

    return results


# Make sure this only runs when the dark subtraction is successful
@task
def tiff_export(raw_ref, processed_refs):
    """
    Export processed data into a tiff file.

    Parameters
    ----------
    raw_ref: string
        Reference to a BlueskyRun. Can be a full uid, a partial uid,
        a scan id, or an index (e.g. -1).
    processed_refs: dict
        A dictionary that maps field_name to the matching processed_ref.
    """

    # This is the result of combining 2 streams so we'll set the stream name as primary
    # Maybe we shouldn't use a stream name in the filename at all,
    # but for now we are maintaining backward-compatibility with existing names.
    STREAM_NAME = "primary"

    start_doc = tiled_client_raw[raw_ref].start
    directory = (
        lookup_directory(start_doc)
        / start_doc["project_name"]
        / f"{start_doc['scan_id']}"
    )
    directory.mkdir(parents=True, exist_ok=True)

    logger = get_run_logger()
    logger.info(f"starting tiff export to {directory}")

    for field, processed_uid in processed_refs.items():
        dataset = tiled_client_processed[processed_uid]
        assert field == dataset.metadata["field"]
        num_frames = len(dataset)
        for i in range(num_frames):
            filename = f"{start_doc['scan_id']}-{start_doc['sample_name']}-{STREAM_NAME}-{field}-{i}.tiff"
            logger.info(f"Exporting {filename}")
            dataset.export(directory / filename, slice=(i))

    logger.info(f"wrote tiff files to: {directory}")


# Retry this task if it fails
@task(retries=2, retry_delay_seconds=10)
def csv_export(raw_ref):
    """
    Export each stream as a CSV file.

    - Include only scalar fields (e.g. no images).
    - Put the primary stream at top level with the scan directory
      and put  all other streams in a subdirectory, per Eliot's convention.

    Parameters
    ----------
    raw_ref: string
        Reference to a BlueskyRun. Can be a full uid, a partial uid,
        a scan id, or an index (e.g. -1).

    """

    run = tiled_client_raw[raw_ref]
    start = run.start

    # Make the directories.
    start_doc = tiled_client_raw[raw_ref].start
    base_directory = lookup_directory(start_doc) / start_doc["project_name"]
    base_directory.mkdir(parents=True, exist_ok=True)

    logger = get_run_logger()
    logger.info(f"starting csv export to {base_directory}")

    def add_seq_num(dataset):
        """
        Add a seq_num collumn to the dataset.
        This also converts the dataset to a dataframe.

        We need a seq_num column, which the server does not include, so we
        do export on the client side.
        """

        df = dataset.to_dataframe()
        df2 = df.reset_index()  # Promote 'time' from index to column.
        df2.index.name = "seq_num"
        df3 = df2.reset_index()  # Promote 'seq_num' from index to column.
        df3["seq_num"] += 1  # seq_num starts at 1
        return df3

    for stream_name, stream in run.items():
        logger.info(f"Exporting csv for stream {stream_name}")

        # Figure out the directory to write to.
        scan_directory = f"{start['scan_id']}" if stream_name != "primary" else "."
        directory = base_directory / scan_directory
        directory.mkdir(parents=True, exist_ok=True)

        # Prepare the data.
        dataset = stream["data"]
        scalar_fields = {field for field in dataset if dataset[field].ndim == 1}
        ds = dataset.read(variables=scalar_fields)
        dataframe = add_seq_num(ds)

        # Write the data.
        dataframe.to_csv(
            directory / f"{start['scan_id']}-{start['sample_name']}-{stream_name}.csv",
            index=False,
        )

    logger.info(f"wrote csv files to: {directory}")


@task
def json_export(raw_ref):
    """
    Export start document into a json file.

    Parameters
    ----------
    raw_ref: string
        Reference to a BlueskyRun. Can be a full uid, a partial uid,
        a scan id, or an index (e.g. -1).

    """
    start_doc = tiled_client_raw[raw_ref].start
    directory = (
        lookup_directory(start_doc)
        / start_doc["project_name"]
        / f"{start_doc['scan_id']}"
    )
    directory.mkdir(parents=True, exist_ok=True)

    logger = get_run_logger()
    logger.info(f"starting json export to {directory}")

    with open(
        directory / f"{start_doc['scan_id']}-{start_doc['sample_name']}.json",
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(start_doc, file, ensure_ascii=False, indent=4)

    logger.info(
        f'wrote json file to: {str(directory / str(start_doc["scan_id"]))}-{start_doc["sample_name"]}.json'
    )


# Make the Prefect Flow.
# A separate command is needed to register it with the Prefect server.
@flow
def export(ref):
    csv_export(ref)
    json_export(ref)
    processed_refs = write_dark_subtraction(ref)
    if processed_refs:
        tiff_export(ref, processed_refs)


# This line will mark this flow as succeeded based on
# the csv and json export tasks succeeding.
# TODO: Do we need this line?
# flow.set_reference_tasks([csv_export_task, json_export_task])
