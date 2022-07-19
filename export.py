import json
import re
import sys
from pathlib import Path

import httpx
import numpy
from prefect import Flow, Parameter, task
from tiled.client import from_profile

EXPORT_PATH = Path("/nsls2/data/dssi/scratch/prefect-outputs/rsoxs/")

tiled_client = from_profile("nsls2", username=None)["rsoxs"]
tiled_client_raw = tiled_client["raw"]
tiled_client_processed = tiled_client["sandbox"]


def lookup_directory(start_doc):
    """
    Return the path for the proposal directory.

    PASS gives us a *list* of cycles, and we have created a proposal directory under each cycle.
    """
    DATA_SESSION_PATTERN = re.compile("pass-([0-9]+)")

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

    # Map field to processed uid to use in other tasks.
    results = {}

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
                "raw_uid": full_uid,
                "operation": "dark subtraction",
            },
        )
        results[field] = processed_uid
    return results


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

    start = tiled_client_raw[raw_ref].start

    directory = EXPORT_PATH / "auto" / start["project_name"] / f"{start['scan_id']}"
    directory.mkdir(parents=True, exist_ok=True)
    for field, processed_uid in processed_refs.items():
        dataset = tiled_client_processed[processed_uid]
        assert field == dataset.metadata["field"]
        num_frames = len(dataset)
        for i in range(num_frames):
            dataset.export(
                directory
                / f"{start['scan_id']}-{start['sample_name']}-{STREAM_NAME}-{field}-{i}.tiff",
                slice=(i, 0),
            )


@task
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
    base_directory = EXPORT_PATH / "auto" / start["project_name"]
    base_directory.mkdir(parents=True, exist_ok=True)
    stream_directory = base_directory / f"{start['scan_id']}"
    stream_directory.mkdir(parents=True, exist_ok=True)

    def is_scalar(structure, field):
        """
        Checks if a field is scalar.
        """
        # TODO Soon Tiled will support
        # >>> stream["data"].search(ArrayNdim(1))
        # to filter it down to only scalar fields.
        # For now we need to filter on the client side.
        shape = structure.macro.data_vars[field].macro.variable.macro.shape
        return len(shape) == 1

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

        # Figure out the directory to write to.
        scan_directory = f"{start['scan_id']}" if stream_name != "primary" else "."
        directory = base_directory / scan_directory

        # Prepare the data.
        dataset = stream["data"]
        structure = dataset.structure()
        scalar_fields = {field for field in dataset if is_scalar(structure, field)}
        ds = dataset.read(variables=scalar_fields)
        dataframe = add_seq_num(ds)

        # Write the data.
        dataframe.to_csv(
            directory / f"{start['scan_id']}-{start['sample_name']}-{stream_name}.csv",
            index=False,
        )


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
    directory = lookup_directory(start_doc) / f"{start_doc['scan_id']}"
    directory.mkdir(parents=True, exist_ok=True)

    with open(
        directory / f"{start_doc['scan_id']}-{start_doc['sample_name']}.json",
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(start_doc, file, ensure_ascii=False, indent=4)


# Make the Prefect Flow.
# A separate command is needed to register it with the Prefect server.
with Flow("export") as flow:
    raw_ref = Parameter("ref")
    # processed_refs = write_dark_subtraction(raw_ref)
    # tiff_export(raw_ref, processed_refs)
    # csv_export(raw_ref)
    json_export(raw_ref)
