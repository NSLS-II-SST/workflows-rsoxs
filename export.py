import json
import sys
from pathlib import Path

import numpy
from prefect import Flow, Parameter, task
from tiled.client import from_profile

tiled_client = from_profile("nsls2", username=None)["rsoxs"]
tiled_client_raw = tiled_client["raw"]
tiled_client_processed = tiled_client["sandbox"]

EXPORT_PATH = Path("/nsls2/data/dssi/scratch/prefect-outputs/rsoxs/")


@task
def write_dark_subtraction(ref):

    """
    This is a Prefect task that perform dark subtraction.

    Subtract dark frame images from the data,
    and write the result to tiled.

    This task is typically triggered manually from the Prefect webapp, but eventually
    it can be triggered by the end_of_run workflow.

    Parameters
    ----------
    ref: string
        This is the reference to the BlueskyRun to be exported. It can be
        a partial uid, a full uid, a scan_id, or an index (e.g. -1).
    """

    # Defining some variable that we will use later in this function.
    run = tiled_client_raw[ref]
    # Note: We need to grab the full uid so we can pair the subtracted
    # data and the raw data together when we write the subtracted data
    # into Tiled.
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
                "raw_uid": full_uid,
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
def tiff_export(ref, processed_uids):

    """
    Export processed data into a tiff file.

    Parameters
    ----------
    ref: string
        Reference to a BlueskyRun. Can be a full uid, a partial uid,
        a scan id, or an index (e.g. -1).

    """
    start = tiled_client_raw[ref].start
    # This is the result of combining 2 streams so we'll set the stream name as primary
    # Maybe we shouldn't use a stream name in the filename at all,
    # but for now we are maintaining backward-compatibility with existing names.
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
                / f"{start['scan_id']}-{start['sample_name']}-{STREAM_NAME}-{field}-{i}.tiff",
                slice=(i, 0),
            )


@task
def csv_export(ref):

    """
    Export each stream as a CSV file.

    - Include only scalar fields (e.g. no images).
    - Put the primary stream at top level with the scan directory
      and put  all other streams in a subdirectory, per Eliot's convention.

    Parameters
    ----------
    ref: string
        Reference to a BlueskyRun. Can be a full uid, a partial uid,
        a scan id, or an index (e.g. -1).

    """
    run = tiled_client_raw[ref]
    start = run.start

    base_directory = EXPORT_PATH / "auto" / start["project_name"]
    base_directory.mkdir(parents=True, exist_ok=True)
    for stream_name, stream in run.items():
        if stream_name != "primary":
            directory = base_directory / f"{start['scan_id']}"
            directory.mkdir(parents=True, exist_ok=True)
        else:
            directory = base_directory
        # TODO Soon Tiled will support
        # >>> stream["data"].search(ArrayNdim(1))
        # to filter it down to only scalar fields.
        # For now we need to filter on the client side.
        scalar_fields = []
        dataset = stream["data"]
        structure = dataset.structure()
        for field in dataset:
            # TODO: seq_num column
            # WARNING: Future Tiled release is highly likely to break this API
            # (but also provide the nicer way to do this described above).
            shape = structure.macro.data_vars[field].macro.variable.macro.shape
            ndim = len(shape)
            if ndim == 1:
                scalar_fields.append(field)
        if set(scalar_fields) == set(dataset):
            scalar_fields = None
        # We need a seq_num column, which the server does not include, so we
        # do export on the client side.
        ds = dataset.read(variables=scalar_fields)
        df = ds.to_dataframe()
        df2 = df.reset_index()  # Promote 'time' from index to column.
        df2.index.name = "seq_num"
        df3 = df2.reset_index()  # Promote 'seq_num' from index to column.
        df3["seq_num"] += 1  # seq_num starts at 1
        df3.to_csv(
            directory / f"{start['scan_id']}-{start['sample_name']}-{stream_name}.csv",
            index=False,
        )


@task
def json_export(ref):

    """
    Export start document into a json file.

    Parameters
    ----------
    ref: string
        Reference to a BlueskyRun. Can be a full uid, a partial uid,
        a scan id, or an index (e.g. -1).

    """
    start = tiled_client_raw[ref].start
    directory = EXPORT_PATH / "auto" / start["project_name"] / f"{start['scan_id']}"
    directory.mkdir(parents=True, exist_ok=True)

    with open(
        directory / f"{start['scan_id']}-{start['sample_name']}.json",
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(start, file, ensure_ascii=False, indent=4)


# Make the Prefect Flow.
# A separate command is needed to register it with the Prefect server.
with Flow("export") as flow:
    ref = Parameter("ref")
    processed_uids = write_dark_subtraction(ref)
    tiff_export(ref, processed_uids)
    csv_export(ref)
    json_export(ref)
