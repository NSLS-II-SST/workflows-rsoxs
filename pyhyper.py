import re
import warnings
import pyFAI
from pathlib import Path
#test
import httpx
import prefect
import PyHyperScattering
from prefect import Flow, Parameter, task
from prefect.triggers import all_finished
from tiled.client import from_profile

PATH = "/nsls2/data/dssi/scratch/prefect-outputs/rsoxs/"

DATA_SESSION_PATTERN = re.compile("[passGUCP]*-([0-9]+)")


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
def write_run_artifacts(scan_id):
    """
    Example live-analysis function

    Parameters:
        run_to_plot (int): the local scan id from DataBroker
    """
    start_doc = tiled_client_raw[scan_id].start
    directory = (
            lookup_directory(start_doc)
            / start_doc["project_name"]
            / f"{start_doc['scan_id']}"
    )
    directory.mkdir(parents=True, exist_ok=True)

    logger = prefect.context.get("logger")
    logger.info(f"starting pyhyper export to {directory}")

    logger.info(f"{PyHyperScattering.__version__}")

    c = from_profile("nsls2", username=None)
    logger.info("Loaded RSoXS Profile...")


    logger.info("created RSoXS catalog loader...")

    # except Exception:
    #    logger.warning("Couldn't save as NeXus file.")
    logger.info("Done!")
    return integratedimages


@task
def log_status(trigger=all_finished):
    logger = prefect.context.get("logger")
    logger.info("Done!")


with Flow("pyhyper-flow") as flow:
    scan_id = Parameter("scan_id", default=36106)
    da = write_run_artifacts(scan_id)

    # check start document if pyhyper reduction is needed
    log_status(upstream_tasks=[da])
