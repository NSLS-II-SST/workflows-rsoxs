import warnings
import httpx
import json
import re
import sys
from pathlib import Path
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
    client = httpx.Client(base_url="https://api-staging.nsls2.bnl.gov")
    data_session = start_doc["data_session"]  # works on old-style Header or new-style BlueskyRun

    try:
        digits = int(DATA_SESSION_PATTERN.match(data_session).group(1))
    except AttributeError:
        raise AttributeError(f"incorrect data_session: {data_session}")

    response = client.get(f"/proposal/{digits}/directories")
    response.raise_for_status()

    paths = [path_info['path'] for path_info in response.json()]

    # Filter out paths from other beamlines.
    paths = [path for path in paths if 'sst' == path.lower().split('/')[3]]

    # Filter out paths from other cycles and paths for commisioning.
    paths = [path for path in paths
             if path.lower().split('/')[5] == 'commissioning'
             or path.lower().split('/')[5] == start_doc['cycle']]

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
    # Prefect logger
    logger = prefect.context.get("logger")
    logger.info("Starting...")
    logger.info(f"{PyHyperScattering.__version__}")

    c = from_profile("nsls2", username=None)
    logger.info("Loaded RSoXS Profile...")
    rsoxsload = PyHyperScattering.load.SST1RSoXSDB(
        corr_mode="none", catalog=c["rsoxs"]["raw"]
    )

    logger.info("created RSoXS catalog loader...")
    itp = rsoxsload.loadRun(c["rsoxs"]["raw"][scan_id], dims=["energy"])

    logger.info("Getting mask")
    if itp.rsoxs_config == "waxs":
        maskmethod = "nika"
        mask = "/nsls2/data/sst/legacy/RSoXS/analysis/SST1_WAXS_mask.hdf"
    elif itp.rsoxs_config == "saxs":
        maskmethod = "nika"
        mask = "/nsls2/data/sst/legacy/RSoXS/analysis/SST1-SAXS_mask.hdf"
    else:
        maskmethod = "none"
        warnings.warn(
            f"Bad rsoxs_config, expected saxs or waxs but found {itp.rsoxs_config}. "
            "This will disable masking and certainly cause issues later.",
            stacklevel=2,
        )

    logger.info("PFEnergySeriesIntegrator")
    integ = PyHyperScattering.integrate.PFEnergySeriesIntegrator(
        maskmethod=maskmethod,
        maskpath=mask,
        geomethod="template_xr",
        template_xr=itp,
        integration_method="csr_ocl",
    )

    name = itp.attrs["sample_name"]

    # DataArray
    logger.info("integrateImageStack")
    # add a data check if this is the right format at all
    integratedimages = integ.integrateImageStack(itp)

    logger.info("Saving Nexus file")

    data_path = lookup_directory(c['rsoxs']['raw'].start)
    logger.info(f'writing to {data_path}')
    #try:
    integratedimages.fileio.saveNexus(f"{PATH}reduced_{scan_id}_{name}.nxs")
    #except Exception:
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
