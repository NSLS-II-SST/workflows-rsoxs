import re
from pathlib import Path

import httpx
import PyHyperScattering
from prefect import flow, get_run_logger, task
from tiled.client import from_profile
from tiled.client.xarray import write_xarray_dataset

PATH = "/nsls2/data/dssi/scratch/prefect-outputs/rsoxs/"

DATA_SESSION_PATTERN = re.compile("[passGUCP]*-([0-9]+)")


@task
def load_and_reduce(
    scanid, override_bcx=None, override_bcy=None, override_dist=None, override_mask=None
):
    scan = load.loadRun(scanid)
    scan_us = scan.unstack("system")  # this is expensive, do it only once
    if "energy" in scan_us.dims:
        integrator = PyHyperScattering.integrate.PFEnergySeriesIntegrator
    else:
        integrator = PyHyperScattering.integrate.PFGeneralIntegrator
    integrator = integrator(
        integration_method="csr", geomethod="template_xr", template_xr=scan
    )
    if override_bcx is not None:
        integrator.ni_beamcenter_x = override_bcx
    if override_bcy is not None:
        integrator.ni_beamcenter_y = override_bcy
    if override_dist is not None:
        integrator.ni_distance = override_dist
    if override_mask is not None:
        integrator.mask = override_mask
    return (
        integrator.integrateImageStack(scan)
        .to_dataset(name="reduced")
        .unstack("system")
    )


load = PyHyperScattering.load.SST1RSoXSDB(corr_mode="none")
writable_tiled = from_profile("rsoxs")["reduced_sandbox"]


@task
def load_reduce_and_write_to_tiled(scanid):
    logger = get_run_logger()
    try:
        scan = load_and_reduce(scanid)
    except Exception as e:
        logger.warning(f"Exception during reduction {e}")
    write_xarray_dataset(writable_tiled, scan)
    return scan


"""
def auto_reduce_recent_data_if_not_reduced(number):
    for scan_ref in range (-number,-1):
        try:
            if load.c[scan_ref].stop is not None: # if the scan is currently running, stop will be None.
                local_scan_id = load.c[scan_ref].metadata['summary']['scan_id']
                if len(analyzed_tiled.search(Eq('attrs.start.scan_id',local_scan_id))) > 0:
                    continue
                load_reduce_and_write_to_tiled(scan_ref)
        except Exception as e:
            print(f'error in processing {scan_ref}, {e}')

"""


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


@flow
def pyhyper_flow(scan_id=36106):
    load_reduce_and_write_to_tiled(scan_id)
    # scan = load_reduce_and_write_to_tiled(scan_id)
    # TODO: decide and save these artifacts write_run_artifacts(scan)
    log_status()


@task
def log_status():
    logger = get_run_logger()
    logger.info("Done!")
