import prefect
from prefect import task, Flow, Parameter
from prefect.tasks.prefect import create_flow_run
from prefect.triggers import all_finished

from tiled.client import from_profile

import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm
import PyHyperScattering

print(f'{PyHyperScattering.__version__}')

RUN_TO_PLOT = 36106

PATH = "/nsls2/data/dssi/scratch/prefect-outputs/rsoxs/"

@task
def write_run_artifacts(RUN_TO_PLOT):
    '''
    Example live-analysis function

    Parameters:
        run_to_plot (int): the local scan id from DataBroker
    '''
    # Prefect logger
    logger = prefect.context.get("logger")
    logger.info("Starting...")

    c = from_profile("nsls2", username=None)
    rsoxsload = PyHyperScattering.load.SST1RSoXSDB(corr_mode='none', catalog=c["rsoxs"]["raw"])
    itp = rsoxsload.loadRun(c["rsoxs"]["raw"][RUN_TO_PLOT],dims=['energy'])

    logger.info("Getting mask")
    if itp.rsoxs_config == 'waxs':
        maskmethod = 'nika'
        mask = '/nsls2/data/sst/legacy/RSoXS/analysis/SST1_WAXS_mask.hdf'
    elif itp.rsoxs_config == 'saxs':
        maskmethod = 'nika'
        mask = '/nsls2/data/sst/legacy/RSoXS/analysis/SST1-SAXS_mask.hdf'
    else:
        maskmethod = 'none'
        warnings.warn(f'Bad rsoxs_config, expected saxs or waxs but found {itp.rsoxs_config}.  This will disable masking and certainly cause issues later.',stacklevel=2)

    logger.info("PFEnergySeriesIntegrator")
    integ = PyHyperScattering.integrate.PFEnergySeriesIntegrator(maskmethod=maskmethod,maskpath=mask,geomethod='template_xr',template_xr=itp,integration_method='csr_ocl')

    name = itp.attrs["sample_name"]

    # DataArray
    logger.info("integrateImageStack")
    integratedimages = integ.integrateImageStack(itp)

    logger.info("Saving Nexus file")
    try:
        integratedimages.fileio.saveNexus(f'{PATH}reduced_{RUN_TO_PLOT}_{name}.nxs')
    except Exception:
        logger.warning("Couldn't save as NeXus file.")

    return integratedimages


@task
def log_status(trigger=all_finished):
    logger = prefect.context.get("logger")
    logger.info("Done!")


with Flow("pyhyper-flow") as flow:
    scan_id = Parameter("scan_id", default=36106)
    da = write_run_artifacts(scan_id)
    log_status(upstream_tasks=[da])

