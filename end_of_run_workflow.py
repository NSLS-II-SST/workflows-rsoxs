import prefect
from prefect import Flow, Parameter, task
from prefect.tasks.prefect import create_flow_run

# from tiled.client import from_profile


@task
def log_completion():
    logger = prefect.context.get("logger")
    logger.info("Complete")


with Flow("end-of-run-workflow") as flow:

    stop_doc = Parameter("stop_doc")
    uid = stop_doc["run_start"]
    upstream_tasks = []

    validation_flow = create_flow_run(
        flow_name="general-data-validation",
        project_name="RSoXS",
        parameters={"beamline_acronym": "rsoxs", "uid": uid},
    )
    upstream_tasks.append(validation_flow)

    # if energy and and image are in the primary fields, do pyHyper
    export_flow = create_flow_run(
        flow_name="export", project_name="RSoXS", parameters={"ref": uid}
    )
    upstream_tasks.append(export_flow)

    # here is where to add check of start docuyment to see what kind of analysis is useful
    # tiled_client = from_profile("nsls2", username=None)["rsoxs"]
    # tiled_client_raw = tiled_client["raw"]
    # run = tiled_client_raw[uid]

    # The set of fields that should be exported if found in the scan.
    # image_fields = {
    #    "Synced_saxs_image",
    #    "Synced_waxs_image",
    #    "Small Angle CCD Detector_image",
    #    "Wide Angle CCD Detector_image",
    # }

    # The set of fields for the primary data set.
    # primary_fields = set(run["primary"]["data"])

    # The export_fields that are found in the primary dataset.
    # found_fields = image_fields & primary_fields

    # if found_fields and 'en_energy' in primary_fields:
    #    pyhyper_flow = create_flow_run(
    #        flow_name="pyhyper-flow", project_name="RSoXS", parameters={"scan_id": uid}
    #    )
    #    upstream_tasks.append(pyhyper_flow)

    log_completion(upstream_tasks=upstream_tasks)
