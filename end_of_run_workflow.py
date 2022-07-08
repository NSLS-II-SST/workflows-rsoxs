import prefect
from prefect import Flow, Parameter, task
from prefect.tasks.prefect import create_flow_run


@task
def log_completion():
    logger = prefect.context.get("logger")
    logger.info("Complete")


with Flow("end-of-run-workflow") as flow:

    stop_doc = Parameter("stop_doc")
    uid = stop_doc["run_start"]

    validation_flow = create_flow_run(
        flow_name="general-data-validation",
        project_name="RSoXS",
        parameters={"beamline_acronym": "rsoxs", "uid": uid},
    )

    export_flow = create_flow_run(
        flow_name="export", project_name="RSoXS", parameters={"ref": uid}
    )

    log_completion(upstream_tasks=[validation_flow, export_flow])
