from pydantic import BaseModel  # type:ignore


class TaskStartModel(BaseModel):
    """
    This is the interface that services must expose.

    The run_id tracks the execution of a job (a pipeline of tasks), the work_id tracks
    individial tasks in the pipeline.

    The run_id can be looked up from the work_id, but both are sent to avoid ambiguity
    and to allow some degree of validation of the requests - they are only valid if the
    two IDs match valid records in the state database.

    The config item contains information relating to this specific execution of the
    task, for example date ranges, filters and page ids.
    """

    run_id: str = ""
    work_id: str = ""
    config: dict = {}
