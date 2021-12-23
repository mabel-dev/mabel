import os
import time
import traceback
import uvicorn  
from fastapi import FastAPI, HTTPException 
from typing import Union
from mabel.logging import get_logger, set_log_name  
from mabel.utils.common import build_context 

# set up context - assumes it is in config.json
context = build_context()

JOB_NAME = context.get("job_name", "")
if len(JOB_NAME or "") < 1:
    JOB_NAME = "SET `job_name` in `config.json`"
# set up logging
set_log_name(JOB_NAME)
logger = get_logger()

# set up API interface
application = FastAPI()


def _update_environent_references(context: Union[dict, str]) -> Union[dict, str]:
    if isinstance(context, dict):
        new_context = context.copy()
        for k, v in new_context.items():
            if isinstance(v, dict):
                new_context[k] = _update_environent_references(v)
            if isinstance(v, list):
                new_list = []
                for item in v:
                    new_list.append(_update_environent_references(item))
                new_context[k] = new_list
        return new_context
    return context


def load_request_parameters_into_context(request, context):
    new_context = context.copy()
    for k, v in request.dict().items():
        new_context[k] = v
    for k in os.environ:
        if k in new_context:
            new_context[k] = os.environ[k]
    return _update_environent_references(new_context)


def serve_request(flow: Flow, my_context: dict) -> dict:  # pragma: no cover
    start_time = time.time()
    work_id = ""
    try:
        work_id = my_context.get("work_id", "no_work_id")
        run_id = my_context.get("run_id")
        logger.debug(f"Started (ID:`{work_id})`")

        with flow as runner:
            runner(data=None, context=my_context)



    except SystemExit as err:
        trace = traceback.format_exc()
        error_message = f"Fatal error executing `{JOB_NAME}` - {type(err).__name__}\n{trace}\n(ID:`{work_id}`)"
        logger.alert(error_message)
        raise HTTPException(status_code=500, detail=error_message)
    except Exception as err:
        trace = traceback.format_exc()
        error_message = f"Error executing `{JOB_NAME}` - {type(err).__name__}\n{trace}\n(ID:`{work_id}`)"
        logger.error(error_message)
        raise HTTPException(status_code=500, detail=error_message)
    finally:
        logger.info(
            {
                "event": "complete",
                "seconds": (time.time() - start_time),
                "request": work_id,
            }
        )
    return {"request": work_id}


def start_server():  # pragma: no cover
    uvicorn.run(
        "internals.core:application",
        host="0.0.0.0",  # nosec - targetting CloudRun
        port=int(os.environ.get("PORT", 8080)),
    )
