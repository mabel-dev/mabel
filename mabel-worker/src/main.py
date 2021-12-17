from internals import core
from internals.models import TaskStartModel
from fastapi.responses import ORJSONResponse


def example_flow(context):
    from mabel.operators import NoOpOperator, EndOperator  # type:ignore

    return NoOpOperator() > EndOperator()


@core.application.post("/START", response_class=ORJSONResponse)
def handle_start_request(request: TaskStartModel):
    my_context = core.load_request_parameters_into_context(request, core.context)
    flow = example_flow(my_context)
    response = core.serve_request(flow, my_context)
    return response


# tell the server to start
if __name__ == "__main__":
    core.start_server()
