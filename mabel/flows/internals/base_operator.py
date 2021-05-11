import re
import abc  # abstract base class library
import sys
import time
import inspect
import hashlib
import datetime
import functools
from typing import List
from ...logging import get_logger  # type:ignore
from ...errors import render_error_stack
from ...data.formats.json import parse, serialize


# inheriting ABC is part of ensuring that this class only ever
# interited from
class BaseOperator(abc.ABC):

    @staticmethod
    @functools.lru_cache(1)
    def sigterm():
        """
        Create a termination signal - SIGTERM, when the data record is
        this, we do any closure activities.

        Change this each cycle to reduce the chances of it being sent
        accidently (it shouldn't but what could happen is the payload
        is saved from one job and fed to another)
        """
        full_hash = hashlib.sha256(datetime.datetime.now().isoformat().encode())
        return "SIGTERM-" + full_hash.hexdigest()

    def __init__(self, **kwargs):
        """
        All Operators should inherit from this class, it will help ensure a
        common structure to Operator classes and provide some common
        functionality and interfaces.

        This Base Operator is relatively complex but serves to simplify the
        implementation of functional Operators so engineers can focus on
        handling data, not getting a pipeline to work.

        You are expected to override the execute method with the logic of the
        Operator, this should return None, if the pipeline should terminate
        at this Operator (for example, if the Operator filters records),
        return a tuple of (data, context), or a generator/list of (data,
        context).

        Parameters:
            retry_count: integer (optional)
                The number of times to attempt a Operator before aborting,
                this defaults to 2 and is limited between 1 and 5
            retry_wait: integer (optional)
                The number of seconds to wait between retries, this defaults
                to 5 and is limited between 1 and 300
            rolling_failure_window: integer (optional)
                The number of previous Operators to remember the 
                success/failure of, when >50% of the Operators in this
                window are failures, the job aborts. This defaults to 10 and is
                limited between 1 (single failure aborts) and 100
        """
        self.flow = None
        self.records_processed = 0      # number of times this Operator has been run
        self.execution_time_ns = 0      # nano seconds of cpu execution time
        self.errors = 0                 # number of errors
        self.commencement_time = None   # the time processing started
        self.logger = get_logger()      # get the mabel logger

        self.name = self.__class__.__name__

        # read retry settings, clamp values to practical ranges
        self.retry_count = self._clamp(kwargs.get('retry_count', 2), 1, 5)
        self.retry_wait = self._clamp(kwargs.get('retry_wait', 5), 1, 300)
        rolling_failure_window = self._clamp(kwargs.get('rolling_failure_window', 10), 1, 100)
        self.last_few_results = [1] * rolling_failure_window  # track the last n results

        # Log the hashes of the __call__ and version methods
        call_hash = self._hash(inspect.getsource(self.__call__))[-12:]    
        version_hash = self._hash(inspect.getsource(self.version))[-12:]
        self.logger.audit({
                "operator": self.name,
                "call_hash": call_hash,
                "version_hash": self.version(),
                "version_method": version_hash })


    @abc.abstractmethod
    def execute(self, data: dict = {}, context: dict = {}):
        """
        YOU MUST OVERRIDE THIS METHOD

        This is where the main logic for the Operator is implemented.
        
        Parameters:
            data: Dictionary (or Any)
                The data to be processed, the Base Operator is opinionated
                for the data to be a dictionary, but any data type will work
            context: Dictionary
                Information to support the exeuction of the Operator

        Returns:
            None
                Do not continue further through the flow
            Tuple (data, context)
                The data to pass to the next Operator
            Iterable(data, context)
                Run the next Operator multiple times
        """
        pass  # pragma: no cover

    def finalize(self, context: dict = None):
        """
        Any code to finalize an operator should be implemented here.

        This should return the context to pass along
        """
        if not context:
            context = {}
        return context

    def __call__(self, data: dict = {}, context: dict = {}):
        """
        DO NOT OVERRIDE THIS METHOD

        This method wraps the `execute` method, which must be overridden, to
        to add management of the execution such as sensors and retries.
        """
        if data == self.sigterm():
            outcome = None
            try:
                outcome = self.finalize(context)
            except Exception as err:
                self.logger.error(F"Problem finalizing {self.name} - {type(err).__name__} - {err} - {context.get('run_id')}")
            finally:
                if not outcome:
                    outcome = context
            return self.sigterm(), outcome

        if self.commencement_time is None:
            self.commencement_time = datetime.datetime.now()
        self.records_processed += 1
        attempts_to_go = self.retry_count
        while attempts_to_go > 0:
            try:
                start_time = time.perf_counter_ns()
                outcome = self.execute(data, context)
                my_execution_time = time.perf_counter_ns() - start_time
                self.execution_time_ns += my_execution_time
                # add a success to the last_few_results list
                self.last_few_results.append(1)
                self.last_few_results.pop(0)
                break
            except Exception as err:
                self.errors += 1
                attempts_to_go -= 1
                if attempts_to_go:
                    self.logger.error(F"{self.name} - {type(err).__name__} - {err} - retry in {self.retry_wait} seconds ({context.get('run_id')})")
                    time.sleep(self.retry_wait)
                else:
                    error_log_reference = ''
                    error_reference = err
                    try:
                        error_payload = (
                                F"timestamp  : {datetime.datetime.today().isoformat()}\n"
                                F"operator   : {self.name}\n"
                                F"error type : {type(err).__name__}\n"
                                F"details    : {err}\n"
                                "------------------------------------------------------------------------------------------------------------------------\n"
                                F"{self._wrap_text(render_error_stack(), 120)}\n"
                                "-------------------------------------------------------  context  ------------------------------------------------------\n"
                                F"{self._wrap_text(str(context), 120)}\n"
                                "--------------------------------------------------------  data  --------------------------------------------------------\n"
                                F"{self._wrap_text(str(data), 120)}\n"
                                "------------------------------------------------------------------------------------------------------------------------\n")
                        error_log_reference = self.error_writer(error_payload)  # type:ignore
                    except Exception as err:
                        self.logger.error(F"Problem writing to the error bin, a record has been lost. {self.name}, {type(err).__name__} - {err} - {context.get('run_id')}")
                    finally:
                        # finally blocks are called following a try/except block regardless of the outcome
                        self.logger.alert(F"{self.name} - {type(error_reference).__name__} - {error_reference} - tried {self.retry_count} times before aborting ({context.get('run_id')}) {error_log_reference}")
                    outcome = None
                    # add a failure to the last_few_results list
                    self.last_few_results.append(0)
                    self.last_few_results.pop(0)

        # message tracing
        if context.get('trace', False):
            data_hash = self._hash(data)
            context['execution_trace'].add_block(data_hash=data_hash,
                                                 operator=self.name,
                                                 operator_version=self.version(),
                                                 execution_ns=my_execution_time,
                                                 data_block=serialize(data))
            self.logger.audit(F"{context.get('run_id')} {self.name} {data_hash}")

        # if there is a high failure rate, abort
        if sum(self.last_few_results) < (len(self.last_few_results) / 2):
            self.logger.alert(F"Failure Rate for {self.name} over last {len(self.last_few_results)} executions is over 50%, aborting.")
            sys.exit(1)

        return outcome

    def read_sensors(self):
        """
        Format data about the transformation, this can be overridden but it
        should include this information
        """
        response = {
            "operator": self.name,
            "version": self.version(),
            "records_processed": self.records_processed,
            "error_count": self.errors,
            "execution_sec": self.execution_time_ns / 1e9
        }
        if self.records_processed == 0:
            self.logger.warning(F"{self.name} processed 0 records")
        if self.commencement_time:
            response['commencement_time'] = self.commencement_time.isoformat()
        return response

    @functools.lru_cache(1)
    def version(self):
        """
        DO NOT OVERRIDE THIS METHOD.

        The version of the Operator code, this is intended to facilitate
        reproducability and auditability of the pipeline. The version is the
        last 12 characters of the hash of the source code of the 'execute'
        method. This removes the need for the developer to remember to
        increment a version variable.

        Hashing isn't security sensitive here, it's to identify changes
        rather than protect information.
        """
        source = inspect.getsource(self.execute)
        source = self._only_alpha_nums(source)
        full_hash = hashlib.sha256(source.encode())
        return full_hash.hexdigest()[-12:]

    def __del__(self):
        # do nothing - prevents errors if someone thinks they're being a good
        # citizen and calls super().__del__
        pass

    def error_writer(self, record):
        # this is a stub to be overridden
        raise ValueError('no error_writer attached')

    def __gt__(self, next_operators):
        from ...flows.flow import Flow
        if not isinstance(next_operators, (list, set)):
            next_operators = [next_operators]
        if self.flow:
            # if I have a graph already, build on it
            flow = self.flow
        else:
            # if I don't have a graph, create one
            flow = Flow()
            flow.add_operator(F"{self.name}-{id(self)}", self)

        for operator in next_operators:
            if isinstance(operator, Flow):
                # if we're pointing to a graph, merge with the current graph,
                # we need to find the node with no incoming nodes we identify
                # the entry-point
                flow.merge(operator)
                flow.link_operators(
                    F"{self.name}-{id(self)}",
                    operator.get_entry_points().pop(),
                )
            elif issubclass(type(operator), BaseOperator):
                # otherwise add the node and edge and set the graph further
                # down the line
                flow.add_operator(F"{operator.__class__.__name__}-{id(operator)}", operator)
                flow.link_operators(F"{self.name}-{id(self)}", F"{operator.__class__.__name__}-{id(operator)}")
                operator.flow = flow
            else:
                label = type(operator).__name__
                if hasattr(operator, '__name__'):
                    label = operator.__name__
                #  deepcode ignore return~not~implemented: appears to be false positive
                raise TypeError(F"Operator {label} must inherit BaseOperator, this error also occurs when the Operator has not been correctly instantiated.")
        # this variable only exists to build the graph, we don't need it
        # anymore so destroy it
        self.flow = None
        return flow

    def _clamp(self, value, low_bound, high_bound):
        """
        'clamping' is fixing a value within a range
        """
        value = min(value, high_bound)
        value = max(value, low_bound)
        return value

    def _only_alpha_nums(self, text):
        pattern = re.compile(r"[\W_]+")
        return pattern.sub("", text)

    def _hash(self, block):
        try:
            bytes_object = serialize(block)
        except:
            bytes_object = str(block)
        raw_hash = hashlib.sha256(bytes_object.encode())
        hex_hash = raw_hash.hexdigest()
        return hex_hash

    def _wrap_text(self, text, line_len):
        from textwrap import fill
        def _inner(text):
            for line in text.splitlines():
                yield fill(line, line_len)
        return '\n'.join(list(_inner(text)))

    def __repr__(self):
        return F"<{self.name}, version: {self.version()}>"
