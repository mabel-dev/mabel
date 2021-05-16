import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.operators import ProfileDataOperator, EndOperator
from mabel.operators.disk import DiskBatchWriterOperator
from mabel.operators.minio import MinIoBatchWriterOperator
from mabel.data.validator import Schema
from mabel import Flow
from rich import traceback

traceback.install()


def test_saving_the_profile():
    # fmt: off
    TEST_DATA = [
        { "name": "Sirius Black", "age": 40, "dob": "1970-01-02", "gender": "male" },
        { "name": "Harry Potter", "age": 11, "dob": "1999-07-30", "gender": "male" },
        { "name": "Hermione Grainger", "age": 10, "dob": "1999-12-14", "gender": "female" },
        { "name": "Fleur Isabelle Delacour", "age": 11, "dob": "1999-02-08", "gender": "female" },
        { "name": "James Potter", "age": 40, "dob": "1971-12-30", "gender": "male" },
        { "name": "James Potter", "age": 0, "dob": "2010-12-30", "gender": "male" }
    ]
    TEST_SCHEMA = {
        "fields": [
            { "name": "age",     "type": "numeric" },
            { "name": "gender",  "type": "enum", "symbols": ["male", "female", "not the above"] },
            { "name": "dob",     "type": "date"    },
            { "name": "name",    "type": "string"  }
        ]
    }
    # fmt: on

    pdo = ProfileDataOperator(schema=Schema(TEST_SCHEMA))
    std = DiskBatchWriterOperator(
        end_point=os.getenv("MINIO_END_POINT"),
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=False,
        dataset="_temp/profile",
    )
    end = EndOperator()

    flow = pdo > std > end

    with flow as runner:
        for entry in TEST_DATA:
            runner(entry, {})


if __name__ == "__main__":  # pragma: no cover
    test_saving_the_profile()
    print("okay")
