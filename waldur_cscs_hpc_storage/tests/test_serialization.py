import json
import uuid
from enum import Enum

from waldur_cscs_hpc_storage.serialization import JSONResponse, dump_json


class ExampleEnum(Enum):
    OPTION_A = "Option A"
    OPTION_B = "Option B"


def test_dump_json_with_uuid():
    u = uuid.uuid4()
    data = {"id": u}
    json_str = dump_json(data)
    loaded_data = json.loads(json_str)
    assert loaded_data["id"] == u.hex


def test_dump_json_with_enum():
    data = {"option": ExampleEnum.OPTION_A}
    json_str = dump_json(data)
    loaded_data = json.loads(json_str)
    assert loaded_data["option"] == "Option A"


def test_dump_json_with_mixed_types():
    u = uuid.uuid4()
    data = {
        "id": u,
        "name": "test",
        "count": 10,
        "status": ExampleEnum.OPTION_B,
    }
    json_str = dump_json(data)
    loaded_data = json.loads(json_str)
    assert loaded_data["id"] == u.hex
    assert loaded_data["name"] == "test"
    assert loaded_data["count"] == 10
    assert loaded_data["status"] == "Option B"


def test_dump_json_list_with_uuid():
    u1 = uuid.uuid4()
    u2 = uuid.uuid4()
    data = [u1, u2]
    json_str = dump_json(data)
    loaded_data = json.loads(json_str)
    assert loaded_data == [u1.hex, u2.hex]


def test_json_response_with_uuid():
    u = uuid.uuid4()
    data = {"id": u}
    response = JSONResponse(data)
    assert response.body == json.dumps({"id": u.hex}, separators=(",", ":")).encode(
        "utf-8"
    )


def test_json_response_with_enum():
    data = {"option": ExampleEnum.OPTION_A}
    response = JSONResponse(data)
    assert response.body == json.dumps(
        {"option": "Option A"}, separators=(",", ":")
    ).encode("utf-8")
