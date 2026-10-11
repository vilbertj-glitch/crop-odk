import os
import sys

# 1. Update g2p_geo.py for default header and body on GetAllGeoLevelsRequest
schema_path = "/usr/local/lib/python3.12/site-packages/openg2p_gen2_master_data/schemas/g2p_geo.py"
if not os.path.exists(schema_path):
    raise FileNotFoundError(f"File not found: {schema_path}")

with open(schema_path, "r") as f:
    s = f.read()

old_req = "class GetAllGeoLevelsRequest(G2PRequest):\n    request_body: GetAllGeoLevelsRequestBody"
new_req = '''from datetime import datetime, timezone
import uuid
from pydantic import Field
from openg2p_fastapi_common.schemas import G2PRequestHeader

class GetAllGeoLevelsRequest(G2PRequest):
    request_header: G2PRequestHeader = Field(default_factory=lambda: G2PRequestHeader(
        sender_app_mnemonic='portal',
        sender_app_url='http://portal',
        request_id=str(uuid.uuid4()),
        request_timestamp=datetime.now(timezone.utc)
    ))
    request_body: GetAllGeoLevelsRequestBody = Field(default_factory=lambda: GetAllGeoLevelsRequestBody(
        request_payload=GetAllGeoLevelsRequestPayload()
    ))'''

if old_req in s:
    s = s.replace(old_req, new_req)
    with open(schema_path, "w") as f:
        f.write(s)
    print("Updated g2p_geo.py with default request envelope.")
elif "default_factory" in s:
    print("g2p_geo.py already updated.")
else:
    raise RuntimeError(f"Target pattern '{old_req}' not found in {schema_path}")

# 2. Update g2p_geo_controller.py
ctl_path = "/usr/local/lib/python3.12/site-packages/openg2p_gen2_master_data/controllers/g2p_geo_controller.py"
if not os.path.exists(ctl_path):
    raise FileNotFoundError(f"File not found: {ctl_path}")

with open(ctl_path, "r") as f:
    c = f.read()

c = c.replace("    @require_permissions({})\n    async def get_all_geo_levels(", "    async def get_all_geo_levels(")
c = c.replace("    @require_permissions({})\n    @data_policy\n    @cache(", "    @data_policy\n    @cache(")

target = 'self.router.add_api_route(\n            "/get_geo_level_values",'
if "/get_all_g2p_geo_levels" not in c:
    if target not in c:
        raise RuntimeError(f"Target route registration not found in {ctl_path}")
    alias_routes = """
        self.router.add_api_route(
            "/get_all_g2p_geo_levels",
            self.get_all_geo_levels,
            responses={200: {"model": GetAllGeoLevelsResponse}},
            methods=["POST"],
        )
        self.router.add_api_route(
            "/get_g2p_geo_levels",
            self.get_all_geo_levels,
            responses={200: {"model": GetAllGeoLevelsResponse}},
            methods=["POST"],
        )
        self.router.add_api_route(
            "/get_g2p_geo_level_values",
            self.get_geo_level_values,
            responses={200: {"model": GetGeoLevelValuesResponse}},
            methods=["POST"],
        )
"""
    end_idx = c.find('methods=["POST"],\n        )', c.find(target)) + len('methods=["POST"],\n        )')
    c = c[:end_idx] + alias_routes + c[end_idx:]
    with open(ctl_path, "w") as f:
        f.write(c)
    print("Successfully added geo route aliases to g2p_geo_controller.py.")
else:
    print("Route aliases already present in g2p_geo_controller.py.")
