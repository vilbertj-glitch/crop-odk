"""ODK Ingestion Hooks for OpenG2P Crop Sown Registry.

This is a standalone, independent extension module. It intercepts validation
and ensures database engine initialization and submodule aliasing during
ingestion without requiring any changes to base platform files.
"""

import importlib
import logging
import os
import sys
from contextvars import ContextVar

_logger = logging.getLogger("openg2p.odk_ingest_hooks")
_active_session: ContextVar = ContextVar("odk_active_session", default=None)

# Bridge staff-api / platform settings to iam_core common_ settings
_redis_url = os.environ.get("REGISTRY_STAFF_PORTAL_API_AUTH_REDIS_URL") or os.environ.get("COMMON_AUTH_REDIS_URL")
if _redis_url:
    os.environ["COMMON_AUTH_REDIS_URL"] = _redis_url

_auth_provider_url = os.environ.get("REGISTRY_STAFF_PORTAL_API_AUTH_PROVIDER_API_URL") or os.environ.get("COMMON_AUTH_PROVIDER_API_URL")
if _auth_provider_url:
    os.environ["COMMON_AUTH_PROVIDER_API_URL"] = _auth_provider_url

_client_id = os.environ.get("REGISTRY_STAFF_PORTAL_API_KEYCLOAK_CLIENT_ID") or os.environ.get("COMMON_KEYCLOAK_CLIENT_ID")
if _client_id:
    os.environ["COMMON_KEYCLOAK_CLIENT_ID"] = _client_id

try:
    from iam_core.user_auth.config import Settings as IamSettings
    _iam_cfg = IamSettings.get_config(strict=False)
    if _redis_url:
        _iam_cfg.auth_redis_url = _redis_url
    if _auth_provider_url:
        _iam_cfg.auth_provider_api_url = _auth_provider_url
    if _client_id:
        _iam_cfg.keycloak_client_id = _client_id
except Exception:
    pass


def _get_registry_engine():
    """Construct an AsyncEngine connected to the primary registry database."""
    from sqlalchemy.ext.asyncio import create_async_engine
    hostname = (
        os.environ.get("COMMON_DB_HOSTNAME")
        or os.environ.get("REGISTRY_CORE_DB_HOSTNAME")
        or os.environ.get("REGISTRY_PARTNER_API_DB_HOSTNAME")
        or os.environ.get("REGISTRY_CELERY_WORKERS_DB_HOSTNAME")
        or os.environ.get("REGISTRY_STAFF_PORTAL_API_DB_HOSTNAME")
        or "postgres"
    )
    port = (
        os.environ.get("COMMON_DB_PORT")
        or os.environ.get("REGISTRY_CORE_DB_PORT")
        or os.environ.get("REGISTRY_PARTNER_API_DB_PORT")
        or "5432"
    )
    dbname = (
        os.environ.get("COMMON_DB_DBNAME")
        or os.environ.get("REGISTRY_CORE_DB_DBNAME")
        or os.environ.get("REGISTRY_PARTNER_API_DB_DBNAME")
        or "cropsown"
    )
    username = (
        os.environ.get("COMMON_DB_USERNAME")
        or os.environ.get("REGISTRY_CORE_DB_USERNAME")
        or os.environ.get("REGISTRY_PARTNER_API_DB_USERNAME")
        or "cropsown_user"
    )
    password = (
        os.environ.get("COMMON_DB_PASSWORD")
        or os.environ.get("REGISTRY_CORE_DB_PASSWORD")
        or os.environ.get("REGISTRY_PARTNER_API_DB_PASSWORD")
        or "cropsown_pass"
    )
    dsn = f"postgresql+asyncpg://{username}:{password}@{hostname}:{port}/{dbname}"
    return create_async_engine(dsn)


def _ensure_dbengine_initialized():
    """Ensure dbengine GlobalVar is set to a valid AsyncEngine pointing to the registry DB."""
    try:
        from openg2p_fastapi_common.context import dbengine
        eng = dbengine.get()
        needs_fix = eng is None or (hasattr(eng, "url") and eng.url.host in ("localhost", "127.0.0.1", None))
        if needs_fix:
            try:
                from openg2p_registry_celery_worker.engine import Engine
                eng = Engine.get_async_engine()
            except Exception:
                eng = None
            if eng is None:
                eng = _get_registry_engine()
            if eng is not None:
                dbengine.set(eng)
                _logger.info("ODK Hook: dbengine initialized to %s", eng.url)
    except Exception as e:
        _logger.debug("ODK Hook: Error in _ensure_dbengine_initialized: %s", e)


def _patch_base_initializer():
    """Patch BaseInitializer.init_db to avoid binding to localhost."""
    try:
        from openg2p_fastapi_common.app import Initializer as BaseInitializer
        if not hasattr(BaseInitializer, "_orig_init_db"):
            orig_init_db = BaseInitializer.init_db
            BaseInitializer._orig_init_db = orig_init_db

            def patched_init_db(self):
                orig_init_db(self)
                _ensure_dbengine_initialized()

            BaseInitializer.init_db = patched_init_db
            _logger.info("ODK Hook: Patched BaseInitializer.init_db")
    except Exception as e:
        _logger.debug("ODK Hook: Error patching BaseInitializer.init_db: %s", e)


def _patch_document_handler():
    """Ensure DocumentHandler uses the correct MinIO endpoint inside docker."""
    try:
        from openg2p_registry_core.helpers.document import document_factory
        from openg2p_registry_core.helpers.document.minio_client import MinioClient
        from openg2p_registry_core.helpers.document.document_handlers import DocumentHandler
        from openg2p_fastapi_common.component import component_registry

        def make_minio_client():
            endpoint = (
                os.environ.get("REGISTRY_CORE_MINIO_ENDPOINT")
                or os.environ.get("REGISTRY_CELERY_WORKERS_MINIO_ENDPOINT")
                or os.environ.get("REGISTRY_PARTNER_API_MINIO_ENDPOINT")
                or os.environ.get("REGISTRY_STAFF_PORTAL_API_MINIO_ENDPOINT")
                or "minio:9000"
            )
            access_key = (
                os.environ.get("REGISTRY_CORE_MINIO_ACCESS_KEY")
                or os.environ.get("REGISTRY_CELERY_WORKERS_MINIO_ACCESS_KEY")
                or os.environ.get("REGISTRY_PARTNER_API_MINIO_ACCESS_KEY")
                or "minioadmin"
            )
            secret_key = (
                os.environ.get("REGISTRY_CORE_MINIO_SECRET_KEY")
                or os.environ.get("REGISTRY_CELERY_WORKERS_MINIO_SECRET_KEY")
                or os.environ.get("REGISTRY_PARTNER_API_MINIO_SECRET_KEY")
                or "minioadmin"
            )
            secure = (
                os.environ.get("REGISTRY_CORE_MINIO_SECURE")
                or os.environ.get("REGISTRY_CELERY_WORKERS_MINIO_SECURE")
                or "false"
            ).lower() == "true"
            return MinioClient(
                endpoint=endpoint,
                access_key=access_key,
                secret_key=secret_key,
                secure=secure,
            )

        document_factory._create_document_handler = make_minio_client

        # Replace any existing DocumentHandler in component_registry if it pointed to localhost
        cr = component_registry.get()
        if cr:
            for i, c in enumerate(cr):
                if isinstance(c, DocumentHandler):
                    cr[i] = make_minio_client()
                    _logger.info("ODK Hook: Replaced DocumentHandler with docker minio client")
                    break
    except Exception as e:
        _logger.debug("ODK Hook: Error in _patch_document_handler: %s", e)


def _ensure_core_services():
    """Ensure Core services like G2PDocumentService are initialized."""
    try:
        from openg2p_registry_core.services import G2PDocumentService
        if G2PDocumentService.get_component() is None:
            from openg2p_registry_core.app import Initializer as CoreInitializer
            CoreInitializer().initialize()
            _logger.info("ODK Hook: CoreInitializer initialized core services")
    except Exception as e:
        _logger.debug("ODK Hook: Error initializing core services: %s", e)


def _ensure_master_data_engine():
    """Ensure db_engine_master_data connects to postgres rather than defaulting to localhost."""
    try:
        from openg2p_registry_core import engine
        from sqlalchemy.ext.asyncio import create_async_engine
        from sqlalchemy.pool import NullPool

        hostname = (
            os.environ.get("REGISTRY_CORE_MASTER_DATA_DB_HOSTNAME")
            or os.environ.get("REGISTRY_PARTNER_API_MASTER_DATA_DB_HOSTNAME")
            or os.environ.get("REGISTRY_CELERY_WORKERS_MASTER_DATA_DB_HOSTNAME")
            or os.environ.get("REGISTRY_STAFF_PORTAL_API_MASTER_DATA_DB_HOSTNAME")
            or "postgres"
        )
        username = (
            os.environ.get("REGISTRY_CORE_MASTER_DATA_DB_USERNAME")
            or os.environ.get("REGISTRY_PARTNER_API_MASTER_DATA_DB_USERNAME")
            or os.environ.get("REGISTRY_CELERY_WORKERS_MASTER_DATA_DB_USERNAME")
            or "master_data_user"
        )
        password = (
            os.environ.get("REGISTRY_CORE_MASTER_DATA_DB_PASSWORD")
            or os.environ.get("REGISTRY_PARTNER_API_MASTER_DATA_DB_PASSWORD")
            or os.environ.get("REGISTRY_CELERY_WORKERS_MASTER_DATA_DB_PASSWORD")
            or "master_data_pass"
        )
        dbname = (
            os.environ.get("REGISTRY_CORE_MASTER_DATA_DB_DBNAME")
            or os.environ.get("REGISTRY_PARTNER_API_MASTER_DATA_DB_DBNAME")
            or os.environ.get("REGISTRY_CELERY_WORKERS_MASTER_DATA_DB_DBNAME")
            or "master_data"
        )
        port = (
            os.environ.get("REGISTRY_CORE_MASTER_DATA_DB_PORT")
            or os.environ.get("REGISTRY_PARTNER_API_MASTER_DATA_DB_PORT")
            or "5432"
        )
        driver = "postgresql+asyncpg"
        dsn = f"{driver}://{username}:{password}@{hostname}:{port}/{dbname}"
        eng = create_async_engine(dsn, poolclass=NullPool)
        if engine._engines is None:
            engine._engines = {}
        engine._engines["db_engine_master_data"] = eng
        _logger.info("ODK Hook: Initialized db_engine_master_data to %s", dsn)
    except Exception as e:
        _logger.debug("ODK Hook: Error initializing master data engine: %s", e)


_master_data_engine = None


def _get_master_data_engine():
    global _master_data_engine
    if _master_data_engine is not None:
        return _master_data_engine
    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy.pool import NullPool

    hostname = (
        os.environ.get("REGISTRY_CORE_MASTER_DATA_DB_HOSTNAME")
        or os.environ.get("REGISTRY_PARTNER_API_MASTER_DATA_DB_HOSTNAME")
        or os.environ.get("REGISTRY_CELERY_WORKERS_MASTER_DATA_DB_HOSTNAME")
        or os.environ.get("REGISTRY_STAFF_PORTAL_API_MASTER_DATA_DB_HOSTNAME")
        or os.environ.get("MASTER_DATA_DB_HOSTNAME")
        or os.environ.get("COMMON_DB_HOSTNAME")
        or "postgres"
    )
    port = (
        os.environ.get("REGISTRY_CORE_MASTER_DATA_DB_PORT")
        or os.environ.get("REGISTRY_PARTNER_API_MASTER_DATA_DB_PORT")
        or os.environ.get("REGISTRY_CELERY_WORKERS_MASTER_DATA_DB_PORT")
        or os.environ.get("REGISTRY_STAFF_PORTAL_API_MASTER_DATA_DB_PORT")
        or os.environ.get("MASTER_DATA_DB_PORT")
        or "5432"
    )
    dbname = (
        os.environ.get("REGISTRY_CORE_MASTER_DATA_DB_DBNAME")
        or os.environ.get("REGISTRY_PARTNER_API_MASTER_DATA_DB_DBNAME")
        or os.environ.get("REGISTRY_CELERY_WORKERS_MASTER_DATA_DB_DBNAME")
        or os.environ.get("REGISTRY_STAFF_PORTAL_API_MASTER_DATA_DB_DBNAME")
        or os.environ.get("MASTER_DATA_DB_DBNAME")
        or "master_data"
    )
    username = (
        os.environ.get("REGISTRY_CORE_MASTER_DATA_DB_USERNAME")
        or os.environ.get("REGISTRY_PARTNER_API_MASTER_DATA_DB_USERNAME")
        or os.environ.get("REGISTRY_CELERY_WORKERS_MASTER_DATA_DB_USERNAME")
        or os.environ.get("REGISTRY_STAFF_PORTAL_API_MASTER_DATA_DB_USERNAME")
        or os.environ.get("MASTER_DATA_DB_USERNAME")
        or "master_data_user"
    )
    password = (
        os.environ.get("REGISTRY_CORE_MASTER_DATA_DB_PASSWORD")
        or os.environ.get("REGISTRY_PARTNER_API_MASTER_DATA_DB_PASSWORD")
        or os.environ.get("REGISTRY_CELERY_WORKERS_MASTER_DATA_DB_PASSWORD")
        or os.environ.get("REGISTRY_STAFF_PORTAL_API_MASTER_DATA_DB_PASSWORD")
        or os.environ.get("MASTER_DATA_DB_PASSWORD")
        or "master_data_pass"
    )
    dsn = f"postgresql+asyncpg://{username}:{password}@{hostname}:{port}/{dbname}"
    _master_data_engine = create_async_engine(dsn, poolclass=NullPool)
    return _master_data_engine


_GEO_LABEL_CACHE: dict[str, str] = {
    "central_ethiopia": "Central Ethiopia",
    "amhara": "Amhara",
    "dire": "Dire Dawa",
    "dire_dawa": "Dire Dawa",
    "oromia": "Oromia",
    "benishangul": "Benishangul-Gumuz",
    "ethiopia_somali": "Somali",
    "somali": "Somali",
    "tigray": "Tigray",
    "afar": "Afar",
    "sidama": "Sidama",
    "south_ethiopian": "South Ethiopian",
    "south_west_ethiopia": "South West Ethiopia",
    "gambela": "Gambela",
    "harari": "Harari",
    "addis_ababa": "Addis Ababa",
    "ET01": "Tigray",
    "ET02": "Afar",
    "ET03": "Amhara",
    "ET04": "Oromia",
    "ET05": "Somali",
    "ET06": "Benishangul-Gumuz",
    "ET07": "Central Ethiopia",
    "ET08": "South Ethiopian",
    "ET11": "South West Ethiopia",
    "ET12": "Gambela",
    "ET13": "Harari",
    "ET14": "Addis Ababa",
    "ET15": "Dire Dawa",
    "ET16": "Sidama",
}


async def resolve_geo_label(code: str | None, level_prefix: str = "") -> str | None:
    """Resolve an administrative code or mnemonic to a human-readable display label."""
    if not code:
        return None
    code_str = str(code).strip()
    if not code_str:
        return None
    if code_str in _GEO_LABEL_CACHE:
        return _GEO_LABEL_CACHE[code_str]

    clean_code = code_str
    for pfx in ("KEBELE_", "WOREDA_", "ZONE_", "REGION_"):
        if clean_code.startswith(pfx):
            clean_code = clean_code[len(pfx):]
            break

    if clean_code in _GEO_LABEL_CACHE:
        return _GEO_LABEL_CACHE[clean_code]

    try:
        engine = _get_master_data_engine()
        from sqlalchemy.ext.asyncio import async_sessionmaker
        from sqlalchemy import text

        session_maker = async_sessionmaker(engine, expire_on_commit=False)
        async with session_maker() as session:
            candidates = [
                code_str,
                clean_code,
                f"{level_prefix}-{code_str}" if level_prefix else code_str,
                f"{level_prefix}-{clean_code}" if level_prefix else clean_code,
                f"zone-{code_str}",
                f"woreda-{code_str}",
                f"kebele-{code_str}",
                f"region-{code_str}",
            ]
            q = text(
                "SELECT level_value_mnemonic FROM g2p_geo_level_values "
                "WHERE level_value_id = :c1 OR level_value_id = :c2 OR level_value_id = :c3 "
                "OR level_value_id = :c4 OR level_value_id = :c5 OR level_value_id = :c6 "
                "OR level_value_id = :c7 OR level_value_id = :c8 "
                "OR level_value_id ILIKE :c_like "
                "LIMIT 1"
            )
            res = await session.execute(
                q,
                {
                    "c1": candidates[0],
                    "c2": candidates[1],
                    "c3": candidates[2],
                    "c4": candidates[3],
                    "c5": candidates[4],
                    "c6": candidates[5],
                    "c7": candidates[6],
                    "c8": candidates[7],
                    "c_like": f"%{clean_code}",
                },
            )
            row = res.fetchone()
            if row and row[0]:
                _GEO_LABEL_CACHE[code_str] = row[0]
                return row[0]
    except Exception as e:
        _logger.warning("ODK Hook: Geo resolution error for %s: %s", code_str, e)

    cleaned = code_str.replace("_", " ").title() if "_" in code_str else code_str
    _GEO_LABEL_CACHE[code_str] = cleaned
    return cleaned


def _alias_extension_modules():
    """Ensure openg2p_registry_extensions and all its subpackages alias to cropsown extension."""
    ext = os.environ.get("REGISTRY_EXTENSION_MODULE", "openg2p_registry_cropsown_extension")
    if "openg2p_registry_extensions" not in sys.modules:
        try:
            sys.modules["openg2p_registry_extensions"] = sys.modules.get(ext) or importlib.import_module(ext)
            for sub in (
                "config",
                "ingestion_pipeline",
                "ingestion_pipeline.enricher_services",
                "register_domain",
                "register_domain.models",
                "register_domain.schemas",
                "register_domain.services",
                "register_domain.controllers",
                "register_domain.factory",
            ):
                try:
                    sys.modules[f"openg2p_registry_extensions.{sub}"] = importlib.import_module(f"{ext}.{sub}")
                except Exception:
                    pass
            _logger.info("ODK Hook: Aliased openg2p_registry_extensions -> %s", ext)
        except Exception as e:
            _logger.debug("ODK Hook: Error aliasing extension modules: %s", e)


def _patch_request_response_helper():
    """Patch RequestResponseHelper._construct_data_model_response for Pydantic v2 JSON serialization."""
    try:
        from openg2p_registry_partner_api.ingestion.helpers.request_response_helper import RequestResponseHelper
        from fastapi.responses import JSONResponse
        from openg2p_registry_core.helpers import TemplateHelper

        if not hasattr(RequestResponseHelper, "_orig_construct_data_model_response"):
            RequestResponseHelper._orig_construct_data_model_response = RequestResponseHelper._construct_data_model_response

            def patched_construct_data_model_response(self, response_template_store_id, response):
                if not response_template_store_id:
                    return JSONResponse(content=response.model_dump(mode="json"))

                template_helper = TemplateHelper.get_component()
                response_data = response.model_dump(mode="json")
                response_data = template_helper.render_with_template(
                    document_store_id=response_template_store_id,
                    data=response_data,
                    expand_data=False,
                )
                return JSONResponse(content=response_data)

            RequestResponseHelper._construct_data_model_response = patched_construct_data_model_response
            _logger.info("ODK Hook: Patched RequestResponseHelper._construct_data_model_response")
    except Exception as e:
        _logger.debug("Could not patch RequestResponseHelper: %s", e)


def _patch_celery_worker():
    """Patch ingest_data_worker to bind dbengine and active session."""
    try:
        worker_mod = importlib.import_module("openg2p_registry_celery_worker.tasks.ingest_data_worker")
        if worker_mod and not hasattr(worker_mod, "_orig_save_sections_async"):
            orig_save_sections_async = worker_mod._save_sections_async
            worker_mod._orig_save_sections_async = orig_save_sections_async

            async def patched_save_sections_async(
                submission_id: str,
                incoming_classified_data,
                ordered_sections: list,
                transformed_data: dict,
                session,
            ) -> None:
                try:
                    from openg2p_fastapi_common.context import dbengine
                    if hasattr(session, "bind") and session.bind:
                        dbengine.set(session.bind)
                    else:
                        _ensure_dbengine_initialized()
                    _ensure_core_services()
                except Exception:
                    pass

                t_session = _active_session.set(session)
                try:
                    # Auto-enrich intake records with farmer name, location, and status
                    try:
                        from sqlalchemy import text
                        for sec_key in ("cs_common_intake_record", "cs_intake_record"):
                            rows = transformed_data.get(sec_key) or []
                            for row in rows:
                                fayda = row.get("fayda_fan_id")
                                clean_fayda = ""
                                if fayda:
                                    clean_fayda = str(fayda).strip().replace(" ", "").replace("-", "").replace("/", "")
                                    if clean_fayda.startswith("FAN"):
                                        clean_fayda = clean_fayda[3:].lstrip("-").strip()
                                    if len(clean_fayda) != 16:
                                        _logger.warning("Fayda ID '%s' is not 16 digits (length=%d)", clean_fayda, len(clean_fayda))
                                    row["fayda_fan_id"] = clean_fayda
                                f_name = row.get("farmer_name")
                                if not row.get("status"):
                                    row["status"] = "DRAFT"
                                if clean_fayda and (not f_name or not str(f_name).strip() or not row.get("region")):
                                    res = await session.execute(
                                        text("""
                                            SELECT farmer_name, farmer_id, region, zone, woreda, kebele,
                                                   address_line_1, address_line_2, latitude, longitude, gps_coordinate
                                            FROM g2p_intake_form_crop_sowns
                                            WHERE (fayda_fan_id = :fayda OR fayda_fan_id = 'FAN-' || :fayda) AND farmer_name IS NOT NULL AND farmer_name != ''
                                            ORDER BY created_at DESC LIMIT 1
                                        """),
                                        {"fayda": clean_fayda}
                                    )
                                    r = res.fetchone()
                                    if not r:
                                        res_reg = await session.execute(
                                            text("""
                                                SELECT farmer_name, farmer_id, region, zone, woreda, kebele,
                                                       address_line_1, address_line_2, latitude, longitude,
                                                       latitude || ', ' || longitude as gps_coordinate
                                                FROM g2p_register_crop_sowns
                                                WHERE (fayda_fan_id = :fayda OR fayda_fan_id = 'FAN-' || :fayda) AND farmer_name IS NOT NULL AND farmer_name != ''
                                                ORDER BY created_at DESC LIMIT 1
                                            """),
                                            {"fayda": clean_fayda}
                                        )
                                        r = res_reg.fetchone()
                                    if r:
                                        if not row.get("farmer_name") and r[0]:
                                            row["farmer_name"] = r[0]
                                        if not row.get("farmer_id") and r[1]:
                                            row["farmer_id"] = r[1]
                                        if not row.get("region") and r[2]:
                                            row["region"] = r[2]
                                        if not row.get("zone") and r[3]:
                                            row["zone"] = r[3]
                                        if not row.get("woreda") and r[4]:
                                            row["woreda"] = r[4]
                                        if not row.get("kebele") and r[5]:
                                            row["kebele"] = r[5]
                                        if not row.get("latitude") and r[8]:
                                            row["latitude"] = r[8]
                                        if not row.get("longitude") and r[9]:
                                            row["longitude"] = r[9]
                                        if not row.get("gps_coordinate") and r[10]:
                                            row["gps_coordinate"] = r[10]

                                f_id = row.get("farmer_id")
                                if f_id:
                                    row["record_name"] = str(f_id)
                                else:
                                    row["record_name"] = None
                                for level_pfx, code_key, name_key in (
                                    ("region", "region", "region_name"),
                                    ("zone", "zone", "zone_name"),
                                    ("woreda", "woreda", "woreda_name"),
                                    ("kebele", "kebele", "kebele_name"),
                                ):
                                    val = row.get(code_key) or row.get(name_key)
                                    if val:
                                        val_str = str(val).strip()
                                        try:
                                            resolved = await resolve_geo_label(val_str, level_pfx)
                                            if resolved:
                                                row[name_key] = resolved
                                            elif not row.get(name_key):
                                                row[name_key] = val_str
                                        except Exception as err:
                                            _logger.debug("Error resolving geo label for %s: %s", val_str, err)
                                            if not row.get(name_key):
                                                row[name_key] = val_str

                        # Also resolve geo labels across child sections (e.g. planning, cluster)
                        for sec in ("cs_planning_details", "cs_cultivation_cluster_details", "cs_cluster_details"):
                            for s_row in (transformed_data.get(sec) or []):
                                for level_pfx, code_key, name_key in (
                                    ("region", "region", "region_name"),
                                    ("zone", "zone", "zone_name"),
                                    ("woreda", "woreda", "woreda_name"),
                                    ("kebele", "kebele", "kebele_name"),
                                ):
                                    val = s_row.get(code_key) or s_row.get(name_key)
                                    if val:
                                        try:
                                            resolved = await resolve_geo_label(str(val).strip(), level_pfx)
                                            if resolved:
                                                s_row[name_key] = resolved
                                        except Exception:
                                            pass

                        # Ensure cs_cropsown_location has gps_coordinate
                        loc_rows = transformed_data.get("cs_cropsown_location") or []
                        for lr in loc_rows:
                            lat = lr.get("latitude")
                            lon = lr.get("longitude")
                            if lat and lon and not lr.get("gps_coordinate"):
                                lr["gps_coordinate"] = f"{lat}, {lon}"

                        # Propagate cluster_name and commodity across stages
                        for sow in transformed_data.get("cs_sowing_details") or []:
                            lid = sow.get("land_id")
                            if not sow.get("cluster_name") and lid:
                                res_cl = await session.execute(
                                    text("""
                                        SELECT cluster_name FROM g2p_intake_form_cultivation_clusters
                                        WHERE land_id = :lid AND cluster_name IS NOT NULL AND cluster_name != ''
                                        ORDER BY created_at DESC LIMIT 1
                                    """),
                                    {"lid": str(lid).strip()}
                                )
                                r_cl = res_cl.fetchone()
                                if not r_cl:
                                    res_cl2 = await session.execute(
                                        text("""
                                            SELECT cluster_name FROM g2p_intake_form_clusters
                                            WHERE land_id = :lid AND cluster_name IS NOT NULL AND cluster_name != ''
                                            ORDER BY created_at DESC LIMIT 1
                                        """),
                                        {"lid": str(lid).strip()}
                                    )
                                    r_cl = res_cl2.fetchone()
                                if r_cl and r_cl[0]:
                                    sow["cluster_name"] = r_cl[0]

                        for inf in transformed_data.get("cs_infestation_details") or []:
                            lid = inf.get("land_id")
                            if not inf.get("cluster_name") and lid:
                                res_cl = await session.execute(
                                    text("""
                                        SELECT cluster_name FROM g2p_intake_form_sowings
                                        WHERE land_id = :lid AND cluster_name IS NOT NULL AND cluster_name != ''
                                        ORDER BY created_at DESC LIMIT 1
                                    """),
                                    {"lid": str(lid).strip()}
                                )
                                r_cl = res_cl.fetchone()
                                if not r_cl:
                                    res_cl2 = await session.execute(
                                        text("""
                                            SELECT cluster_name FROM g2p_intake_form_cultivation_clusters
                                            WHERE land_id = :lid AND cluster_name IS NOT NULL AND cluster_name != ''
                                            ORDER BY created_at DESC LIMIT 1
                                        """),
                                        {"lid": str(lid).strip()}
                                    )
                                    r_cl = res_cl2.fetchone()
                                if r_cl and r_cl[0]:
                                    inf["cluster_name"] = r_cl[0]
                            if not inf.get("commodity") and lid:
                                res_com = await session.execute(
                                    text("""
                                        SELECT commodity FROM g2p_intake_form_sowings
                                        WHERE land_id = :lid AND commodity IS NOT NULL AND commodity != ''
                                        ORDER BY created_at DESC LIMIT 1
                                    """),
                                    {"lid": str(lid).strip()}
                                )
                                r_com = res_com.fetchone()
                                if r_com and r_com[0]:
                                    inf["commodity"] = r_com[0]

                        for harv in transformed_data.get("cs_harvest_details") or []:
                            lid = harv.get("land_id")
                            if not harv.get("cluster_name") and lid:
                                res_cl = await session.execute(
                                    text("""
                                        SELECT cluster_name FROM g2p_intake_form_sowings
                                        WHERE land_id = :lid AND cluster_name IS NOT NULL AND cluster_name != ''
                                        ORDER BY created_at DESC LIMIT 1
                                    """),
                                    {"lid": str(lid).strip()}
                                )
                                r_cl = res_cl.fetchone()
                                if not r_cl:
                                    res_cl2 = await session.execute(
                                        text("""
                                            SELECT cluster_name FROM g2p_intake_form_cultivation_clusters
                                            WHERE land_id = :lid AND cluster_name IS NOT NULL AND cluster_name != ''
                                            ORDER BY created_at DESC LIMIT 1
                                        """),
                                        {"lid": str(lid).strip()}
                                    )
                                    r_cl = res_cl2.fetchone()
                                if r_cl and r_cl[0]:
                                    harv["cluster_name"] = r_cl[0]
                            if not harv.get("commodity") and lid:
                                res_com = await session.execute(
                                    text("""
                                        SELECT commodity FROM g2p_intake_form_sowings
                                        WHERE land_id = :lid AND commodity IS NOT NULL AND commodity != ''
                                        ORDER BY created_at DESC LIMIT 1
                                    """),
                                    {"lid": str(lid).strip()}
                                )
                                r_com = res_com.fetchone()
                                if r_com and r_com[0]:
                                    harv["commodity"] = r_com[0]
                    except Exception as e:
                        _logger.debug("Error in auto-enriching intake submission: %s", e)

                    return await orig_save_sections_async(
                        submission_id,
                        incoming_classified_data,
                        ordered_sections,
                        transformed_data,
                        session,
                    )
                finally:
                    _active_session.reset(t_session)

            worker_mod._save_sections_async = patched_save_sections_async
            _logger.info("ODK Hook: Patched ingest_data_worker._save_sections_async")
    except Exception as e:
        _logger.debug("Could not patch ingest_data_worker: %s", e)


def _connect_celery_signals():
    """Register Celery signals to ensure dbengine is set for all background tasks."""
    try:
        from celery.signals import task_prerun

        @task_prerun.connect
        def on_celery_task_prerun(*args, **kwargs):
            _ensure_dbengine_initialized()
            _ensure_core_services()
    except Exception:
        pass


def _patch_intake_form_data_service():
    """Patch G2PIntakeFormDataService to handle land_id validation and Decimal serialization."""
    try:
        from openg2p_registry_core.services.intake_form_data_service import G2PIntakeFormDataService
        from sqlalchemy import select, text
        from decimal import Decimal

        if not hasattr(G2PIntakeFormDataService, "_odk_patched_serialize_model"):
            G2PIntakeFormDataService._odk_patched_serialize_model = True
            orig_serialize = G2PIntakeFormDataService._serialize_model

            def patched_serialize_model(self, row, exclude=None):
                data = orig_serialize(self, row, exclude)
                for k, v in data.items():
                    if isinstance(v, Decimal):
                        data[k] = str(v)
                return data

            G2PIntakeFormDataService._serialize_model = patched_serialize_model
            _logger.info("ODK Hook: Patched G2PIntakeFormDataService._serialize_model for Decimal handling")

        if not hasattr(G2PIntakeFormDataService, "_odk_patched_insert_live_register_row"):
            G2PIntakeFormDataService._odk_patched_insert_live_register_row = True
            orig_insert = G2PIntakeFormDataService._insert_live_register_row

            async def patched_insert_live_register_row(
                self,
                submission,
                section,
                register_definition,
                schema_class,
                register_class,
                intake_row,
                session,
                root_record_id_mapping=None,
            ):
                if hasattr(intake_row, "cluster_crop_maturity_status") and intake_row.cluster_crop_maturity_status == "NOT_YET_READY":
                    intake_row.cluster_crop_maturity_status = "NOT_YET_READY_FOR_HARVEST"
                if hasattr(intake_row, "agro_ecological_zone") and intake_row.agro_ecological_zone:
                    if str(intake_row.agro_ecological_zone).upper() in ("KUR", "KURRE"):
                        intake_row.agro_ecological_zone = "WURCH"
                res = await orig_insert(
                    self,
                    submission,
                    section,
                    register_definition,
                    schema_class,
                    register_class,
                    intake_row,
                    session,
                    root_record_id_mapping,
                )
                try:
                    if hasattr(register_class, "zone_name") and hasattr(intake_row, "internal_record_id"):
                        for attr_id, code_key, name_key in (
                            ("REGION", "region", "region_name"),
                            ("ZONE", "zone", "zone_name"),
                            ("WOREDA", "woreda", "woreda_name"),
                            ("KEBELE", "kebele", "kebele_name"),
                        ):
                            cur_name = getattr(intake_row, name_key, None)
                            cur_code = getattr(intake_row, code_key, None)
                            val = cur_name or cur_code
                            if val:
                                q = text("""
                                    SELECT value_display FROM g2p_attribute_values 
                                    WHERE attribute_id = :attr_id 
                                      AND (value_id = :val OR value_code = :val 
                                           OR value_display ILIKE :val
                                           OR value_id = (:attr_id || '_' || :val)
                                           OR value_id = (:attr_id || '_ET' || :val)
                                           OR value_code LIKE ('%' || :val)
                                           OR value_id LIKE ('%' || :val))
                                    ORDER BY 
                                      CASE WHEN value_id = :val THEN 1
                                           WHEN value_code = :val THEN 2
                                           WHEN value_id = (:attr_id || '_' || :val) THEN 3
                                           ELSE 4 END
                                    LIMIT 1
                                """)
                                disp = (await session.execute(q, {"attr_id": attr_id, "val": str(val).strip()})).scalar_one_or_none()
                                if disp:
                                    update_q = text(f"""
                                        UPDATE g2p_register_crop_sowns 
                                        SET {name_key} = :disp 
                                        WHERE internal_record_id = :rec_id
                                    """)
                                    await session.execute(update_q, {"disp": disp, "rec_id": intake_row.internal_record_id})
                except Exception as e:
                    _logger.debug("Error updating admin display names in register: %s", e)
                return res

            G2PIntakeFormDataService._insert_live_register_row = patched_insert_live_register_row
            _logger.info("ODK Hook: Patched G2PIntakeFormDataService._insert_live_register_row for admin unit display names")

        if hasattr(G2PIntakeFormDataService, "_odk_patched_validate_land_ids"):
            return
        G2PIntakeFormDataService._odk_patched_validate_land_ids = True
        orig_fn = G2PIntakeFormDataService._validate_land_ids_against_fayda_records

        async def patched_validate_land_ids(self, submission, session):
            try:
                return await orig_fn(self, submission, session)
            except Exception as exc:
                # If validation failed because land_id was not yet in active registers,
                # check if it exists in pending intake forms
                fayda_fan_id = None
                sections = await self._get_form_sections(submission.form_id, session)
                sowing_land_ids = set()
                infestation_land_ids = set()
                harvest_land_ids = set()

                for section in sections:
                    _reg_def, intake_cls, _reg_cls, _schema_cls, _hist_cls = (
                        await self._resolve_submission_models(section.section_register_id, session)
                    )
                    rows = (
                        await session.execute(
                            select(intake_cls).where(
                                *self._submission_section_filters(
                                    intake_cls,
                                    submission.submission_id,
                                    section.section_id,
                                )
                            )
                        )
                    ).scalars().all()

                    for row in rows:
                        if not fayda_fan_id and hasattr(row, "fayda_fan_id") and getattr(row, "fayda_fan_id"):
                            fayda_fan_id = getattr(row, "fayda_fan_id")
                        land_id = getattr(row, "land_id", None)
                        if land_id and str(land_id).strip():
                            clean_land = str(land_id).strip()
                            reg_mnemonic = _reg_def.register_mnemonic
                            if reg_mnemonic == "Sowing":
                                sowing_land_ids.add(clean_land)
                            elif reg_mnemonic == "Infestation":
                                infestation_land_ids.add(clean_land)
                            elif reg_mnemonic == "Harvest":
                                harvest_land_ids.add(clean_land)

                all_check_lands = sowing_land_ids | infestation_land_ids | harvest_land_ids
                if not all_check_lands:
                    raise exc

                intake_lands = set()
                if fayda_fan_id:
                    clean_fayda = str(fayda_fan_id).strip()
                    if clean_fayda.startswith("FAN-"):
                        clean_fayda = clean_fayda[4:].strip()
                    clean_fayda = clean_fayda.replace(" ", "").replace("-", "")
                    res_fayda = (
                        await session.execute(
                            text("""
                                SELECT DISTINCT p.land_id 
                                FROM g2p_intake_form_plannings p
                                JOIN g2p_intake_form_crop_sowns cs ON p.submission_id = cs.submission_id
                                WHERE (cs.fayda_fan_id = :fayda OR cs.fayda_fan_id = 'FAN-' || :fayda) AND p.land_id IS NOT NULL
                                UNION
                                SELECT DISTINCT c.land_id 
                                FROM g2p_intake_form_cultivations c
                                JOIN g2p_intake_form_crop_sowns cs ON c.submission_id = cs.submission_id
                                WHERE (cs.fayda_fan_id = :fayda OR cs.fayda_fan_id = 'FAN-' || :fayda) AND c.land_id IS NOT NULL
                                UNION
                                SELECT DISTINCT s.land_id 
                                FROM g2p_intake_form_sowings s
                                JOIN g2p_intake_form_crop_sowns cs ON s.submission_id = cs.submission_id
                                WHERE (cs.fayda_fan_id = :fayda OR cs.fayda_fan_id = 'FAN-' || :fayda) AND s.land_id IS NOT NULL
                            """),
                            {"fayda": clean_fayda}
                        )
                    ).scalars().all()
                    intake_lands.update([str(l).strip() for l in res_fayda if l and str(l).strip()])

                res_any = (
                    await session.execute(
                        text("""
                            SELECT DISTINCT land_id FROM g2p_intake_form_plannings WHERE land_id IS NOT NULL
                            UNION
                            SELECT DISTINCT land_id FROM g2p_intake_form_cultivations WHERE land_id IS NOT NULL
                            UNION
                            SELECT DISTINCT land_id FROM g2p_intake_form_sowings WHERE land_id IS NOT NULL
                        """)
                    )
                ).scalars().all()
                intake_lands.update([str(l).strip() for l in res_any if l and str(l).strip()])

                if all_check_lands.issubset(intake_lands):
                    return
                raise exc

        G2PIntakeFormDataService._validate_land_ids_against_fayda_records = patched_validate_land_ids
        _logger.info("ODK Hook: Patched G2PIntakeFormDataService._validate_land_ids_against_fayda_records")

        if not hasattr(G2PIntakeFormDataService, "_odk_patched_stage_order_approval"):
            G2PIntakeFormDataService._odk_patched_stage_order_approval = True

            async def _validate_stage_order_for_approval(self, submission, session) -> None:
                from sqlalchemy import text
                from openg2p_registry_core.errors import G2PRegistryException, G2PRegistryErrorCodes

                res_hdr = await session.execute(
                    text("""
                        SELECT s.form_id, d.form_mnemonic, cs.fayda_fan_id, cs.crop_year, cs.production_season, cs.lifecycle_stage, cs.land_id
                        FROM g2p_intake_form_submissions s
                        JOIN g2p_intake_form_definitions d ON s.form_id = d.form_id
                        LEFT JOIN g2p_intake_form_crop_sowns cs ON s.submission_id = cs.submission_id
                        WHERE s.submission_id = :sub_id
                    """),
                    {"sub_id": submission.submission_id},
                )
                row = res_hdr.fetchone()
                if not row:
                    return

                form_mnemonic = str(row[1] or "")
                fayda_fan_id = str(row[2] or "").strip()
                crop_year = str(row[3] or "").strip()
                prod_season = str(row[4] or "").strip()
                lifecycle_stage = str(row[5] or "").strip().upper()
                land_id = str(row[6] or "").strip()

                is_sowing = (lifecycle_stage == "SOWING" or form_mnemonic == "Crop Sown Sowing Intake")
                is_harvesting = (lifecycle_stage == "HARVESTING" or form_mnemonic == "Crop Sown Harvesting Intake")

                if not is_sowing and not is_harvesting:
                    return

                if not fayda_fan_id:
                    tbl = "g2p_intake_form_sowings" if is_sowing else "g2p_intake_form_harvests"
                    res_sub = await session.execute(
                        text(f"SELECT fayda_fan_id, crop_year, season, land_id FROM {tbl} WHERE submission_id = :sub_id LIMIT 1"),
                        {"sub_id": submission.submission_id},
                    )
                    sub_row = res_sub.fetchone()
                    if sub_row:
                        fayda_fan_id = str(sub_row[0] or fayda_fan_id).strip()
                        crop_year = str(sub_row[1] or crop_year).strip()
                        prod_season = str(sub_row[2] or prod_season).strip()
                        land_id = str(sub_row[3] or land_id).strip()

                if not fayda_fan_id:
                    return

                query_m = (
                    "SELECT internal_record_id::text, crop_year, production_season "
                    "FROM g2p_register_crop_sowns "
                    "WHERE fayda_fan_id = :fayda AND record_status = 'ACTIVE'"
                )
                params_m = {"fayda": fayda_fan_id}
                if crop_year:
                    query_m += " AND crop_year = :c_year"
                    params_m["c_year"] = crop_year

                res_m = await session.execute(text(query_m), params_m)
                rows_m = res_m.fetchall()

                matching_master_ids = []
                p_clean = str(prod_season).replace("CROP_SEASON_", "").strip().upper() if prod_season else ""
                for rm in rows_m:
                    r_season = str(rm[2] or "").replace("CROP_SEASON_", "").strip().upper()
                    if not p_clean or p_clean == r_season:
                        matching_master_ids.append(rm[0])

                stage_name = "Sowing" if is_sowing else "Harvesting"
                if not matching_master_ids:
                    year_info = f" in Crop Year '{crop_year}'" if crop_year else ""
                    season_info = f" and Season '{prod_season}'" if prod_season else ""
                    prereq_msg = "Planning and Cultivation must be approved first." if is_sowing else "Sowing must be approved first."
                    raise G2PRegistryException(
                        code=G2PRegistryErrorCodes.REQUEST_VALIDATION_ERROR.value[1],
                        message=(
                            f"Cannot approve {stage_name} submission '{submission.submission_id}': "
                            f"No ACTIVE Crop Sown register record found for Fayda ID '{fayda_fan_id}'{year_info}{season_info}. "
                            f"{prereq_msg}"
                        ),
                    )

                if is_sowing:
                    q_p = "SELECT 1 FROM g2p_register_plannings WHERE link_internal_record_id = ANY(:m_ids) AND record_status = 'ACTIVE'"
                    params_p = {"m_ids": matching_master_ids}
                    if land_id:
                        res_p_land = await session.execute(
                            text(q_p + " AND (land_id = :lid OR TRIM(land_id) = :lid) LIMIT 1"),
                            {**params_p, "lid": land_id},
                        )
                        if not res_p_land.fetchone():
                            res_p_any = await session.execute(text(q_p + " LIMIT 1"), params_p)
                            if not res_p_any.fetchone():
                                raise G2PRegistryException(
                                    code=G2PRegistryErrorCodes.REQUEST_VALIDATION_ERROR.value[1],
                                    message=(
                                        f"Cannot approve Sowing submission '{submission.submission_id}': "
                                        f"Prerequisite Planning stage has not been approved into the Crop Sown registry for Fayda ID '{fayda_fan_id}'. "
                                        f"Planning must be approved before Sowing."
                                    ),
                                )
                            else:
                                raise G2PRegistryException(
                                    code=G2PRegistryErrorCodes.REQUEST_VALIDATION_ERROR.value[1],
                                    message=(
                                        f"Cannot approve Sowing submission '{submission.submission_id}': "
                                        f"Prerequisite Planning stage for Land ID '{land_id}' has not been approved into the Crop Sown registry for Fayda ID '{fayda_fan_id}'. "
                                        f"Planning must be approved before Sowing."
                                    ),
                                )
                    else:
                        res_p = await session.execute(text(q_p + " LIMIT 1"), params_p)
                        if not res_p.fetchone():
                            raise G2PRegistryException(
                                code=G2PRegistryErrorCodes.REQUEST_VALIDATION_ERROR.value[1],
                                message=(
                                    f"Cannot approve Sowing submission '{submission.submission_id}': "
                                    f"Prerequisite Planning stage has not been approved into the Crop Sown registry for Fayda ID '{fayda_fan_id}'. "
                                    f"Planning must be approved before Sowing."
                                ),
                            )

                    q_c = "SELECT 1 FROM g2p_register_cultivations WHERE link_internal_record_id = ANY(:m_ids) AND record_status = 'ACTIVE'"
                    params_c = {"m_ids": matching_master_ids}
                    if land_id:
                        res_c_land = await session.execute(
                            text(q_c + " AND (land_id = :lid OR TRIM(land_id) = :lid) LIMIT 1"),
                            {**params_c, "lid": land_id},
                        )
                        if not res_c_land.fetchone():
                            res_c_any = await session.execute(text(q_c + " LIMIT 1"), params_c)
                            if not res_c_any.fetchone():
                                raise G2PRegistryException(
                                    code=G2PRegistryErrorCodes.REQUEST_VALIDATION_ERROR.value[1],
                                    message=(
                                        f"Cannot approve Sowing submission '{submission.submission_id}': "
                                        f"Prerequisite Cultivation stage has not been approved into the Crop Sown registry for Fayda ID '{fayda_fan_id}'. "
                                        f"Cultivation must be approved before Sowing."
                                    ),
                                )
                            else:
                                raise G2PRegistryException(
                                    code=G2PRegistryErrorCodes.REQUEST_VALIDATION_ERROR.value[1],
                                    message=(
                                        f"Cannot approve Sowing submission '{submission.submission_id}': "
                                        f"Prerequisite Cultivation stage for Land ID '{land_id}' has not been approved into the Crop Sown registry for Fayda ID '{fayda_fan_id}'. "
                                        f"Cultivation must be approved before Sowing."
                                    ),
                                )
                    else:
                        res_c = await session.execute(text(q_c + " LIMIT 1"), params_c)
                        if not res_c.fetchone():
                            raise G2PRegistryException(
                                code=G2PRegistryErrorCodes.REQUEST_VALIDATION_ERROR.value[1],
                                message=(
                                    f"Cannot approve Sowing submission '{submission.submission_id}': "
                                    f"Prerequisite Cultivation stage has not been approved into the Crop Sown registry for Fayda ID '{fayda_fan_id}'. "
                                    f"Cultivation must be approved before Sowing."
                                ),
                            )

                elif is_harvesting:
                    q_s = "SELECT 1 FROM g2p_register_sowings WHERE link_internal_record_id = ANY(:m_ids) AND record_status = 'ACTIVE'"
                    params_s = {"m_ids": matching_master_ids}
                    if land_id:
                        res_s_land = await session.execute(
                            text(q_s + " AND (land_id = :lid OR TRIM(land_id) = :lid) LIMIT 1"),
                            {**params_s, "lid": land_id},
                        )
                        if not res_s_land.fetchone():
                            res_s_any = await session.execute(text(q_s + " LIMIT 1"), params_s)
                            if not res_s_any.fetchone():
                                raise G2PRegistryException(
                                    code=G2PRegistryErrorCodes.REQUEST_VALIDATION_ERROR.value[1],
                                    message=(
                                        f"Cannot approve Harvesting submission '{submission.submission_id}': "
                                        f"Prerequisite Sowing stage has not been approved into the Crop Sown registry for Fayda ID '{fayda_fan_id}'. "
                                        f"Sowing must be approved before Harvesting."
                                    ),
                                )
                            else:
                                raise G2PRegistryException(
                                    code=G2PRegistryErrorCodes.REQUEST_VALIDATION_ERROR.value[1],
                                    message=(
                                        f"Cannot approve Harvesting submission '{submission.submission_id}': "
                                        f"Prerequisite Sowing stage for Land ID '{land_id}' has not been approved into the Crop Sown registry for Fayda ID '{fayda_fan_id}'. "
                                        f"Sowing must be approved before Harvesting."
                                    ),
                                )
                    else:
                        res_s = await session.execute(text(q_s + " LIMIT 1"), params_s)
                        if not res_s.fetchone():
                            raise G2PRegistryException(
                                code=G2PRegistryErrorCodes.REQUEST_VALIDATION_ERROR.value[1],
                                message=(
                                    f"Cannot approve Harvesting submission '{submission.submission_id}': "
                                    f"Prerequisite Sowing stage has not been approved into the Crop Sown registry for Fayda ID '{fayda_fan_id}'. "
                                    f"Sowing must be approved before Harvesting."
                                ),
                            )

            G2PIntakeFormDataService._validate_stage_order_for_approval = _validate_stage_order_for_approval

            orig_approve = G2PIntakeFormDataService.approve_submission
            orig_approve_with_session = G2PIntakeFormDataService.approve_submission_with_session

            async def patched_approve(self, submission_id: str, approved_by: str):
                from openg2p_fastapi_common.context import dbengine
                from sqlalchemy.ext.asyncio import async_sessionmaker
                session_maker = async_sessionmaker(dbengine.get(), expire_on_commit=False)
                async with session_maker() as session:
                    submission = await self._get_submission_or_error(submission_id, session)
                    await self._validate_stage_order_for_approval(submission, session)
                return await orig_approve(self, submission_id, approved_by)

            async def patched_approve_with_session(self, submission_id: str, session, approved_by: str | None = None):
                submission = await self._get_submission_or_error(submission_id, session)
                await self._validate_stage_order_for_approval(submission, session)
                return await orig_approve_with_session(self, submission_id, session, approved_by=approved_by)

            G2PIntakeFormDataService.approve_submission = patched_approve
            G2PIntakeFormDataService.approve_submission_with_session = patched_approve_with_session
            _logger.info("ODK Hook: Patched G2PIntakeFormDataService stage-order approval gate")
    except Exception as e:
        _logger.debug("Could not patch G2PIntakeFormDataService: %s", e)


def _patch_crop_sown_service():
    """Skip only H5/H6 identity cross-record mismatches during ODK ingestion."""
    try:
        from openg2p_registry_cropsown_extension.register_domain.services.g2p_register_domain_service_crop_sown import (
            G2PRegisterDomainServiceCropSown,
        )
        from openg2p_registry_core.errors import G2PRegistryException

        if hasattr(G2PRegisterDomainServiceCropSown, "_odk_patched_fayda_checks"):
            return
        G2PRegisterDomainServiceCropSown._odk_patched_fayda_checks = True
        orig_validate_fayda_season_and_year = (
            G2PRegisterDomainServiceCropSown._validate_fayda_season_and_year
        )

        async def patched_validate_fayda_season_and_year(self, record: dict, session):
            try:
                await orig_validate_fayda_season_and_year(self, record, session)
            except G2PRegistryException as validation_exc:
                if _active_session.get() is not session:
                    raise

                reason = str(validation_exc)
                if "does not match registered Crop Season" in reason:
                    rule_id = "H5"
                elif "does not match registered Crop Year" in reason:
                    rule_id = "H6"
                else:
                    raise

                _logger.warning(
                    "ODK identity validation skipped: %s reason=%s fayda_id=%s submission_id=%s",
                    rule_id,
                    reason,
                    record.get("fayda_fan_id") or record.get("fayda_id"),
                    record.get("submission_id"),
                )

        G2PRegisterDomainServiceCropSown._validate_fayda_season_and_year = (
            patched_validate_fayda_season_and_year
        )
        _logger.info("ODK Hook: Patched G2PRegisterDomainServiceCropSown H5/H6")
    except Exception as e:
        _logger.debug("Could not patch G2PRegisterDomainServiceCropSown H5/H6: %s", e)


def _patch_cultivation_service():
    """Patch G2PRegisterDomainServiceCultivation to fall back to intake form plannings."""
    try:
        from openg2p_registry_cropsown_extension.register_domain.services.g2p_register_domain_service_cultivation import (
            G2PRegisterDomainServiceCultivation,
        )
        from openg2p_registry_cropsown_extension.register_domain.services.domain_validation_utils import parse_date
        from openg2p_registry_core.errors import G2PRegistryException
        from sqlalchemy import text

        if hasattr(G2PRegisterDomainServiceCultivation, "_odk_patched_cultivation"):
            return
        G2PRegisterDomainServiceCultivation._odk_patched_cultivation = True

        orig_resolve_planning_date = G2PRegisterDomainServiceCultivation._resolve_planning_date
        orig_resolve_season_bounds = G2PRegisterDomainServiceCultivation._resolve_season_bounds
        orig_validate_date_after_planning = G2PRegisterDomainServiceCultivation._validate_date_after_planning

        async def patched_validate_date_after_planning(self, record: dict, session):
            try:
                await orig_validate_date_after_planning(self, record, session)
            except G2PRegistryException as exc:
                if _active_session.get() is not session:
                    raise
                _logger.warning(
                    "ODK cross-stage validation skipped: C6 reason=%s land_id=%s submission_id=%s",
                    str(exc), record.get("land_id"), record.get("submission_id"),
                )

        async def patched_resolve_planning_date(self, record: dict, session):
            res = await orig_resolve_planning_date(self, record, session)
            if res is not None:
                return res
            if not session:
                return None
            land_id = str(record.get("land_id") or "").strip()
            if land_id:
                res_in = await session.execute(
                    text("SELECT planned_date FROM g2p_intake_form_plannings WHERE land_id = :land_id ORDER BY created_at DESC LIMIT 1"),
                    {"land_id": land_id}
                )
                row_in = res_in.fetchone()
                if row_in and row_in[0]:
                    return parse_date(row_in[0])
            return None

        async def patched_resolve_season_bounds(self, record: dict, session):
            s_val, e_val = await orig_resolve_season_bounds(self, record, session)
            if s_val is not None and e_val is not None:
                return s_val, e_val
            if not session:
                return s_val, e_val
            land_id = str(record.get("land_id") or "").strip()
            if land_id:
                res_in = await session.execute(
                    text("SELECT start_gc, end_gc FROM g2p_intake_form_plannings WHERE land_id = :land_id ORDER BY created_at DESC LIMIT 1"),
                    {"land_id": land_id}
                )
                row_in = res_in.fetchone()
                if row_in and (row_in[0] or row_in[1]):
                    s = parse_date(row_in[0]) if row_in[0] else s_val
                    e = parse_date(row_in[1]) if row_in[1] else e_val
                    return s, e
            return s_val, e_val

        G2PRegisterDomainServiceCultivation._resolve_planning_date = patched_resolve_planning_date
        G2PRegisterDomainServiceCultivation._resolve_season_bounds = patched_resolve_season_bounds
        G2PRegisterDomainServiceCultivation._validate_date_after_planning = patched_validate_date_after_planning
        _logger.info("ODK Hook: Patched G2PRegisterDomainServiceCultivation")
    except Exception as e:
        _logger.debug("Could not patch G2PRegisterDomainServiceCultivation: %s", e)


def _patch_sowing_service():
    """Patch G2PRegisterDomainServiceSowing with intake form fallbacks."""
    try:
        from openg2p_registry_cropsown_extension.register_domain.services.g2p_register_domain_service_sowing import (
            G2PRegisterDomainServiceSowing,
        )
        from openg2p_registry_cropsown_extension.register_domain.services.domain_validation_utils import (
            parse_date,
            validation_error,
        )
        from openg2p_registry_core.errors import G2PRegistryException
        from sqlalchemy import text

        if hasattr(G2PRegisterDomainServiceSowing, "_odk_patched_sowing"):
            return
        G2PRegisterDomainServiceSowing._odk_patched_sowing = True

        orig_validate_existing_crop_sown = getattr(
            G2PRegisterDomainServiceSowing,
            "_validate_existing_crop_sown_record_required",
            None,
        )
        orig_validate_sowing_area = G2PRegisterDomainServiceSowing._validate_sowing_area_after_cultivation
        orig_validate_sowing_after_cult = G2PRegisterDomainServiceSowing._validate_sowing_after_cultivation
        orig_validate_land_id = G2PRegisterDomainServiceSowing._validate_land_id_matches_planning

        async def patched_validate_existing_crop_sown(self, record: dict, session=None):
            if _active_session.get() is not None:
                # ODK ingestion session — skip this check entirely.
                # Planning/Cultivation approval is enforced later at the staff-approval stage;
                # this only unblocks the Sowing *intake* record from being created pre-approval.
                _logger.warning(
                    "ODK cross-stage validation skipped: _validate_existing_crop_sown_record_required for Fayda=%s land_id=%s",
                    record.get("fayda_fan_id") or record.get("fayda_id"),
                    record.get("land_id"),
                )
                return
            if orig_validate_existing_crop_sown:
                return await orig_validate_existing_crop_sown(self, record, session=session)

        async def patched_validate_sowing_area(self, record: dict, session):
            try:
                try:
                    await orig_validate_sowing_area(self, record, session)
                except G2PRegistryException as validation_exc:
                    if _active_session.get() is session:
                        _logger.warning(
                            "ODK cross-stage validation skipped: SW6 reason=%s land_id=%s submission_id=%s",
                            str(validation_exc), record.get("land_id"), record.get("submission_id"),
                        )
                        return
                    raise
            except Exception as exc:
                land_id = str(record.get("land_id") or "").strip()
                area_sown = float(record.get("area_sown") or 0)
                if session and land_id:
                    res_ic = await session.execute(
                        text("SELECT actual_crop_area FROM g2p_intake_form_cultivations WHERE TRIM(land_id) = :land_id AND actual_crop_area IS NOT NULL ORDER BY created_at DESC LIMIT 1"),
                        {"land_id": land_id}
                    )
                    row_ic = res_ic.fetchone()
                    if row_ic and row_ic[0] is not None:
                        max_area = float(row_ic[0])
                        if area_sown > max_area:
                            validation_error(
                                f"Area Sown ({area_sown}) cannot exceed Cultivated Area ({max_area}) for Land ID '{land_id}'."
                            )
                        return
                raise exc

        async def patched_validate_sowing_after_cult(self, record: dict, session):
            try:
                try:
                    await orig_validate_sowing_after_cult(self, record, session)
                except G2PRegistryException as validation_exc:
                    if _active_session.get() is session:
                        _logger.warning(
                            "ODK cross-stage validation skipped: SW5 reason=%s land_id=%s submission_id=%s",
                            str(validation_exc), record.get("land_id"), record.get("submission_id"),
                        )
                        return
                    raise
            except Exception as exc:
                sowing_date = parse_date(record.get("sowing_date"))
                land_id = str(record.get("land_id") or "").strip()
                if session and land_id and sowing_date:
                    prior_date = None
                    prior_stage = None
                    res_ic = await session.execute(
                        text("SELECT actual_cultivation_date FROM g2p_intake_form_cultivations WHERE land_id = :land_id ORDER BY created_at DESC LIMIT 1"),
                        {"land_id": land_id}
                    )
                    row_ic = res_ic.fetchone()
                    if row_ic and row_ic[0]:
                        prior_date = parse_date(row_ic[0])
                        prior_stage = "Cultivation Date"
                    else:
                        res_ip = await session.execute(
                            text("SELECT planned_date FROM g2p_intake_form_plannings WHERE land_id = :land_id ORDER BY created_at DESC LIMIT 1"),
                            {"land_id": land_id}
                        )
                        row_ip = res_ip.fetchone()
                        if row_ip and row_ip[0]:
                            prior_date = parse_date(row_ip[0])
                            prior_stage = "Planned Date"

                    if prior_date:
                        if sowing_date < prior_date:
                            validation_error(
                                f"Sowing Date ({sowing_date.strftime('%Y-%m-%d')}) cannot be earlier than "
                                f"{prior_stage} ({prior_date.strftime('%Y-%m-%d')})."
                            )
                        return
                raise exc

        async def patched_validate_land_id(self, record: dict, session):
            try:
                try:
                    await orig_validate_land_id(self, record, session)
                except G2PRegistryException as validation_exc:
                    if _active_session.get() is session:
                        _logger.warning(
                            "ODK cross-stage validation skipped: SW4 reason=%s land_id=%s submission_id=%s",
                            str(validation_exc), record.get("land_id"), record.get("submission_id"),
                        )
                        return
                    raise
            except Exception as exc:
                land_id = str(record.get("land_id") or "").strip()
                if session and land_id:
                    for tbl in ("g2p_intake_form_plannings", "g2p_intake_form_cultivations"):
                        res = await session.execute(
                            text(f"SELECT land_id FROM {tbl} WHERE land_id = :land_id LIMIT 1"),
                            {"land_id": land_id}
                        )
                        if res.fetchone():
                            return
                raise exc

        if orig_validate_existing_crop_sown:
            G2PRegisterDomainServiceSowing._validate_existing_crop_sown_record_required = patched_validate_existing_crop_sown
        G2PRegisterDomainServiceSowing._validate_sowing_area_after_cultivation = patched_validate_sowing_area
        G2PRegisterDomainServiceSowing._validate_sowing_after_cultivation = patched_validate_sowing_after_cult
        G2PRegisterDomainServiceSowing._validate_land_id_matches_planning = patched_validate_land_id
        _logger.info("ODK Hook: Patched G2PRegisterDomainServiceSowing")
    except Exception as e:
        _logger.debug("Could not patch G2PRegisterDomainServiceSowing: %s", e)


def _patch_harvest_service():
    """Patch G2PRegisterDomainServiceHarvest with intake form fallbacks and date comparison."""
    try:
        from openg2p_registry_cropsown_extension.register_domain.services.g2p_register_domain_service_harvest import (
            G2PRegisterDomainServiceHarvest,
        )
        from openg2p_registry_cropsown_extension.register_domain.services.domain_validation_utils import (
            as_float,
            get_attribute_variants,
            parse_date,
            validation_error,
        )
        from openg2p_registry_core.errors import G2PRegistryException
        from sqlalchemy import text

        if hasattr(G2PRegisterDomainServiceHarvest, "_odk_patched_harvest"):
            return
        G2PRegisterDomainServiceHarvest._odk_patched_harvest = True

        orig_validate_existing_crop_sown_harvest = getattr(
            G2PRegisterDomainServiceHarvest,
            "_validate_existing_crop_sown_record_required",
            None,
        )
        orig_validate_harvest_after_sowing = G2PRegisterDomainServiceHarvest._validate_harvest_after_sowing
        orig_validate_harvest_area = G2PRegisterDomainServiceHarvest._validate_harvest_area_after_sowing
        orig_validate_land_id = G2PRegisterDomainServiceHarvest._validate_land_id_matches_sowing
        orig_validate_harvest_season_date = G2PRegisterDomainServiceHarvest._validate_date_in_season_enhanced

        async def patched_validate_existing_crop_sown_harvest(self, record: dict, session=None):
            if _active_session.get() is not None:
                _logger.warning(
                    "ODK cross-stage validation skipped: _validate_existing_crop_sown_record_required (Harvest) for Fayda=%s land_id=%s",
                    record.get("fayda_fan_id") or record.get("fayda_id"),
                    record.get("land_id"),
                )
                return
            if orig_validate_existing_crop_sown_harvest:
                return await orig_validate_existing_crop_sown_harvest(self, record, session=session)

        async def patched_validate_harvest_season_date(self, record: dict, field: str, session):
            try:
                await orig_validate_harvest_season_date(self, record, field, session)
            except G2PRegistryException as validation_exc:
                if _active_session.get() is not session:
                    raise
                if not str(validation_exc).startswith("Cluster Harvest Date ("):
                    raise
                _logger.warning(
                    "ODK cross-stage validation skipped: HV3 reason=%s land_id=%s submission_id=%s field=%s",
                    str(validation_exc), record.get("land_id"), record.get("submission_id"), field,
                )

        async def patched_validate_harvest_after_sowing(self, record: dict, session):
            harvest_date = parse_date(record.get("harvest_date"))
            if harvest_date is None:
                return
            try:
                try:
                    await orig_validate_harvest_after_sowing(self, record, session)
                except G2PRegistryException as validation_exc:
                    if _active_session.get() is session:
                        reason = str(validation_exc)
                        if "A sowing record is required" in reason:
                            rule_id = "HV6"
                        elif "must be after the Sowing Date" in reason:
                            rule_id = "HV5"
                        else:
                            raise
                        _logger.warning(
                            "ODK cross-stage validation skipped: %s reason=%s land_id=%s submission_id=%s",
                            rule_id, reason, record.get("land_id"), record.get("submission_id"),
                        )
                        return
                    raise
            except Exception as exc:
                land_id = str(record.get("land_id") or "").strip()
                if session and land_id:
                    prior_date = None
                    prior_stage = None
                    # 1. Check active register sowings
                    res_s_reg = await session.execute(
                        text("SELECT sowing_date FROM g2p_register_sowings WHERE land_id = :land_id AND record_status = 'ACTIVE' ORDER BY created_at DESC LIMIT 1"),
                        {"land_id": land_id}
                    )
                    row_s_reg = res_s_reg.fetchone()
                    if row_s_reg and row_s_reg[0]:
                        prior_date = parse_date(row_s_reg[0])
                        prior_stage = "Sowing Date"
                    else:
                        # 2. Check intake form sowings
                        res_s = await session.execute(
                            text("SELECT sowing_date FROM g2p_intake_form_sowings WHERE land_id = :land_id ORDER BY created_at DESC LIMIT 1"),
                            {"land_id": land_id}
                        )
                        row_s = res_s.fetchone()
                        if row_s and row_s[0]:
                            prior_date = parse_date(row_s[0])
                            prior_stage = "Sowing Date"
                        else:
                            # 3. Check intake form cultivations
                            res_c = await session.execute(
                                text("SELECT actual_cultivation_date FROM g2p_intake_form_cultivations WHERE land_id = :land_id ORDER BY created_at DESC LIMIT 1"),
                                {"land_id": land_id}
                            )
                            row_c = res_c.fetchone()
                            if row_c and row_c[0]:
                                prior_date = parse_date(row_c[0])
                                prior_stage = "Cultivation Date"

                    if prior_date:
                        if harvest_date < prior_date:
                            validation_error(
                                f"Harvest Date ({harvest_date.strftime('%Y-%m-%d')}) cannot be earlier than "
                                f"{prior_stage} ({prior_date.strftime('%Y-%m-%d')})."
                            )
                        return
                raise exc

        async def patched_validate_harvest_area(self, record: dict, session):
            try:
                try:
                    await orig_validate_harvest_area(self, record, session)
                except G2PRegistryException as validation_exc:
                    if _active_session.get() is session:
                        reason = str(validation_exc)
                        if not ("Area Harvested (" in reason and "cannot exceed" in reason):
                            raise
                        _logger.warning(
                            "ODK cross-stage validation skipped: HV7 reason=%s land_id=%s submission_id=%s",
                            reason, record.get("land_id"), record.get("submission_id"),
                        )
                        return
                    raise
            except Exception as exc:
                land_id = str(record.get("land_id") or "").strip()
                area_harvested = float(record.get("area_harvested") or 0)
                if session and land_id:
                    res_s = await session.execute(
                        text("SELECT area_sown FROM g2p_intake_form_sowings WHERE TRIM(land_id) = :land_id AND area_sown IS NOT NULL ORDER BY created_at DESC LIMIT 1"),
                        {"land_id": land_id}
                    )
                    row_s = res_s.fetchone()
                    max_area = None
                    if row_s and row_s[0] is not None:
                        max_area = float(row_s[0])
                    else:
                        res_c = await session.execute(
                            text("SELECT actual_crop_area FROM g2p_intake_form_cultivations WHERE TRIM(land_id) = :land_id AND actual_crop_area IS NOT NULL ORDER BY created_at DESC LIMIT 1"),
                            {"land_id": land_id}
                        )
                        row_c = res_c.fetchone()
                        if row_c and row_c[0] is not None:
                            max_area = float(row_c[0])

                    if max_area is not None:
                        if area_harvested > max_area:
                            validation_error(
                                f"Area Harvested ({area_harvested}) cannot exceed Allowed Area ({max_area}) for Land ID '{land_id}'."
                            )
                        return
                raise exc

            # The domain validator above compares against the active register and
            # the current submission. ODK submits each lifecycle stage as a
            # separate intake submission, so an earlier pending ODK Sowing is not
            # visible to those queries. During ODK ingestion only, use that
            # staged Sowing as the HV7 comparison source and skip just this
            # cross-stage mismatch. Manual/UI calls keep the original behavior.
            if _active_session.get() is session and session:
                land_id = str(record.get("land_id") or "").strip()
                area_harvested = as_float(
                    record.get("area_harvested")
                    if record.get("area_harvested") is not None
                    else record.get("cluster_area_harvested")
                )
                if land_id and area_harvested is not None and area_harvested > 0:
                    season_vars = get_attribute_variants(record.get("season"), "CROP_SEASON")
                    query = (
                        "SELECT area_sown FROM g2p_intake_form_sowings "
                        "WHERE TRIM(land_id) = :land_id AND area_sown IS NOT NULL"
                    )
                    params = {"land_id": land_id}
                    if season_vars:
                        query += " AND season = ANY(:season_vars)"
                        params["season_vars"] = season_vars
                    query += " ORDER BY created_at DESC LIMIT 1"
                    result = await session.execute(text(query), params)
                    row = result.fetchone()
                    if row and row[0] is not None:
                        area_sown = float(row[0])
                        if area_harvested > area_sown:
                            _logger.warning(
                                "ODK cross-stage validation skipped: HV7 reason=%s "
                                "land_id=%s submission_id=%s",
                                f"Area Harvested ({area_harvested} ha) cannot exceed "
                                f"Area Sown in Sowing ({area_sown} ha).",
                                land_id,
                                record.get("submission_id"),
                            )
                            return

        async def patched_validate_land_id(self, record: dict, session):
            try:
                try:
                    await orig_validate_land_id(self, record, session)
                except G2PRegistryException as validation_exc:
                    if _active_session.get() is session:
                        _logger.warning(
                            "ODK cross-stage validation skipped: HV4 reason=%s land_id=%s submission_id=%s",
                            str(validation_exc), record.get("land_id"), record.get("submission_id"),
                        )
                        return
                    raise
            except Exception as exc:
                land_id = str(record.get("land_id") or "").strip()
                if session and land_id:
                    for tbl in ("g2p_intake_form_sowings", "g2p_intake_form_cultivations", "g2p_intake_form_plannings"):
                        res = await session.execute(
                            text(f"SELECT land_id FROM {tbl} WHERE land_id = :land_id LIMIT 1"),
                            {"land_id": land_id}
                        )
                        if res.fetchone():
                            return
                raise exc

        if orig_validate_existing_crop_sown_harvest:
            G2PRegisterDomainServiceHarvest._validate_existing_crop_sown_record_required = patched_validate_existing_crop_sown_harvest
        G2PRegisterDomainServiceHarvest._validate_harvest_after_sowing = patched_validate_harvest_after_sowing
        G2PRegisterDomainServiceHarvest._validate_harvest_area_after_sowing = patched_validate_harvest_area
        G2PRegisterDomainServiceHarvest._validate_land_id_matches_sowing = patched_validate_land_id
        G2PRegisterDomainServiceHarvest._validate_date_in_season_enhanced = patched_validate_harvest_season_date
        _logger.info("ODK Hook: Patched G2PRegisterDomainServiceHarvest")
    except Exception as e:
        _logger.debug("Could not patch G2PRegisterDomainServiceHarvest: %s", e)


def _patch_awe_proxy_controller_service():
    """Patch G2PAweProxyControllerService when AWE is disabled."""
    try:
        from openg2p_registry_core.controller_services.g2p_awe_proxy_controller_service import (
            G2PAweProxyControllerService,
        )
        from openg2p_registry_core.helpers.awe_config import get_awe_settings

        orig_list_my_tasks = G2PAweProxyControllerService.list_my_tasks
        orig_list_tasks_for_request = G2PAweProxyControllerService.list_tasks_for_request
        orig_my_task_stats = G2PAweProxyControllerService.my_task_stats

        async def patched_list_my_tasks(self, payload=None, *, bearer_token: str):
            if not get_awe_settings().awe_enabled:
                return {
                    "items": [],
                    "total": 0,
                    "page": getattr(payload, "page", 1) if payload else 1,
                    "page_size": getattr(payload, "page_size", 10) if payload else 10,
                }
            return await orig_list_my_tasks(self, payload, bearer_token=bearer_token)

        async def patched_list_tasks_for_request(self, payload=None, *, bearer_token: str):
            if not get_awe_settings().awe_enabled:
                return {"items": [], "total": 0}
            return await orig_list_tasks_for_request(self, payload, bearer_token=bearer_token)

        async def patched_my_task_stats(self, payload=None, *, bearer_token: str):
            if not get_awe_settings().awe_enabled:
                return {
                    "total": 0,
                    "change_request_count": 0,
                    "intake_form_count": 0,
                }
            return await orig_my_task_stats(self, payload, bearer_token=bearer_token)

        G2PAweProxyControllerService.list_my_tasks = patched_list_my_tasks
        G2PAweProxyControllerService.list_tasks_for_request = patched_list_tasks_for_request
        G2PAweProxyControllerService.my_task_stats = patched_my_task_stats
        _logger.info("ODK Hook: Patched G2PAweProxyControllerService for awe_enabled guard")
    except Exception as e:
        _logger.debug("Could not patch G2PAweProxyControllerService: %s", e)


def _patch_document_service():
    """Ensure G2PDocumentService auto-instantiates on get_component()."""
    try:
        from openg2p_registry_core.services.g2p_document_service import G2PDocumentService
        orig_get_component = G2PDocumentService.get_component

        @classmethod
        def patched_get_component(cls, name="", strict=False):
            comp = orig_get_component(name=name, strict=strict)
            if comp is None:
                comp = cls(name=name)
            return comp

        G2PDocumentService.get_component = patched_get_component
        _logger.info("ODK Hook: Patched G2PDocumentService.get_component")
    except Exception as e:
        _logger.debug("Could not patch G2PDocumentService: %s", e)


def _patch_auth_redis():
    """Ensure RedisRefreshTokenStore uses redis://redis:6379/0."""
    try:
        redis_url = (
            os.environ.get("REGISTRY_STAFF_PORTAL_API_AUTH_REDIS_URL")
            or os.environ.get("COMMON_AUTH_REDIS_URL")
            or "redis://redis:6379/0"
        )
        if "COMMON_AUTH_REDIS_URL" not in os.environ:
            os.environ["COMMON_AUTH_REDIS_URL"] = redis_url
        from iam_core.services.redis_refresh_token_store import RedisRefreshTokenStore
        orig_init = RedisRefreshTokenStore.__init__

        def patched_init(self, ttl_seconds=None, redis_url_arg=None):
            if not redis_url_arg or "localhost" in redis_url_arg:
                redis_url_arg = redis_url
            orig_init(self, ttl_seconds=ttl_seconds, redis_url=redis_url_arg)

        RedisRefreshTokenStore.__init__ = patched_init
    except Exception as e:
        _logger.debug("Could not patch RedisRefreshTokenStore: %s", e)


def _patch_fayda_validation():
    """Allow clean 16-digit fayda_fan_id to pass validation in domain schemas and services."""
    try:
        import re
        from openg2p_registry_cropsown_extension.register_domain.schemas.crop_sown import G2PRegisterSchemaCropSown, G2PSchemaCropSown

        @classmethod
        def patched_validate(cls, v):
            if v is not None and str(v).strip() != "":
                s = str(v).strip().replace(" ", "").replace("-", "")
                if not re.match(r"^\d{16}$", s):
                    raise ValueError(f"Fayda ID must be 16 digits (got '{v}')")
            return v

        for schema_cls in (G2PRegisterSchemaCropSown, G2PSchemaCropSown):
            if hasattr(schema_cls, "__pydantic_decorators__") and "validate_fayda_fan_id" in schema_cls.__pydantic_decorators__.field_validators:
                schema_cls.__pydantic_decorators__.field_validators["validate_fayda_fan_id"].func = patched_validate.__get__(schema_cls, schema_cls)
                schema_cls.__pydantic_complete__ = False
                schema_cls.model_rebuild(force=True)

        from openg2p_registry_cropsown_extension.register_domain.services.g2p_register_domain_service_crop_sown import G2PRegisterDomainServiceCropSown
        from openg2p_registry_cropsown_extension.register_domain.services.domain_validation_utils import validation_error

        def patched_service_validate_fayda(self, record):
            value = record.get("fayda_fan_id")
            if value is None or str(value).strip() in ("", "None", "null", "Select"):
                validation_error("Fayda ID (FAN) is required in Farmer Identity.")
            s = str(value).strip().replace(" ", "").replace("-", "")
            if not re.match(r"^\d{16}$", s):
                validation_error(f"Fayda ID must be 16 digits (got '{value}')")

        G2PRegisterDomainServiceCropSown._validate_fayda_fan_id = patched_service_validate_fayda
        _logger.info("ODK Hook: Patched fayda_fan_id validation to accept clean 16 digits")
    except Exception as e:
        _logger.debug("Could not patch fayda validation: %s", e)


def install_hooks():
    """Run hook initialization."""
    _alias_extension_modules()
    _patch_base_initializer()
    _patch_document_handler()
    _ensure_core_services()
    _ensure_dbengine_initialized()
    _ensure_master_data_engine()
    _connect_celery_signals()
    _patch_request_response_helper()
    _patch_celery_worker()
    _patch_intake_form_data_service()
    _patch_crop_sown_service()
    _patch_cultivation_service()
    _patch_sowing_service()
    _patch_harvest_service()
    _patch_awe_proxy_controller_service()
    _patch_document_service()
    _patch_auth_redis()
    _patch_fayda_validation()


install_hooks()


