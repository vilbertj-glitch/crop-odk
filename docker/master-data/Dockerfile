# Crop Sown Registry — Master Data API image.
#
# Bakes route aliases and schema handling into the upstream master-data-api base image:
# - Aliases /geo/get_all_g2p_geo_levels and /geo/get_g2p_geo_level_values
# - Supports default request envelopes for cascading address dropdowns
# - Installs curl for container health checks
#
# Build context = repo root.
ARG MASTER_DATA_IMAGE=openg2p/master-data-api:1.1.0
FROM ${MASTER_DATA_IMAGE}

USER root
RUN apk add --no-cache curl

COPY docker/master-data/patch_geo_routes.py /app/patch_geo_routes.py
RUN python3 /app/patch_geo_routes.py
