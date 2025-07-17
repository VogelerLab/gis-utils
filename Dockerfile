# Author: Daniel Rode


# https://docs.docker.com/reference/dockerfile/


FROM alpine:edge

LABEL Author="Daniel Rode"

# Install Alpine Linux packages
RUN ash <<'EOF'
    set -e  # Exit on error

    apk update
    apk add \
        R \
        bash \
        fd \
        gdal-tools \
        openssh \
        parallel \
        pdal \
        proj-util \
        python3 \
        udunits \
    ;
EOF

# Install binary dependencies from sister containers
COPY --from=ghcr.io/vogelerlab/lidr-container:main \
    /usr/local/rlib /usr/local/rlib
COPY --from=ghcr.io/vogelerlab/python-gis-container:main \
    /usr/local/pylib /usr/local/pylib
COPY --from=ghcr.io/vogelerlab/pdal_wrench:main \
    /usr/local/bin/pdal_wrench /usr/local/bin/pdal_wrench

ENV R_LIBS_USER=/usr/local/rlib
ENV PATH="/usr/local/pylib/bin:$PATH:/vogeler/bin"

# Install scripts from this repo
COPY bin /vogeler/bin
COPY lib /vogeler/lib

ENV PYTHONPATH="/vogeler/lib/py"
