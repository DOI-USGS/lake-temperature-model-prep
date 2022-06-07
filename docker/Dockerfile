FROM rocker/geospatial:4.1.2

# Disable the annoying bell on WSL2
RUN sed -i 's/^# set bell-style none$/set bell-style none/' /etc/inputrc
RUN echo 'set visualbell' >> /root/.vimrc

# Add DOI CA to local CAs so that SSL can work over VPN
COPY DOIRootCA2.crt /usr/local/share/ca-certificates
RUN update-ca-certificates
ENV CURL_CA_BUNDLE /etc/ssl/certs/ca-certificates.crt

# Dependencies
RUN apt-get update && apt-get -y --no-install-recommends install \
  apt-utils \
  libgd-dev \
  libnetcdf-dev \
  m4  \
  vim-tiny \
  && rm -rf /var/lib/apt/lists/*

# Install our custom packages
RUN Rscript -e 'library(remotes); \
                install_github("richfitz/remake", upgrade = "never"); \
                install_github("USGS-R/scipiper", upgrade = "never"); \
                install_github("USGS-R/geoknife", upgrade = "never"); \
                install_github("USGS-R/ncdfgeom", upgrade = "never"); \
                install_github("USGS-R/lakeattributes", upgrade = "never");' \
  && rm -rf /tmp/downloaded_packages/ /tmp/*.rds

# Install the necessary pipeline and parallelization packages for R
RUN install2.r --error \
  arrow \
  dataRetrieval \
  data.table \
  dplyr \
  feather \
  ggplot2 \
  jsonlite \
  MESS \
  ncdf4 \
  odbc \
  purrr \
  readr \
  retry \
  RCurl \
  RNetCDF \
  sbtools \
  scico \
  sf \
  smoothr \
  targets \
  tidyr \
  && rm -rf /tmp/downloaded_packages/ /tmp/*.rds

# Fix umask problems with RStudio in singularity
RUN echo "Sys.umask(mode=002)" >> /usr/local/lib/R/etc/Rprofile.site

# RStudio does not pick up the full environment from the singularity container!
# The normal way to make environment variables visible to RStudio would be to
# add them to /usr/local/lib/R/etc/Renviron.site, as is done with
# DOCKER_BUILD_REPO above. However, singularity itself creates some environment
# variables which we want to have visible, and once we're in that environment
# we no longer have write access to /usr/local/lib/R/etc/Renviron.site. The
# workaround is to create a writable empty file on the host and bind-mount it to
# /usr/local/lib/R/etc/Renviron.site in the container (see the slurm launch
# script launch-rstudio-container.slurm for this). Here we create a copy of the
# Renviron.site so that we don't lose its contents when we bind-mount the empty
# file.
RUN cp /usr/local/lib/R/etc/Renviron.site /usr/local/lib/R/etc/Renviron.site.orig
