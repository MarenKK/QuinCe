#!/bin/bash

############################################################
#
# This script undoes changes done by setup_hide_changes,
# so that changes to config files are visible to git. This
# concerns the files:
# - quince.properties
# - context.xml
# - web.xml
#
############################################################

git update-index --no-assume-unchanged   \
  WebApp/WebContent/META-INF/context.xml \
  WebApp/WebContent/WEB-INF/web.xml      \
  configuration/quince.properties        \
  NRT/config.toml  \
  WebApp/WebContent/resources/python/export/config_carbon.toml \
  WebApp/WebContent/resources/python/export/config_quince.toml \
  WebApp/WebContent/resources/python/export/config_copernicus.toml

