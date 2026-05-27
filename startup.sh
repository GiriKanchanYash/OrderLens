#!/bin/bash
set -e

# Azure Web App startup script for Streamlit
python -m streamlit run app.py \
  --server.port 8000 \
  --server.address 0.0.0.0 \
  --server.enableCORS true \
  --server.enableXsrfProtection true \
  --server.headless true \
  --server.maxUploadSize 200 \
  --logger.level warning \
  --client.showErrorDetails true \
  --client.toolbarMode minimal