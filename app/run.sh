#!/bin/sh

required_env_vars="COOKIE_NPS_INVESTIDOR AUTHORIZATION_NPS_INVESTIDOR"

for required_env_var in $required_env_vars; do
  if [ -z "$(eval echo \$$required_env_var)" ]; then
    echo "Required env var $required_env_var not found"
    exit 1
  fi
done

python /app/main.py
