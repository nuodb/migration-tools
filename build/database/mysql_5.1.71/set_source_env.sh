#!/bin/bash

set -e
set -o pipefail

export SOURCE_DRIVER=com.mysql.jdbc.Driver
export SOURCE_CATALOG=nuodbtest
export SOURCE_URL=jdbc:mysql://localhost:3306/${SOURCE_CATALOG}
export SOURCE_USERNAME=root
export SOURCE_PASSWORD=
export SOURCE_JDBCJAR=${ARG_DRIVER}

