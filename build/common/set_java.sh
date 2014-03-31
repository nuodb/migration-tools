#!/bin/bash

set -e
set -o pipefail

export JAVA_HOME=${ARG_JDK}
export PATH=.:${JAVA_HOME}/bin:${PATH}

