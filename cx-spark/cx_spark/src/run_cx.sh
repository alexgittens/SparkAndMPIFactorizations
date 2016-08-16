#!/bin/bash

case "$1" in
    --help|-h) spark-submit --driver-java-options '-Dlog4j.configuration=log4j.properties' run_cx.py print_help
        ;;
    *) spark-submit --driver-java-options '-Dlog4j.configuration=log4j.properties' --executor-memory 7G --driver-memory 8G --py-files cx.py,rma_utils.py,utils.py,rowmatrix.py,sparse_row_matrix.py run_cx.py $@
esac
