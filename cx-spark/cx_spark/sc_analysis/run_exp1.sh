#!/bin/bash
spark-submit --driver-java-options '-Dlog4j.configuration=log4j.properties' --executor-memory 7G --driver-memory 8G \
 --py-files /global/u2/m/msingh/sc_paper/new_version/sc-2015/cx_spark/src/comp_sketch.py,/global/u2/m/msingh/sc_paper/new_version/sc-2015/cx_spark/src/cx.py,/global/u2/m/msingh/sc_paper/new_version/sc-2015/cx_spark/src/rma_utils.py,/global/u2/m/msingh/sc_paper/new_version/sc-2015/cx_spark/src/utils.py,/global/u2/m/msingh/sc_paper/new_version/sc-2015/cx_spark/src/rowmatrix.py,/global/u2/m/msingh/sc_paper/new_version/sc-2015/cx_spark/src/projections.py /global/u2/m/msingh/sc_paper/new_version/sc-2015/cx_spark/src/exp1.py $@ 2>&1 | tee test.log

