#!/bin/bash
if [ "$#" -lt 1 ]; then
   process_name="Driver"
else
   process_name=${1}
fi
jps | grep ${process_name} | awk '{print $1}' | xargs --no-run-if-empty -- kill -9