#!/bin/bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

source ./env.sh

export PROCESS_NAMES=("Kafka" "CoordinatorServer" "TabletServer" "arthas-boot.jar")
export WORKER_PROCESS_NAMES=("BenchmarkWorker" "arthas-boot.jar")


# kill client worker process add delete log
for WORKER in "${OMB_CLIENT_ADDRESSES[@]}"; do
	# kill process
	echo "$WORKER:"
	for PROCESS in "${WORKER_PROCESS_NAMES[@]}"; do
		COMMAND="ps -ef | grep java | grep \"$PROCESS\" | grep -v grep | awk '{print \$2}'"
		PID=$(ssh "$WORKER" "$COMMAND")
		if [ -z "$PID" ]; then
			echo "No process $PROCESS"
		else
			ssh "$WORKER" "kill -9 $PID"
                	echo "killed process $PROCESS"
                	sleep 1
        	fi
	done

	# delete arthas result
	COMMAND="rm -rf $OMB_INSTALL_PATH/arthas-output"
        ssh "$WORKER" "$COMMAND"

	# delete the json file
	COMMAND="rm -rf $OMB_INSTALL_PATH/*.json"
	ssh "$WORKER" "$COMMAND"

	# delete log
	COMMAND="rm -rf $OMB_INSTALL_PATH/benchmark-worker.log $OMB_INSTALL_PATH/benchmark_worker_output.log"
	ssh "$WORKER" "$COMMAND"
        echo "delete log for OMB worker"

	# print empty row
	echo
done