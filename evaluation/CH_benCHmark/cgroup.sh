#!/bin/bash

sudo chmod o+w /sys/fs/cgroup/cgroup.procs

if [ ! -d /sys/fs/cgroup/locator.slice ]; then
	sudo mkdir /sys/fs/cgroup/locator.slice
	sudo chmod o+w /sys/fs/cgroup/locator.slice/cgroup.procs
	echo 48G | sudo tee /sys/fs/cgroup/locator.slice/memory.max > /dev/null
fi