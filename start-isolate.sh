#!/bin/bash

# Trap for cleanup on exit (rmdir to avoid leaks)
cleanup() {
    rmdir /sys/fs/cgroup/isolate 2>/dev/null || true
}
trap cleanup EXIT

# Remount cgroup root as read-write (unified v2 default ro)
mount -o remount,rw /sys/fs/cgroup

# Manual cgroup v2 root creation (unified scope dir for Isolate)
mkdir -p /sys/fs/cgroup/isolate

# V2 delegation: enable controllers for sub-cgroups + join PID
echo "+cpu +memory +pids +io +rdma" > /sys/fs/cgroup/isolate/cgroup.subtree_control
echo $$ > /sys/fs/cgroup/isolate/cgroup.procs

# Update config file with simple root
echo "/sys/fs/cgroup/isolate" > /run/isolate/cgroup

# Export env for Isolate binary (overrides file if needed)
export ISOLATE_CGROOT=/sys/fs/cgroup/isolate
export ISOLATE_CG=2

# Wait for delegation writes
sleep 2

# Verify root dir exists
if [ -d "/sys/fs/cgroup/isolate" ]; then
    echo "CGroup root created successfully"
else
    echo "Warning: CGroup root failed"
    ls -la /sys/fs/cgroup/ | grep isolate || true  # Debug
fi

# Execute the main application
exec "$@"