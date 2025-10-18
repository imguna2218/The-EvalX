#!/bin/bash
set -e

echo "--- Setting up Base Chroot Environment ---"

BASE_CHROOT="/opt/evalx/chroots/base"
sudo mkdir -p $BASE_CHROOT/{bin,lib,lib64,usr,dev,proc,tmp,etc}

# Copy essential binaries
sudo cp /bin/sh $BASE_CHROOT/bin/
sudo cp /bin/bash $BASE_CHROOT/bin/
sudo cp /bin/ls $BASE_CHROOT/bin/
sudo cp /bin/cat $BASE_CHROOT/bin/
sudo cp /bin/echo $BASE_CHROOT/bin/
sudo cp /bin/mkdir $BASE_CHROOT/bin/
sudo cp /bin/rm $BASE_CHROOT/bin/
sudo cp /bin/cp $BASE_CHROOT/bin/

# Copy essential libraries
copy_library() {
    local lib=$1
    if [ -f "$lib" ]; then
        sudo cp "$lib" "$BASE_CHROOT$lib"
        # Copy all dependencies
        for dep in $(ldd "$lib" 2>/dev/null | grep -o '/[^ ]*' | grep -v '('); do
            if [ -f "$dep" ]; then
                sudo mkdir -p "$BASE_CHROOT$(dirname "$dep")"
                sudo cp "$dep" "$BASE_CHROOT$dep"
            fi
        done
    fi
}

# Copy dynamic linker
sudo cp /lib64/ld-linux-x86-64.so.2 $BASE_CHROOT/lib64/
if [ -e /lib/ld-linux-x86-64.so.2 ]; then
    sudo cp /lib/ld-linux-x86-64.so.2 $BASE_CHROOT/lib/
fi

# Copy essential libraries
copy_library /lib/x86_64-linux-gnu/libc.so.6
copy_library /lib/x86_64-linux-gnu/libm.so.6
copy_library /lib/x86_64-linux-gnu/libdl.so.2
copy_library /lib/x86_64-linux-gnu/libpthread.so.0
copy_library /lib/x86_64-linux-gnu/libgcc_s.so.1
copy_library /lib/x86_64-linux-gnu/libstdc++.so.6

# Create device files
sudo mknod -m 666 $BASE_CHROOT/dev/null c 1 3
sudo mknod -m 666 $BASE_CHROOT/dev/zero c 1 5
sudo mknod -m 666 $BASE_CHROOT/dev/urandom c 1 9

# Set permissions
sudo chmod -R +rx $BASE_CHROOT

echo "✅ Base chroot environment created successfully"