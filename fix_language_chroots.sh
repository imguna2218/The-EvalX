#!/bin/bash
set -e

echo "--- Fixing Language Chroot Environments ---"

BASE_CHROOT="/opt/evalx/chroots/base"

# Function to setup language-specific chroot
setup_language_chroot() {
    local lang=$1
    local binaries=$2
    
    echo "Setting up chroot for: $lang"
    local lang_chroot="/opt/evalx/chroots/$lang"
    
    # Copy base chroot
    sudo rm -rf "$lang_chroot"
    sudo cp -r "$BASE_CHROOT" "$lang_chroot"
    
    # Copy language-specific binaries and their dependencies
    for binary in $binaries; do
        if [ -f "$binary" ]; then
            # Copy binary
            sudo mkdir -p "$lang_chroot$(dirname "$binary")"
            sudo cp "$binary" "$lang_chroot$binary"
            
            # Copy all library dependencies
            for lib in $(ldd "$binary" 2>/dev/null | grep -o '/[^ ]*' | grep -v '('); do
                if [ -f "$lib" ]; then
                    sudo mkdir -p "$lang_chroot$(dirname "$lib")"
                    sudo cp "$lib" "$lang_chroot$lib"
                fi
            done
        fi
    done
    
    # Special handling for different languages
    case $lang in
        java11|java21)
            # Copy Java runtime libraries
            java_lib_dir=$(dirname "$(echo $binaries | awk '{print $1}')")/../lib
            if [ -d "$java_lib_dir" ]; then
                sudo cp -r "$java_lib_dir" "$lang_chroot$(dirname "$java_lib_dir")/"
            fi
            ;;
        python*)
            # Copy Python standard library
            python_lib=$(/usr/bin/python3 -c "import sys; print(sys.prefix + '/lib')" 2>/dev/null || echo "/usr/lib/python3.9")
            if [ -d "$python_lib" ]; then
                sudo mkdir -p "$lang_chroot$(dirname "$python_lib")"
                sudo cp -r "$python_lib" "$lang_chroot$python_lib"
            fi
            ;;
        javascript*)
            # Copy Node.js libraries
            node_path=$(/usr/bin/node -e "console.log(process.execPath)" 2>/dev/null | xargs dirname | xargs dirname)
            if [ -d "$node_path/lib" ]; then
                sudo mkdir -p "$lang_chroot$node_path"
                sudo cp -r "$node_path/lib" "$lang_chroot$node_path/"
            fi
            ;;
    esac
    
    echo "✅ Chroot for $lang created successfully"
}

# Setup each language
setup_language_chroot "c" "/usr/bin/gcc /usr/bin/g++"
setup_language_chroot "cpp" "/usr/bin/g++ /usr/bin/gcc"
setup_language_chroot "go" "/usr/bin/go"
setup_language_chroot "java11" "/usr/lib/jvm/java-11-openjdk-amd64/bin/java /usr/lib/jvm/java-11-openjdk-amd64/bin/javac"
setup_language_chroot "java21" "/usr/lib/jvm/java-21-openjdk-amd64/bin/java /usr/lib/jvm/java-21-openjdk-amd64/bin/javac"
setup_language_chroot "python3.9" "/usr/bin/python3.9"
setup_language_chroot "javascript18" "/usr/bin/node"

echo "✅ All language chroots fixed successfully"