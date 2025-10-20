#!/bin/bash
echo "🚀 EMERGENCY SANDBOX RESET - FIXING PERMISSION CORRUPTION"

# Stop everything
sudo pkill -f evalx
sleep 2

# Clean all isolate boxes
echo "🧹 Cleaning all isolate boxes..."
for i in {0..49}; do
    sudo isolate --cg --box-id=$i --cleanup 2>/dev/null && echo "Cleaned box $i" || echo "Box $i already clean"
done

# Reset Redis
echo "🔴 Resetting Redis pools..."
redis-cli del "evalx:sandboxes:ready" "evalx:sandboxes:cleanup" 2>/dev/null

echo "✅ Reset complete. Start services normally:"
echo "   ./target/release/evalx"
echo "   sudo ./target/release/evalx worker"