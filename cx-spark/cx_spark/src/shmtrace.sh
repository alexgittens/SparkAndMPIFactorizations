while true; do
    echo "RECORD_START `date`"
    du -hs /dev/shm
    find /dev/shm -type f | xargs -I{} -- ls -lt {}
    echo "RECORD_END"
    echo
    sleep 10
done
