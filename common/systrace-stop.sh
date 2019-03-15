adb shell atrace --async_dump -z > tmp.trace
name=$(date +%Y%m%d-%H%M%S).html
python2 /home/wxl/workspace/android-sdk-linux/platform-tools/systrace/systrace.py --from-file=tmp.trace -o $name
google-chrome $name
