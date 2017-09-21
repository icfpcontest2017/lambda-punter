# extracts a play of a game as a list of JSON objects

#!/bin/bash
echo '{"game":'
echo '[{"start":{}},'
cat $1 | grep JSON: | sed 's/^.*JSON:\(.*\)$/\1,/'
echo ' {"stop":{}}]}'
