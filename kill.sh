#!/bin/bash
ps=$(pgrep -u punter)
while [[ ! -z $ps ]]
do
    sudo -u punter pkill -u punter
    ps=$(pgrep -u punter)
done
sudo -u punter rm -fr /tmp/*
