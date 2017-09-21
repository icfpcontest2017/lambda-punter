#!/bin/bash
dir=`pwd`

if [ $# -gt 1 ]; then
  cd $dir/punters/$2/$1
  sudo -u punter ./punter 2> /dev/null
else
  cd $dir/punters/$1
  ./punter 2> /dev/null
fi;
cd $dir
