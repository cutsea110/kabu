#!/bin/sh

STARTDATE=2015-01-01
ENDDATE=2016-01-01

CURRENTDATE=$STARTDATE
while [ 1 ] ; do
  if [ $CURRENTDATE = $ENDDATE ] ; then
     break
  fi
  echo -n $CURRENTDATE
  ./kabu $CURRENTDATE
  if [ $? -ne 0 ]
  then
    echo "	skip..."
  else
    echo "	done..."
  fi
  CURRENTDATE=`date -d "$CURRENTDATE 1day" "+%Y-%m-%d"`
done
