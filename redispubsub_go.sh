#!/bin/bash
# Run this (redisnet_go.sh) for sample redis pub/sub network with publisher and subscriber 
if [ -z "$1" ]; then
  echo "Usage: $0 <cmake build dir>"
  exit 1
fi

DIE=0
srcdir=`dirname $0`
bdir=$1
test -z "$srcdir" && srcdir=.
pwd

(test -f ./$bdir/clientSubscriber/ClientSubscribe) || {
  echo
  echo "**Error**: You must have a \"$bdir/clientSubscriber\" folder with file \"ClientSubscribe\" built from CMakeLists"
  DIE=1
}
(test -f ./$bdir/clientPublisher/ClientPublish) || {
  echo
  echo "**Error**: You must have a \"$bdir/clientPublisher\" folder with file \"ClientPublish\" built from CMakeLists"
  DIE=1
}


if test "$DIE" -eq 1; then
  cd ..
  echo "Finished with failure"
  exit 1
fi

. ./set_env.sh

(docker compose up -d)
if compgen -G "output_*" > /dev/null; then 
  echo "Cleared output_*" 
  rm output_* 
fi

count=1
while [ $count -le 5 ]; do
  sleep .4
  export REDIS_PUBSUB_SUBSCRIBER_LOGFILE=output_pubsub_subscriber_$count.log
  (./$bdir/clientSubscriber/ClientSubscribe > output_scrb_$$_$count.log 2>&1 &)
  ((count++))
done

sleep .4
(./$bdir/clientPublisher/ClientPublish > output_publ_$$.log 2>&1 &)

cd ..
echo "Redisnet running in "\`$srcdir\'". Use redisnet_stop.sh to end the processes running."
echo "Type \"docker compose logs -f\" to show redis container logs."
exit 0



