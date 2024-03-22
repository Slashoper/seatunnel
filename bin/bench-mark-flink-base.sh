
WORKDIR=$(cd "$(dirname "$0")" || exit; pwd)
echo $WORKDIR
#:<< COMMENT
for i in {1..10000}; do
 echo "======提交$i个任务========= !!"
# $WORKDIR/start-seatunnel-flink-15-connector-v2.sh --config $WORKDIR/../config/v2.batch.config.template
  $WORKDIR/seatunnel.sh --config $WORKDIR/../config/v2.batch.config.template
done
#COMMENT