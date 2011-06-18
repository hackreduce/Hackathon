CLUSTER=1
USER=team9

#gradle
scp -i hackreduce.pem build/libs/HackReduce-0.2.jar hadoop@hackreduce-cluster-${CLUSTER}.hopper.to:~/users/${USER}/
ssh -i hackreduce.pem hadoop@hackreduce-cluster-${CLUSTER}.hopper.to hadoop/bin/hadoop jar "~/users/${USER}/HackReduce-0.2.jar" org.thebigjc.hackreduce.XBarJob /datasets/nyse/daily_prices /users/team9/xbar &
open http://hackreduce-cluster-${CLUSTER}.hopper.to:50030 &
wait

