Launch with profiles:
-version,number

Number is needed to understand which project send/read in parallel run (accepted from 1 to 5)
Version must be `reactor` for Project Reactor library or `spring` for SCS.

CommonLib contain Reactor Kafka COnfiguration module, Spring JPA Configuration and some common dtos.

TBD


ConsumerCoordinator.refreshCommittedOffsetsIfNeeded for reactive part doesn't work

====

Query
select ackedby, msgtype, count(*) from kafkacheck.kafka group by ackedby, msgtype order by msgtype, ackedby;
select ackreceived, msgtype, count(*) from kafkacheck.kafka group by ackreceived, msgtype;





---
reset env
kubectl delete po $(kubectl get po | grep kafka | grep -v manager | sed 's/ .*//')
truncate kafkacheck.kafka;
