# Session 3
### [👉 Watch the session recordings]()

### [👉 Slides]()


## Goals 🎯

- [x] Fix bug in candles service and make it work
    - [x] Well, there is no bug.
    - [x] Format the candle message
    - [x] EMIT_INCOMPLETE_CANDLES
    
- [ ] Dockerize the candles service
- [ ] Build the technical indicators service

## Questions

### Artem Philippov
Hi. I think that the problem with .current. It should be .final

You can use one or the other. We have a boolean parameter in our config called EMIT_INCOMPLETE_CANDLES
to do one or the other.

### Stefan Pajovic
Is redpanda saving all these trades infinitely? what happen when our disk gets full?

You can set the retention period for each topic, which is the time that Redpanda keeps
the data in disk. After that, the message is dropped.

By default, in our redpanda dev cluster the retention is infinite (meaning data is never dropped).
On the other hand, the kind of event we are saving is light (e.g. 22k trades -> 4MB)

### Carlo Casorzo
if you wanted to emit not only 1m candles but also for example 1h, 1d, etc, would you normally want to use the same service and have separate sdf/out_topics?

You would spin up at least 1 microservice for each frequency. For example:
- Candles 60-seconds
- Candles 5 minutes
- Candles 1-hour

that all read trades from the SAME input topic (`trades`) and output either to

- The same `candle` topic. In this case, you would need to add another field to your candle
messages with the frequency of the candle.

or

- To different `candle_60_sec`, `candle_5_minute`, `candle_1_hour` topics.


### Vincent Reynard Satyadharma

How do we horizontally scale the candles services? Will the data aggregation still work if multiple instances are aggregating the same candle period?

[Add slides about topic partitions and consumer groups]

### Carlo Casorzo
but how will the tumbling windows work this way? will quix use those state topics you showed yesterday in order to allow this distributed tumbling window calc?
