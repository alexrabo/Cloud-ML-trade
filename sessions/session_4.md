# Session 3
### [👉 Watch the session recordings]()

### [👉 Slides]()


## Goals 🎯

- [x] Show horizontal scaling in action
- [x] Add `candle_seconds` to our messages
- [ ] Complete to-feature-store service
    - [ ] Dockerize it

- [ ] Docker compose file for our technical-indicators pipeline
- [ ] Building the backfill pipeline.

## Questions

### Carlo Casorzo

*Should the technical indicators sdf first filter out the candles with different window size?*
YES!

*I was actually thinkinh that it would be a good idea to extend the candles service to produce all candles to the "candles" topic with base key, ex: "BTCUSD", but maybe also to a filtered one for its own window size with a separate key, ex: "key:BTCUSD-1m"*

### Alexandre
Hello. I may have missed it, but it is not in the document (see below). Can we have the link or details to pre installing TA lib?
"This is a C library that requires an extra previous installation step [HERE](So we can increase partitions on the fly?)"

### Stefan Pajovic
First partition ih handling 2 keys, so 4th partition is obsolete. So this means we can implement new partition obefore we introduce new keys, then new key is going to new partition? Do I get this right? :)