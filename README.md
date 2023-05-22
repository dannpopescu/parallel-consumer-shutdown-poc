Bugs found for the Parallel Consumer shutdown modes:

1. shutdown drain first
   2. when a record is in flight and the PC is instructed to close, the worker thread is interrupted so the record is not processed correctly (see `givenRecordInFlightAndEmptyQueue_whenCloseDrainFirst_thenFinishInFlightThenClose`)
   3. when a record is in flight and another one is in the internal queue waiting to be processed, when the PC is instructed to close but there are more messages in the topic, it will continue to poll until the shutdown timeout expires (see `givenRecordsInFlightAndMoreWaitingInQueue_whenCloseDrainFirst_thenFinishInFlightAndQueuedThenCloseWithoutPollingMoreMessages`)