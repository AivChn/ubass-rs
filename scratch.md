# Handshake

## Incoming
received hello packet as receiver

### request app ID approval
what do i need?
1. approval oneshot sender
2. handshake done receiver
3. app ID

# What Am I Talking About Today
1. Streams
2. WriteableBuffer
3. Connection states

## Session States
1. Handshake
  1. Incoming
  2. Pending/Outgoing
2. Streaming From
  - IncomingStream - `&mut WriteableBuffer`
3. Streaming To 
  - OutgoingStream - `ReadableBuffer`
4. Up
 - Default state
5. Down
 - Might cause caching in the future

## Streams
- Representation of the specific buffer currently being transmitted through this session
- OutgoingStream - Sending side, responsible for streaming the data from a readable buffer on its side
- IncomingStream - Receiving side, responsible for receiving the data from the transmission the other host is sending

## WriteableBuffer
IncomingStream struct buffer field
Stream struct is owned by the app

Stream:
  1. currently_streaming bool
  2. current position usize
  3. Finished signal

behavior: 
  1. pause/play the stream
  2. close/abort the stream
  3. head of the stream
  4. is it done?

buffers live in protocol state
stream struct updated with channel message

```rust
  pub trait SessionSender {
    type SendType: Send;
    fn send(&mut self, value: Self::SendType) -> Result<(), SendError<T>> {
      self.send(value)
    }
  }

  pub struct InputStream<'buf, B: WriteableBuffer> {
      api: Weak<ApiInner>,
      buffer: &'buf mut B,
      session: SessionId,
      stream_position: usize,
      stream_size: usize,
      playing: bool,
      done: bool,
      done_signal: Notify,
      connection: Connection,
  }
```

write to buffer

# Few things to rememeber
 - FEC state is only relevant when streaming
 - Restransmit is also directly handled by the long living send routine
    - This does mean I need a Restransmit variant for the StreamEvent
