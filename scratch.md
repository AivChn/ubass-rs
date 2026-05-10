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


# Stream structure
> I don't have the time to keep working on this now, but i might do this later
```rust

  pub struct Input;
  pub struct Output;
  trait StreamDirection {}
  impl StreamDirection for Input {}
  impl StreamDirection for Output {}

#[allow(private_bounds)]
#[derive(Debug)]
  pub struct RequestedStream<Direction: StreamDirection> {
      api: Weak<ApiInner>,
      track_id: Box<[u8]>,
      session: SessionId,
      update: watch::Receiver<StreamMessage>,
      _state: PhantomData<Direction>,
  }

#[allow(private_bounds)]
  impl<_T: StreamDirection> RequestedStream<_T> {
      fn new<Direction: StreamDirection>(
          api: Weak<ApiInner>,
          track_id: Box<[u8]>,
          session: SessionId,
      ) -> RequestedStream<Direction> {
          RequestedStream::<Direction> {
              api,
              track_id,
              session,
              update: todo!(),
              _state: PhantomData,
          }
      }
  }

  impl api::types::RequestedStream for RequestedStream<Output> {
      type Stream = OutputStream;
      type Error = ConnectionError;
      type OwningConnection = Connection;

      fn track_id(&self) -> &[u8] {
          self.track_id.as_ref()
      }

      async fn reject(self, reason: impl Into<String>) -> Result<(), Self> {
          todo!()
      }

      async fn approve_and_ready(
          self,
          connection: Self::OwningConnection,
      ) -> core::result::Result<Self::Stream, (Self::Error, Self::OwningConnection)> {
          todo!()
      }

      async fn approve_if_and_ready(
          self,
          f: impl FnOnce(&[u8]) -> bool,
          reject_reason: impl Into<String>,
          connection: Self::OwningConnection,
      ) -> Option<core::result::Result<Self::Stream, (Self::Error, Self::OwningConnection)>> {
          if f(self.track_id.as_ref()) {
              Some(self.approve_and_ready(connection).await)
          } else {
              self.reject(reject_reason).await;
              None
          }
      }
  }

  impl api::types::RequestedStream for RequestedStream<Input> {
      type Stream = InputStream;
      type Error = ConnectionError;
      type OwningConnection = Connection;

      fn track_id(&self) -> &[u8] {
          self.track_id.as_ref()
      }

      async fn reject(self, reason: impl Into<String>) -> Result<(), Self> {
          todo!()
      }

      async fn approve_and_ready(
          self,
          connection: Self::OwningConnection,
      ) -> core::result::Result<Self::Stream, (Self::Error, Self::OwningConnection)> {
          todo!()
      }

      async fn approve_if_and_ready(
          self,
          f: impl FnOnce(&[u8]) -> bool,
          reject_reason: impl Into<String>,
          connection: Self::OwningConnection,
      ) -> Option<core::result::Result<Self::Stream, (Self::Error, Self::OwningConnection)>> {
          if f(self.track_id.as_ref()) {
              Some(self.approve_and_ready(connection).await)
          } else {
              self.reject(reject_reason).await;
              None
          }
      }
  }

#[allow(private_bounds)]
  pub struct PendingStream<Direction: StreamDirection> {
      api: Weak<ApiInner>,
      track_id: Box<[u8]>,
      session: SessionId,
      update: watch::Receiver<StreamMessage>,
      _state: PhantomData<Direction>,
  }

#[allow(private_bounds)]
  impl<_T: StreamDirection> PendingStream<_T> {
      fn new<Direction: StreamDirection>(
          api: Weak<ApiInner>,
          track_id: Box<[u8]>,
          session: SessionId,
      ) -> PendingStream<Direction> {
          PendingStream::<Direction> {
              api,
              track_id,
              session,
              update: todo!(),
              _state: PhantomData,
          }
      }
  }

  impl api::types::PendingStream for PendingStream<Input> {
      type Stream = InputStream;
      type Error = ConnectionError;
      type OwningConnection = Connection;

      async fn ready(
          self,
          connection: Self::OwningConnection,
      ) -> core::result::Result<Self::Stream, (Self::Error, Self::OwningConnection)> {
          todo!()
      }

      async fn discard(self) -> core::result::Result<(), Self::Error> {
          todo!()
      }
  }

  impl api::types::PendingStream for PendingStream<Output> {
      type Stream = OutputStream;
      type Error = ConnectionError;
      type OwningConnection = Connection;

      async fn ready(
          self,
          connection: Self::OwningConnection,
      ) -> core::result::Result<Self::Stream, (Self::Error, Self::OwningConnection)> {
          todo!()
      }

      async fn discard(self) -> core::result::Result<(), Self::Error> {
          todo!()
      }
  }


```
```
```


# What To Do?

1. Hello Packet - send through listening socket instead of port as header [V]
2. StreamDone packet [V]
3. Restransmition [V]
4. key rotation
5. seek [V]

## Seek
`head`s guarantee shifts from "up to here is safe" to "from here onwards it definitely isn't"
`head` advances from current head instead of from index 0, easily skipping seeked over data.
`complete()`` call updates an internal field `complete: Option<boo>`:
  - `None`: complete has not been called
  - `Some(require_full)` - complete has been called, wait for `buffer.is_done() || !require_full`
Buffer being full is an immediate shortcut to done

# Bugs
2 packet test failed, seemingly just didn't send request or the server did not receive it. 
But why?

Echo - client panicked, no idea why. Add logs.
