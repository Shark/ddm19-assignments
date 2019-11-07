package de.hpi.ddm.actors;

import java.io.*;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import akka.NotUsed;
import akka.actor.*;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.util.Timeout;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

public class LargeMessageProxy extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "largeMessageProxy";
	
	public static Props props() {
		return Props.create(LargeMessageProxy.class);
	}

	////////////////////
	// Actor Messages //
	////////////////////
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class LargeMessage<T> implements Serializable {
		private static final long serialVersionUID = 2940665245810221108L;
		private T message;
		private ActorRef receiver;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class BytesMessage<T> implements Serializable {
		private static final long serialVersionUID = 4057807743872319842L;
		private T bytes;
		private ActorRef sender;
		private ActorRef receiver;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	static class AskReference implements Serializable{
		private static final long serialVersionUID = 4057807743851319842L;

		private ActorRef sender;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	static class AnswerReference implements Serializable{
		private static final long serialVersionUID = 4057802342472319842L;

		private ActorRef sender;
	}
	
	/////////////////
	// Actor State //
	/////////////////

	LargeMessage<?> storage = null;
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(LargeMessage.class, this::handle)
				.match(BytesMessage.class, this::handle)
				.match(AskReference.class, this::handle)
				.match(AnswerReference.class, this::handle)
				.match(ActorIdentity.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(LargeMessage<?> message) {
		ActorRef receiver = message.getReceiver();
		ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));
		if(this.storage != null)
			System.out.println("\n\n\n\n\n\n\n");
		this.storage = message;
		AskReference askReference = new AskReference(this.self());
		receiverProxy.tell(new Identify(new byte[0]) , this.self());

		// This will definitely fail in a distributed setting if the serialized message is large!
		// Solution options:
		// 0.5. Serialize the object and store it on file and read.
		// 1. Serialize the object and send its bytes batch-wise (make sure to use artery's side channel then).
		// 2. Serialize the object and send its bytes via Akka streaming.
		// 3. Send the object via Akka's http client-server component.
		// 4. Other ideas ...
		//receiverProxy.tell(new BytesMessage<>(tempFile.getAbsolutePath(), this.sender(), message.getReceiver()), this.self());
	}

	private void handle(ActorIdentity message){
		ActorRef actorRef = message.getActorRef().get();
		File tempFile = null;
		Kryo kryo = new Kryo();
		ByteArrayOutputStream outputStream = null;
		ActorRef shardActor = null;


		try{
			outputStream = new ByteArrayOutputStream();
			Output output = new Output(outputStream);
			kryo.writeClassAndObject(output, storage.getMessage());
			output.close();
			outputStream.close();
		} catch (Exception e) {
			e.printStackTrace();
		}


		Source<byte[], NotUsed> source = Source.from(Arrays.asList(outputStream.toByteArray()));

		Sink<byte[], NotUsed> sink =
				Sink.<byte[]>actorRefWithAck(
						actorRef,
						new StreamInitialized(),
						Ack.INSTANCE,
						new StreamCompleted(),
						ex -> new StreamFailure(ex));
		source.to(sink);
	}

	private void handle(AskReference message) {
		message.getSender().tell(new AnswerReference(this.sender()), this.self());
	}

	private void handle(AnswerReference message) {

	}

	private void handle(BytesMessage<?> message) {
		// Reassemble the message content, deserialize it and/or load the content from some local location before forwarding its content.
		Object object = null;
		try{
			String file = (String)message.getBytes();
			FileInputStream inputStream = new FileInputStream(file);
			Kryo kryo = new Kryo();
			Input input = new Input(inputStream);
			object = kryo.readClassAndObject(input);
			input.close();
		}catch(Exception e){}
		message.getReceiver().tell(object, message.getSender());
	}

	enum Ack {
		INSTANCE;
	}

	static class StreamInitialized {}

	static class StreamCompleted {}

	static class StreamFailure {
		private final Throwable cause;

		public StreamFailure(Throwable cause) {
			this.cause = cause;
		}

		public Throwable getCause() {
			return cause;
		}
	}

}


