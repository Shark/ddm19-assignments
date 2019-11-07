package de.hpi.ddm.actors;

import java.io.*;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

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
	
	/////////////////
	// Actor State //
	/////////////////
	
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
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(LargeMessage<?> message) {
		ActorRef receiver = message.getReceiver();
		ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));
		File tempFile = null;
		Kryo kryo = new Kryo();
		try{
			File tempDir = new File(System.getProperty("java.io.tmpdir"));
			tempFile = File.createTempFile("largeMessage", ".tmp", tempDir);
			FileOutputStream file = new FileOutputStream(tempFile);
			Output output = new Output(file);
			kryo.writeClassAndObject(output, message.getMessage());
			output.close();
			file.close();

		} catch (Exception e) {
			e.printStackTrace();
		}

		// This will definitely fail in a distributed setting if the serialized message is large!
		// Solution options:
		// 0.5. Serialize the object and store it on file and read.
		// 1. Serialize the object and send its bytes batch-wise (make sure to use artery's side channel then).
		// 2. Serialize the object and send its bytes via Akka streaming.
		// 3. Send the object via Akka's http client-server component.
		// 4. Other ideas ...
		receiverProxy.tell(new BytesMessage<>(tempFile.getAbsolutePath(), this.sender(), message.getReceiver()), this.self());
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
}
