package de.hpi.ddm.actors;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.CompletionStage;

import akka.NotUsed;
import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.stream.SourceRef;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.StreamRefs;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import de.hpi.ddm.messages.SourceByteMessage;
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
				.match(SourceByteMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(LargeMessage<?> message) {
		ActorRef receiver = message.getReceiver();
		ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));

		//TODO Way to slow
		byte[] serializedObject = toByte(message.getMessage());
		Byte[] converted = new Byte[serializedObject.length];
		for(int i = 0; i< serializedObject.length; i++)
			converted[i] = Byte.valueOf(serializedObject[i]);

		Source<Byte, NotUsed> source = Source.from(Arrays.asList(converted));
		SourceRef<Byte> sourceRef = source.runWith(StreamRefs.sourceRef(), this.context().system());

		receiverProxy.tell(new SourceByteMessage(sourceRef, this.sender(), message.getReceiver()), this.self());
	}

	private byte[] toByte(Object object){
		try {
			Kryo kryo = new Kryo();
			ByteArrayOutputStream stream = new ByteArrayOutputStream();
			Output output = new Output(stream);
			kryo.writeClassAndObject(output, object);
			output.close();
			stream.close();
			return stream.toByteArray();
		}catch(Exception e){
			return null;
		}
	}

	private void handle(SourceByteMessage message){
		Sink sink = Sink.fold(new ArrayList<Byte>(), (aggr, next) -> add(aggr, (Byte)next));
		Source source = message.getSourceRef().getSource();

		final CompletionStage<ArrayList<Byte>> o = (CompletionStage<ArrayList<Byte>>) source.runWith(sink, this.context().system());
		o.whenComplete((p,a ) -> handleTransmissionComplete(p,a, message.getReceiver(), message.getSender()));
	}

	private void handleTransmissionComplete(ArrayList<Byte> elements, Object exception, ActorRef receiver, ActorRef sender) {
		Object object = null;
		try{
			Object[] array = elements.toArray();
			byte[] converted = new byte[array.length];
			for(int i = 0; i< array.length; i++)
				converted[i] = (byte)array[i];
			Kryo kryo = new Kryo();

			ByteArrayInputStream stream = new ByteArrayInputStream(converted);
			Input input = new Input(stream);
			object = kryo.readClassAndObject(input);
			input.close();
			receiver.tell(object, sender);
		}catch(Exception e){
			System.out.println(e);
		}
	}

	private ArrayList<Byte> add (ArrayList<Byte> list, Byte next){
		list.add(next);
		return list;
	}
}
