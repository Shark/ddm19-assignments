package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.*;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
import de.hpi.ddm.structures.Line;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class Master extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "master";

	public static Props props(final ActorRef reader, final ActorRef collector) {
		return Props.create(Master.class, () -> new Master(reader, collector));
	}

	public Master(final ActorRef reader, final ActorRef collector) {
		this.reader = reader;
		this.collector = collector;
		this.workers = new ArrayList<>();
		this.lineQueue = new LinkedList<>();
		this.idleWorkers = new LinkedList<>();
		this.isAllBatchesReceived = false;
	}

	////////////////////
	// Actor Messages //
	////////////////////

	@Data
	public static class StartMessage implements Serializable {
		private static final long serialVersionUID = -50374816448627600L;
	}
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class BatchMessage implements Serializable {
		private static final long serialVersionUID = 8343040942748609598L;
		private List<Line> lines;
	}

	@Data
	public static class RegistrationMessage implements Serializable {
		private static final long serialVersionUID = 3303081601659723997L;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class HintRequestMessage implements Serializable {
		private static final long serialVersionUID = 8343234942748609598L;
		private char[] permutationKeys; // [b,c,d,e,f,g,h,i,j]
		private char hintKey;           // a
		private String[] hashedHints;   // ["hintHash1", "hintHash2", ...]
		private ActorRef boss;          // ActorRef(master)
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class HintAnswerMessage implements Serializable {
		private static final long serialVersionUID = 8343232442748609598L;
		private char hintKey;         // a
		private String[] solvedHints; // ["hintHash2",  ...]
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class PasswordRequestMessage implements Serializable {
		private static final long serialVersionUID = 8343231242748609598L;
		private char[] permutationKeys; // [ 'a', 'e']
		private int passwordLength;     // 10
		private String hashedPassword;  // "hash1"
		private ActorRef boss;          // ActorRef(master)
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class PasswordAnswerMessage implements Serializable {
		private static final long serialVersionUID = 8343232442748129598L;
		private String solvedPassword;  // "aaeeaaee"
		private String hashedPassword;  // "hint1
	}



	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef reader;
	private final ActorRef collector;
	private final List<ActorRef> workers;
	private final LinkedList<String[]> lineQueue;
	private final LinkedList<ActorRef> idleWorkers;
	private Boolean isAllBatchesReceived;

	private long startTime;
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() {
		Reaper.watchWithDefaultReaper(this);
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(StartMessage.class, this::handle)
				.match(BatchMessage.class, this::handle)
				.match(Terminated.class, this::handle)
				.match(RegistrationMessage.class, this::handle)
				.match(Worker.ReadyMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	protected void handle(StartMessage message) {
		this.startTime = System.currentTimeMillis();
		
		this.reader.tell(new Reader.ReadMessage(), this.self());
	}
	
	protected void handle(BatchMessage message) {
		
		///////////////////////////////////////////////////////////////////////////////////////////////////////
		// The input file is read in batches for two reasons: /////////////////////////////////////////////////
		// 1. If we distribute the batches early, we might not need to hold the entire input data in memory. //
		// 2. If we process the batches early, we can achieve latency hiding. /////////////////////////////////
		// TODO: Implement the processing of the data for the concrete assignment. ////////////////////////////
		///////////////////////////////////////////////////////////////////////////////////////////////////////
		
		if (message.getLines().isEmpty()) {
			this.collector.tell(new Collector.PrintMessage(), this.self());
			this.isAllBatchesReceived = true;
			return;
		}

		/*this.lineQueue.addAll(message.getLines());

		if(!this.idleWorkers.isEmpty()) {
			ActorRef worker = this.idleWorkers.removeFirst();
			String[] line = this.lineQueue.removeFirst();
			worker.tell(new Worker.LineMessage(line), this.self());
		}*/

		Line head = message.getLines().get(0);
		ArrayList<char[]> allHints = allHintSets(head.getPasswordChars());

		int i = 0;

		ArrayList<String> allHintHashes = new ArrayList<String>();
		for(Line line : message.getLines()){
			String[] hints = line.getHints();
			for(String hint : hints) {
				allHintHashes.add(hint);
			}
		}

		String[] allHintHashesArray = new String[allHintHashes.size()];
		allHintHashes.toArray(allHintHashesArray);



		for(ActorRef worker : this.workers){
			//worker.tell(new HintRequestMessage(allHints.get(i), allHintHashesArray, this.self()),this.self());
			i++;
		}
		for (Line line : message.getLines())
			System.out.println(line.toString());
		
		this.collector.tell(new Collector.CollectMessage("Processed batch of size " + message.getLines().size()), this.self());

		//TODO Only read when work is done
		//this.reader.tell(new Reader.ReadMessage(), this.self());
	}

	protected void handle(Worker.ReadyMessage message) {
		if(this.lineQueue.isEmpty()) {
			if(this.isAllBatchesReceived) {
				this.terminate();
			} else {
				this.idleWorkers.add(message.getSender());
			}
			return;
		}

		String[] line = this.lineQueue.removeFirst();
		message.getSender().tell(new Worker.LineMessage(line), this.self());
	}

	private ArrayList<char[]> allHintSets(char[] passwordChars){
		ArrayList<char[] > list = new ArrayList<char[]>();
		for(int i=0; i< passwordChars.length; i++){
			char[] hintSets = new char[passwordChars.length - 1];
			boolean isSkipped = false;
			for(int j = 0; j < passwordChars.length; j++){
				if(i == j){
					isSkipped = true;
				}else if(isSkipped){
					hintSets[j - 1] = passwordChars[j];
				} else{
					hintSets[j] = passwordChars[j];
				}
			}
			list.add(hintSets);
		}
		return list;
	}

	protected void terminate() {
		this.reader.tell(PoisonPill.getInstance(), ActorRef.noSender());
		this.collector.tell(PoisonPill.getInstance(), ActorRef.noSender());
		
		for (ActorRef worker : this.workers) {
			this.context().unwatch(worker);
			worker.tell(PoisonPill.getInstance(), ActorRef.noSender());
		}
		
		this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
		
		long executionTime = System.currentTimeMillis() - this.startTime;
		this.log().info("Algorithm finished in {} ms", executionTime);
	}

	protected void handle(RegistrationMessage message) {
		this.context().watch(this.sender());
		this.workers.add(this.sender());
//		this.log().info("Registered {}", this.sender());
	}
	
	protected void handle(Terminated message) {
		this.context().unwatch(message.getActor());
		this.workers.remove(message.getActor());
//		this.log().info("Unregistered {}", message.getActor());
	}
}
