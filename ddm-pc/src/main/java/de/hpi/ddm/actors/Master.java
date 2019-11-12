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
		this.queuedHintRequestMessages = new LinkedList<>();
		this.queuedPasswordRequestMessages = new LinkedList<>();
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
		private ActorRef sender;
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
		private ActorRef sender;
	}



	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef reader;
	private final ActorRef collector;
	private final List<ActorRef> workers;
	private final LinkedList<Line> lineQueue;
	private final LinkedList<ActorRef> idleWorkers;
	private final LinkedList<HintRequestMessage> queuedHintRequestMessages;
	private final LinkedList<PasswordRequestMessage> queuedPasswordRequestMessages;
	private Boolean isAllBatchesReceived;
	private char[] passwordChars;
	private int passwordLength;
	private int numberOfHints;

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
				.match(HintAnswerMessage.class, this::handle)
				.match(PasswordAnswerMessage.class, this::handle)
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

		if(message.getLines().isEmpty()) {
			this.collector.tell(new Collector.PrintMessage(), this.self());
			this.isAllBatchesReceived = true;
			return;
		}

		if(this.passwordChars == null) {
			this.passwordChars = message.getLines().get(0).getPasswordChars();
		}
		if(this.passwordLength == 0) {
			this.passwordLength = message.getLines().get(0).getPasswordLength();
		}
		if(this.numberOfHints == 0) {
			this.numberOfHints = message.getLines().get(0).getHints().length;
		}
		
		ArrayList<String> allHintsList = new ArrayList<>();
		for(Line line : message.getLines()) {
			for(int i=0; i<line.getHints().length; i++) {
				allHintsList.add(line.getHints()[i]);
			}
		}
		String allHintsArray[] = new String[allHintsList.size()];
		allHintsList.toArray(allHintsArray);

		for(int i=0; i<this.passwordChars.length; i++) {
			char currentChar = this.passwordChars[i];
			char otherChars[] = new char[this.passwordChars.length-1];
			int j = 0;
			for(int k=0; k<this.passwordChars.length; k++) {
				char otherChar = this.passwordChars[k];
				if(currentChar == otherChar) {
					continue;
				}
				otherChars[j++] = otherChar;
			}
			HintRequestMessage msg = new HintRequestMessage();
			msg.setPermutationKeys(otherChars);
			msg.setHintKey(currentChar);
			msg.setBoss(this.getSelf());
			msg.setHashedHints(allHintsArray);
			this.queuedHintRequestMessages.add(msg);
		}

		this.lineQueue.addAll(message.getLines());

		dispatchMessageQueues();
	}

	protected void handle(HintAnswerMessage message) {
		for(Line line : this.lineQueue) {
			for(int i=0; i<line.getHints().length; i++) {
				for(int j=0; j<message.getSolvedHints().length; j++) {
					if(line.getHints()[i].equals(message.getSolvedHints()[j])) {
						line.getCharsNotInPasswordFromHints().push(message.getHintKey());
					}
				}
			}

			if(line.getCharsNotInPasswordFromHints().size() == this.numberOfHints) {
				PasswordRequestMessage msg = new PasswordRequestMessage();
				msg.setBoss(this.getSelf());
				msg.setHashedPassword(line.getHashedPassword());
				LinkedList<Character> linePwCharsList = new LinkedList<>();
				for(int i=0; i<this.passwordChars.length; i++) {
					if(line.getCharsNotInPasswordFromHints().contains(this.passwordChars[i])) {
						continue;
					}
					linePwCharsList.add(this.passwordChars[i]);
				}
				char linePwChars[] = new char[linePwCharsList.size()];
				int i = 0;
				for(Character c : linePwCharsList) {
					linePwChars[i++] = c;
				}
				msg.setPermutationKeys(linePwChars);
				msg.setPasswordLength(this.passwordLength);
				this.queuedPasswordRequestMessages.add(msg);
			}
		}
		handleWorkerFinished(message.getSender());
	}

	protected void handle(PasswordAnswerMessage message) {
		Iterator<Line> it = this.lineQueue.iterator();
		while(it.hasNext()) {
			Line line = it.next();
			if(line.getHashedPassword().equals(message.getHashedPassword())) {
				it.remove();
				this.log().info("Solved {} = {}, {} passwords left", message.getHashedPassword(), message.getSolvedPassword(), this.lineQueue.size());
			}
		}
		handleWorkerFinished(message.getSender());
	}

	private void handleWorkerFinished(ActorRef worker) {
		this.idleWorkers.add(worker);
		dispatchMessageQueues();

		if(this.isFinished()) {
			this.terminate();
		}
	}

	private void dispatchMessageQueues() {
		HintRequestMessage nextHintRequestMessage = this.queuedHintRequestMessages.peek();
		ActorRef nextWorker = this.idleWorkers.peek();
		while(nextWorker != null && nextHintRequestMessage != null) {
			this.idleWorkers.removeFirst();
			this.queuedHintRequestMessages.removeFirst();
			nextWorker.tell(nextHintRequestMessage, this.getSelf());
			nextHintRequestMessage = this.queuedHintRequestMessages.peek();
			nextWorker = this.idleWorkers.peek();
		}

		PasswordRequestMessage nextPasswordRequestMessage = this.queuedPasswordRequestMessages.peek();
		nextWorker = this.idleWorkers.peek();
		while(nextWorker != null && nextPasswordRequestMessage != null) {
			this.idleWorkers.removeFirst();
			this.queuedPasswordRequestMessages.removeFirst();
			nextWorker.tell(nextPasswordRequestMessage, this.getSelf());
			nextPasswordRequestMessage = this.queuedPasswordRequestMessages.peek();
			nextWorker = this.idleWorkers.peek();
		}

		if(this.queuedHintRequestMessages.isEmpty() && this.queuedPasswordRequestMessages.isEmpty()) {
			this.reader.tell(new Reader.ReadMessage(), this.self());
		}
	}

	private Boolean isFinished() {
		return this.isAllBatchesReceived && this.lineQueue.isEmpty();
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
		handleWorkerFinished(this.sender());
//		this.log().info("Registered {}", this.sender());
	}
	
	protected void handle(Terminated message) {
		this.context().unwatch(message.getActor());
		this.workers.remove(message.getActor());
//		this.log().info("Unregistered {}", message.getActor());
	}
}
