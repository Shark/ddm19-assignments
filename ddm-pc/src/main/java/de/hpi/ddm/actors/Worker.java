package de.hpi.ddm.actors;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Array;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberRemoved;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import de.hpi.ddm.MasterSystem;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class Worker extends AbstractLoggingActor {

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "worker";

    public static Props props() {
        return Props.create(Worker.class);
    }

    public Worker() {
        this.cluster = Cluster.get(this.context().system());
    }

    ////////////////////
    // Actor Messages //
    ////////////////////

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class LineMessage implements Serializable {
        private static final long serialVersionUID = -7028402685981649936L;
        private String[] line;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ReadyMessage implements Serializable {
        private static final long serialVersionUID = -3199872003897162373L;
        private ActorRef sender;
    }

    /////////////////
    // Actor State //
    /////////////////

    private Member masterSystem;
    private final Cluster cluster;

    /////////////////////
    // Actor Lifecycle //
    /////////////////////

    @Override
    public void preStart() {
        Reaper.watchWithDefaultReaper(this);

        this.cluster.subscribe(this.self(), MemberUp.class, MemberRemoved.class);
    }

    @Override
    public void postStop() {
        this.cluster.unsubscribe(this.self());
    }

    ////////////////////
    // Actor Behavior //
    ////////////////////

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(CurrentClusterState.class, this::handle)
                .match(MemberUp.class, this::handle)
                .match(Master.HintRequestMessage.class, this::handle)
                .match(Master.PasswordRequestMessage.class, this::handle)
                .match(MemberRemoved.class, this::handle)
                .match(LineMessage.class, this::handle)
                .matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
                .build();
    }

    private void handle(CurrentClusterState message) {
        message.getMembers().forEach(member -> {
            if (member.status().equals(MemberStatus.up()))
                this.register(member);
        });
    }

    private void handle(MemberUp message) {
        this.register(message.member());
    }

    private void register(Member member) {
        if ((this.masterSystem == null) && member.hasRole(MasterSystem.MASTER_ROLE)) {
            this.masterSystem = member;

            this.getContext()
                    .actorSelection(member.address() + "/user/" + Master.DEFAULT_NAME)
                    .tell(new Master.RegistrationMessage(), this.self());

            this.sendReadyMessage();
        }
		//worker.tell(new PasswordRequestMessage(new char[]{'G', 'F'}, 10, "c4712866799881ac48ca55bf78a9540b1883ae033b52109169eb784969be09d5", this.self()), this.self());
    }

    private void handle(Master.HintRequestMessage message) {
        char[] keys = message.getPermutationKeys();
        String[] hashedHints = message.getHashedHints();
        List<String> permutations = new ArrayList<String>();
        heapPermutation(keys, keys.length, keys.length, permutations);
        ArrayList<String> solvedHashes = new ArrayList<String>();

        for (String permutation : permutations) {
            String hash = hash(permutation);
            for (String hashedHint : hashedHints) {
                if (hash.equals(hashedHint)) {
                    solvedHashes.add(hash);
                }
            }
        }

        String[] solvedHashedArray = new String[solvedHashes.size()];
        solvedHashedArray = solvedHashes.toArray(solvedHashedArray);

        Master.HintAnswerMessage answerMessage = new Master.HintAnswerMessage();
        answerMessage.setHintKey(message.getHintKey());
        answerMessage.setSolvedHints(solvedHashedArray);
		answerMessage.setSender(this.self());
		message.getBoss().tell(answerMessage, this.self());
    }

    private void handle(Master.PasswordRequestMessage message) {
		String hashedPassword = message.getHashedPassword();
    	List<String> allPermutations = getAllKLength(message.getPermutationKeys(),message.getPasswordLength());
		for(String permutation :  allPermutations){
			String hash = hash(permutation);
			if(hash.equals(hashedPassword)){
				//We are done
				System.out.println(permutation);
				Master.PasswordAnswerMessage answerMessage = new Master.PasswordAnswerMessage();
				answerMessage.setHashedPassword(hashedPassword);
				answerMessage.setSolvedPassword(permutation);
				answerMessage.setSender(this.self());
				message.getBoss().tell(answerMessage, this.self());
				return;
			}
		}
    }

    private void handle(MemberRemoved message) {
        if (this.masterSystem.equals(message.member()))
            this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
    }

    private void handle(LineMessage message) {
        System.out.println(Arrays.toString(message.line));
        this.sendReadyMessage();
    }

    private String hash(String line) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashedBytes = digest.digest(String.valueOf(line).getBytes("UTF-8"));

            StringBuffer stringBuffer = new StringBuffer();
            for (int i = 0; i < hashedBytes.length; i++) {
                stringBuffer.append(Integer.toString((hashedBytes[i] & 0xff) + 0x100, 16).substring(1));
            }
            return stringBuffer.toString();
        } catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    // Generating all permutations of an array using Heap's Algorithm
    // https://en.wikipedia.org/wiki/Heap's_algorithm
    // https://www.geeksforgeeks.org/heaps-algorithm-for-generating-permutations/
    private void heapPermutation(char[] a, int size, int n, List<String> l) {
        // If size is 1, store the obtained permutation
        if (size == 1)
            l.add(new String(a));

        for (int i = 0; i < size; i++) {
            heapPermutation(a, size - 1, n, l);

            // If size is odd, swap first and last element
            if (size % 2 == 1) {
                char temp = a[0];
                a[0] = a[size - 1];
                a[size - 1] = temp;
            }

            // If size is even, swap i-th and last element
            else {
                char temp = a[i];
                a[i] = a[size - 1];
                a[size - 1] = temp;
            }
        }
    }

    private void sendReadyMessage() {
        this.getContext()
                .actorSelection(this.masterSystem.address() + "/user/" + Master.DEFAULT_NAME)
                .tell(new ReadyMessage(this.self()), this.self());
    }

    // The method that prints all
	// possible strings of length k.
	// It is mainly a wrapper over
	// recursive function printAllKLengthRec()
	// https://www.geeksforgeeks.org/print-all-combinations-of-given-length/
	List<String> getAllKLength(char[] set, int k) {
		int n = set.length;
		ArrayList<String> l = new ArrayList<>();
		getAllKLengthRec(set, "", n, k, l);
		return l;
	}

	// The main recursive method
	// to print all possible
	// strings of length k
	void getAllKLengthRec(char[] set, String prefix, int n, int k, ArrayList<String> l) {
		// Base case: k is 0,
		// print prefix
		if (k == 0) {
			l.add(prefix);
			return;
		}
		// One by one add all characters
		// from set and recursively
		// call for k equals to k-1
		for (int i = 0; i < n; ++i) {
			// Next character of input added
			String newPrefix = prefix + set[i];
			// k is decreased, because
			// we have added a new character
			getAllKLengthRec(set, newPrefix, n, k - 1, l);
		}
	}
}