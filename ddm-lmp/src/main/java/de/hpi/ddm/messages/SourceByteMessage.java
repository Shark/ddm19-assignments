package de.hpi.ddm.messages;

import akka.actor.ActorRef;
import akka.stream.SourceRef;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SourceByteMessage{
    private SourceRef<Byte[]> sourceRef;
    private ActorRef sender;
    private ActorRef receiver;
}