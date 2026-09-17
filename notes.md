
# Attestations

Let's think about this concretely in terms of doing attestation broadcast. 


- Node wants to talk to send attestations to its peers and to collect all the attestations from its peers. 
- Node must inform the AttestationBroadcast module that it intends to send attestation for subnet X
- AB then needs to find peers for subnet X
- AB then passes these peers to waggle and tells them to open topic subnet X with them
- AB then periodically queries for waggle peers, and sends them the attestations that it has.  

## Stream model: 

Connect to peer: 
Open all the waggle streams that you're interested in from the peer. 

Peer: 
Opens all the waggle stream it's interested in. 


## Who runs the stream go routines? 
- For the time being waggle runs them.
