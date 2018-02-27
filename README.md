https://arxiv.org/pdf/1802.07000.pdf

just playing around with this

questions:
  * which proposer does a client talk to? does it just retry on the same 
    one until the ballot gets fast forwarded sufficiently for a write to happen?
notes:
  * nonblocking accept and propose and wait for f + 1 rather than all replies