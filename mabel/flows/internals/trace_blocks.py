"""
Trace Blocks

As data moves between the flows, Trace Blocks is used to create a record of
operation being run.

It uses an approach similar to a block-chain in that each block includes a
hash of the previous block.

The block contains a hash of the data, the name of the operation, a 
programatically determined version of the code that was run, a timestamp and
a hash of the last block.

This isn't distributed, but the intention is that the trace log writes the
block hash at the time the data is processed which this Class creating an
independant representation of the trace. In order to bypass this control,
the user must update the audit log and this trace block.

#nodoc - don't add to the documentation wiki
"""
import json
import hashlib
import datetime
from typing import Optional
from ...utils import entropy

serialize = json.dumps  # prevent circular imports

EMPTY_HASH = "0" * 64


class TraceBlocks:

    __slots__ = ("blocks", "proof")

    def __init__(
        self, uuid="00000000-0000-0000-0000-000000000000", proof: Optional[str] = None
    ):
        """
        Create block chain and seed with the UUID.

        Parameters:
            uuid: string (UUID) (optional but strongly recommended)
                Unique identifier for the run
            proof: string (optional)
                The set of valid final characters for proofing as a string.
                The default is None which does no proofing. If you set this
                wrong, you could end with an endless loop.
        """
        self.proof = proof
        self.blocks = []
        self.blocks.append(
            {"block": 1, "timestamp": datetime.datetime.now().isoformat(), "uuid": uuid}
        )

    def add_block(self, **kwargs):
        """
        Add a new block to the chain.
        """
        previous_block = self.blocks[-1]
        previous_block_hash = self.hash(previous_block)

        # proof is what makes mining for bitcoin so hard, we're setting a low
        # target of the last character being a 0 or a 5 (i.e. 1/5 chance)
        # if you wanted to make this harder, set a different rule to exit
        # while loop. Setting this proof to be harder will impact performance
        # as finding a value to satify the proof will block processing.

        if self.proof:
            proof = str(entropy.random_int())
            while (
                self.hash("".join([proof, previous_block_hash]))[-1] not in self.proof
            ):
                proof = str(entropy.random_int())

        block = {
            "block": len(self.blocks) + 1,
            "timestamp": datetime.datetime.now().isoformat(),
            "previous_block_hash": previous_block_hash,
            **kwargs,
        }
        if self.proof:
            block["proof"] = proof
        self.blocks.append(block)

    def __str__(self):
        return serialize(self.blocks, indent=True)

    def __repr__(self):
        return f"<TraceBlocks with {len(self.blocks)} blocks>"

    def hash(self, block):
        try:
            bytes_object = serialize(block)
        except:
            bytes_object = block
        raw_hash = hashlib.sha256(bytes_object.encode())
        hex_hash = raw_hash.hexdigest()
        return hex_hash
