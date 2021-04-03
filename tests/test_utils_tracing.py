import os
import sys
import string
import random
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from mabel.operators.internals.trace_blocks import TraceBlocks
from mabel.data.formats.json import parse, serialize
try:
    from rich import traceback
    traceback.install()
except ImportError:   # pragma: no cover
    pass

def random_string(length):
    return ''.join(random.choice(string.hexdigits) for i in range(length))


def test_hashes():

    data_hashes = []
    data_hashes.append(random_string(32))
    data_hashes.append(random_string(32))

    tb = TraceBlocks()
    tb.add_block(data_hash=data_hashes[0])
    tb.add_block(data_hash=data_hashes[1])
    blocks = parse(str(tb))

    previous_block = ''

    for index, block in enumerate(blocks):
        
        if index > 0:  # the first block is a seed - it looks different

            # check the data is being written as expected
            assert block.get('data_hash') == data_hashes[index - 1]
        
            # Check the prev hash
            rehash = tb.hash(previous_block)
            assert rehash == block.get('previous_block_hash')

            # Check the proof - the proof is when the number prepended to the
            # previous block's hash and reshashed resultant hash ends with 
            # either 0 or 5.
            
            #reproof = tb.hash(''.join([block.get('proof',''), block.get('previous_block_hash', '')]))
            #assert reproof[-1] in ['0', '5'], reproof

        previous_block = block


if __name__ == "__main__":
    test_hashes()

    print('okay')
