"""

adapted from: https://leetcode.com/problems/implement-trie-prefix-tree/discuss/653851/python-radix-tree-memory-efficient
"""


from collections import defaultdict

class RadixTreeNode():
    def __init__(self,is_word=False,keys=None):
        self.is_word=is_word
        self.children=keys if keys else defaultdict(RadixTreeNode)

class Trie():
    def __init__(self):
        self.root=RadixTreeNode()

    def insert(self, word):
        return self.insert_helper(self.root, word)

    def insert_helper(self, node, word):
        for key,child in node.children.items():
            prefix, split, rest = self.match(key, word)
            if not split:
                #key complete match
                if not rest:
                    #word matched
                    child.is_word=True
                    return True
                else:
                    #match rest of word
                    return self.insert_helper(child, rest)
            if prefix:
                #key partial match, need to split
                new_node=RadixTreeNode(is_word=not rest,keys={split:child})
                node.children[prefix]=new_node
                del node.children[key]
                return self.insert_helper(new_node, rest)
        node.children[word]=RadixTreeNode(is_word=True)

    def search(self, word):
        return self.search_helper(self.root,word)

    def search_helper(self,node, word):
        for key,child in node.children.items():
            prefix, split, rest = self.match(key, word)
            if not split and not rest:
                return child.is_word
            if not split:
                return self.search_helper(child,rest)
        return False

    def startsWith(self,word):
        return self.startsWith_helper(self.root,word)

    def startsWith_helper(self,node,word):
        for key,child in node.children.items():
            prefix, split, rest = self.match(key, word)
            if not rest:
                return True
            if not split:
                return self.startsWith_helper(child,rest)
        return False

    def match(self, key, word):
        i=0
        for k,w in zip(key,word):
            if k!=w:
                break
            i+=1
        return key[:i],key[i:],word[i:]