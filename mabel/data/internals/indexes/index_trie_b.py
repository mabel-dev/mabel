class Trie:
    def __init__(self):
        self.trie = {}

    def insert(self, word: str) -> None:
        node = self.trie
        for ch in word:
            if ch not in node:
                node[ch] = {}
            node = node[ch]
        node['$'] = True
        
    def search(self, word: str) -> bool:
        return '$' in self.searchHelper(word, self.trie)

    def startsWith(self, prefix: str) -> bool:
        return len(self.searchHelper(prefix, self.trie)) > 0
        
    def searchHelper(self, word, node) -> dict:
        for i, ch in enumerate(word):
            if ch in node:
                node = node[ch]
            else:
                return {}
        return node


if __name__ == "__main__":

    t = Trie()

    for i in range(100):
        t.insert(hash(i))