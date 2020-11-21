import unittest
import string
import collections
import itertools

# TODO: pass via command line for better functionality
NUM_OUTPUTS = 12
NUM_INPUTS = 3


class TestMapReduce(unittest.TestCase):
    def test_output(self):
        def expected():
            d = collections.Counter()
            
            for i in range(NUM_INPUTS):
                with open("build/bin/input/testdata_{}.txt".format(i+1)) as f:
                    for line in f:
                        line = line.replace(".", " ")
                        for word in line.split():
                            d[word] += 1
            return d

        def actual():
            d = collections.Counter()
            for i in range(NUM_OUTPUTS):
                with open("build/bin/output/output{}.txt".format(i)) as f:
                    for line in f:
                        (key, val) = line.split()
                        d[key] = int(val)

            return d

        self.assertDictEqual(actual(), expected())

    def test_mutually_exclusive(self):
        sets = [set() for i in range(NUM_OUTPUTS)]

        for i in range(NUM_OUTPUTS):
            with open("build/bin/output/output{}.txt".format(i)) as f:
                for line in f:
                    (key, val) = line.split()
                    sets[i].add(key)

        self.assertTrue(all(s[0].isdisjoint(s[1]) for s in itertools.combinations(sets, 2)))

if __name__ == "__main__":
    unittest.main()