"""Put scripts/ on the import path so tests can `import io_hmm_infer` regardless of where pytest runs."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "scripts"))
