# from .node import Node
import os

import dotenv

dotenv.load_dotenv()

network = os.environ.get("NETWORK")
if network == "mainnet":
    from .mainnet import Mainnet as Network
elif network == "testnet":
    from .testnet import Testnet as Network
elif network == "canary":
    from .canary import Canary as Network
else:
    raise RuntimeError(f"invalid network: {network}")

from .node import Node
