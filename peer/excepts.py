
class NoPeersFound(Exception):
    pass

class TrackerUnavailable(Exception):
    pass

class UnknowStrict(Exception):
    pass

class DownloadError(Exception):
    pass

class PeerOverload(Exception):
    pass

class ChunkNotReady(Exception):
    pass

class OriginURLConnectError(Exception):
    pass

class PieceChecksumError(Exception):
    pass

class IncompleteRead(Exception):
    pass

class RateTooSlow(Exception):
    pass
