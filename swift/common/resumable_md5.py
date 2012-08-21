from _ctypes import POINTER
from ctypes import *
import hashlib

class EVP_MD(Structure):
    _fields_ = [
        ('type', c_int),
        ('pkey_type', c_int),
        ('md_size', c_int),
        ('flags', c_ulong),
        ('init', c_void_p),
        ('update', c_void_p),
        ('final', c_void_p),
        ('copy', c_void_p),
        ('cleanup', c_void_p),
        ('sign', c_void_p),
        ('verify', c_void_p),
        ('required_pkey_type', c_int*5),
        ('block_size', c_int),
        ('ctx_size', c_int),
    ]

class EVP_MD_CTX(Structure):
    _fields_ = [
        ('digest', POINTER(EVP_MD)),
        ('engine', c_void_p),
        ('flags', c_ulong),
        ('md_data', POINTER(c_char)),
    ]


class EVPobject(Structure):
    _fields_ = [
        ('ob_refcnt', c_size_t),
        ('ob_type', c_void_p),
        ('name', py_object),
        ('ctx', EVP_MD_CTX),
    ]

class rmd5:

    def __init__(self, state=None):
        self.hash = hashlib.md5()
        self.evp_ctx = cast(c_void_p(id(self.hash)), POINTER(EVPobject)).contents
        if not state is None:
            self.set_state(state)

    def update(self, bytes):
        self.hash.update(bytes)

    def hexdigest(self):
        return self.hash.hexdigest()

    def set_state(self, state):
        ctx = self.evp_ctx.ctx
        digest = ctx.digest.contents
        memmove(ctx.md_data, state, digest.ctx_size)

    def get_state(self):
        ctx = self.evp_ctx.ctx
        digest = ctx.digest.contents
        return ctx.md_data[:digest.ctx_size]
