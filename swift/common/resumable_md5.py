from ctypes import c_uint32, memmove, create_string_buffer, Structure, sizeof, byref, addressof, util, CDLL

_libmd5 = CDLL(util.find_library('rmd5'))

class rmd5_ctx(Structure):
    _fields_ = [
        ('A', c_uint32),
        ('B', c_uint32),
        ('C', c_uint32),
        ('D', c_uint32),
        ('total', c_uint32 * 2),
        ('buflen', c_uint32),
        ('buffer', c_uint32 * 32)
    ]

    def get(self):
        return buffer(self)[:]

    def set(self, bytes):
        fit = min(len(bytes), sizeof(self))
        memmove(addressof(self), bytes, fit)

class rmd5:

    def __init__(self, ctx=None):
        self.ctx = rmd5_ctx()
        if ctx is None:
            _libmd5.md5_init_ctx(byref(self.ctx))
        else:
            self.ctx.set(ctx)
        self.final = False

    def update(self, bytes):
        size = len(bytes)
        if size % 64 == 0:
            _libmd5.md5_process_block(bytes, size, byref(self.ctx))
        else:
            _libmd5.md5_process_bytes(bytes, size, byref(self.ctx))

    def digest_and_state(self):
        if self.final:
            raise
        state = self.ctx.get()
        result = create_string_buffer(16)
        _libmd5.md5_finish_ctx(byref(self.ctx), byref(result))
        self.final = True
        return result.raw.encode('hex_codec'), state
