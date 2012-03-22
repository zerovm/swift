import gettext


class Version(object):
    def __init__(self, canonical_version, final):
        self.canonical_version = canonical_version
        self.final = final

    @property
    def pretty_version(self):
        if self.final:
            return self.canonical_version
        else:
            return '%s-dev' % (self.canonical_version,)


<<<<<<< HEAD
=======
_version = Version('1.4.8', True)
>>>>>>> Final 1.4.8 versioning
__version__ = _version.pretty_version
__canonical_version__ = _version.canonical_version

gettext.install('swift')
