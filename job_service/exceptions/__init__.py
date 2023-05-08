class NotFoundException(Exception):
    ...


class JobExistsException(Exception):
    ...


class NoSuchImportableDataset(Exception):
    ...


class BadQueryException(Exception):
    ...


class AuthError(Exception):
    ...
