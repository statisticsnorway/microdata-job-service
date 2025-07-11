class NotFoundException(Exception): ...


class JobExistsException(Exception): ...


class JobAlreadyCompleteException(Exception): ...


class NoSuchImportableDataset(Exception): ...


class BadQueryException(Exception): ...


class AuthError(Exception): ...


class BumpingDisabledException(Exception): ...


class NameValidationError(Exception): ...
