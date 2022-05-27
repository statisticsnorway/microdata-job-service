class NotFoundException(Exception):
    pass


class BadRequestException(Exception):
    pass


class JobExistsException(Exception):
    pass


class NoSuchImportableDataset(Exception):
    pass